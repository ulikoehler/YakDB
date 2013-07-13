/* 
 * File:   TableOpenHelper.cpp
 * Author: uli
 * 
 * Created on 6. April 2013, 18:15
 */

#include "TableOpenHelper.hpp"
#include <thread>
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <leveldb/cache.h>
#include <exception>
#include <dirent.h>
#include <unistd.h>
#include <stdint.h>
#include "zutil.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "Tablespace.hpp"
#include "Logger.hpp"
#include "protocol.hpp"

using namespace std;

struct TableOpenParameters {
    uint64_t lruCacheSize;
    uint64_t tableBlockSize;
    uint64_t writeBufferSize;
    uint64_t bloomFilterBitsPerKey;
    bool compressionEnabled;
};

/**
 * Main function for table open worker thread.
 * 
 * Msg format:c
 *      - STOP THREAD msg: One empty frame
 *      - OPEN/CLOSE/TRUNCATE TABLE msg:
 *          - A 1-byte frame with content:
 *              \x00 for open table,
 *              \x01 for close table
 *              \x02 for close & truncate (--> rm -r) table
 *          - A 4-byte frame containing the binary ID
 */
static void HOT tableOpenWorkerThread(zctx_t* context, void* repSocket, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) {
    Logger logger(context, "Table open server");
    while (true) {
        zmsg_t* msg = zmsg_recv(repSocket);
        if (unlikely(!msg)) {
            if (errno == ETERM || errno == EINTR) {
                break;
            }
            debugZMQError("Receive TableOpenServer message", errno);
        }
        //Parse msg parts
        zframe_t* msgTypeFrame = zmsg_first(msg);
        assert(msgTypeFrame);
        assert(zframe_size(msgTypeFrame) == 1);
        uint8_t msgType = (uint8_t) zframe_data(msgTypeFrame)[0];
        //Table ID
        zframe_t* tableIdFrame = zmsg_next(msg);
        assert(tableIdFrame);
        size_t frameSize = zframe_size(tableIdFrame);
        assert(frameSize == sizeof (TableOpenHelper::IndexType));
        uint32_t tableIndex = extractBinary<uint32_t>(tableIdFrame);
        //Check for a STOP msg
        if (frameSize == 0) {
            //Send back the message and exit the loop
            zmsg_send(&msg, repSocket);
            break;
        }
        if (msgType == 0x00) { //Open table
            //Extract parameters (optional, if zerolength, defaults are assumed)
            zframe_t* parametersFrame = zmsg_next(msg);
            assert(parametersFrame);
            const TableOpenParameters* parameters = (TableOpenParameters*) zframe_data(parametersFrame);
            //Resize if neccessary
            if (databases.size() <= tableIndex) {
                databases.reserve(tableIndex + 16); //Avoid large vectors
            }
            //Create the table only if it hasn't been created yet, else just ignore the request
            if (databases[tableIndex] == NULL) {
                logger.info("Creating/opening table #" + std::to_string(tableIndex));
                leveldb::Options options;
                options.create_if_missing = true;
                options.compression = (parameters->compressionEnabled ? leveldb::kSnappyCompression : leveldb::kNoCompression);
                //Set optional parameters
                if (parameters->lruCacheSize != UINT64_MAX) {
                    options.block_cache = leveldb::NewLRUCache(parameters->lruCacheSize);
                } else {
                    //Use a small LRU cache per default, because OS cache doesn't cache uncompressed data
                    // , so it's really slow in random-access-mode for uncompressed data
                    options.block_cache = leveldb::NewLRUCache(1024 * 1024 * 10);
                }
                if (parameters->tableBlockSize != UINT64_MAX) {
                    options.block_size = parameters->tableBlockSize;
                }
                if (parameters->writeBufferSize != UINT64_MAX) {
                    options.write_buffer_size = parameters->writeBufferSize;
                } else {
                    //To counteract slow writes on slow HDDs, we now use a WB per default
                    //The default is tuned not to use too much buffer memory at once
                    options.write_buffer_size = 1024 * 1024 * 64; //64 Mibibytes
                }
                if (parameters->bloomFilterBitsPerKey != UINT64_MAX) {
                    options.filter_policy
                            = leveldb::NewBloomFilterPolicy(parameters->bloomFilterBitsPerKey);
                }
                std::string tableName = "tables/" + std::to_string(tableIndex);
                //Open the table
                leveldb::Status status = leveldb::DB::Open(options, tableName.c_str(), &databases[tableIndex]);
                if (unlikely(!status.ok())) {
                    logger.error("Error while trying to open table #" + std::to_string(tableIndex) + " in directory " + tableName + ": " + status.ToString());
                }
            }
            //In order to improve performance, we reuse the existing frame, we only modify the first byte (additional bytes shall be ignored)
            zframe_data(tableIdFrame)[0] = 0x00; //0x00 == acknowledge, no error
            if (unlikely(zmsg_send(&msg, repSocket) == -1)) {
                logger.error("Communication error while trying to send table open reply: " + std::string(zmq_strerror(errno)));
            }
        } else if (msgType == 0x01) { //Close table
            if (databases.size() <= tableIndex || databases[tableIndex] == NULL) {
                zframe_data(tableIdFrame)[0] = 0x01; //0x01 == Table already closed
            } else {
                leveldb::DB* db = databases[tableIndex];
                databases[tableIndex] = NULL;
                delete db;
                zframe_data(tableIdFrame)[0] = 0x00; //0x00 == acknowledge, no error
            }
            if (unlikely(zmsg_send(&msg, repSocket) == -1)) {
                logger.error("Communication error while trying to send table open reply: " + std::string(zmq_strerror(errno)));
            }
        } else if (msgType == 0x02) { //Close & truncate
            uint8_t responseCode = 0x00;
            //Close if not already closed
            if (!(databases.size() <= tableIndex || databases[tableIndex] == nullptr)) {
                leveldb::DB* db = databases[tableIndex];
                databases[tableIndex] = nullptr;
                delete db;
            }
            /**
             * Truncate, based on the assumption LevelDB only creates files,
             * but no subdirectories
             */
            DIR *dir;
            string dirname = "tables/" + std::to_string(tableIndex);
            struct dirent *ent;
            std::string dotDir = ".";
            std::string dotdotDir = "..";
            if ((dir = opendir(dirname.c_str())) != NULL) {
                while ((ent = readdir(dir)) != NULL) {
                    //Skip . and ..
                    if (dotDir == ent->d_name || dotdotDir == ent->d_name) {
                        continue;
                    }
                    string fullFileName = dirname + "/" + std::string(ent->d_name);
                    logger.trace(fullFileName);
                    unlink(fullFileName.c_str());
                }
                closedir(dir);
                responseCode = 0x00; //0x00 == acknowledge, no error
            } else {
                //For now we just assume, error means it does not exist
                logger.trace("Tried to truncate " + dirname + " but it does not exist");
                responseCode = 0x01; //ACK, deletion not neccesary
            }
            //Now remove the table directory itself
            //Errors (e.g. for nonexistent dirs) do not exist
            rmdir(dirname.c_str());
            logger.debug("Truncated table in " + dirname);
            sendConstFrame(&responseCode, 1, repSocket);
        } else {
            logger.error("Internal protocol error: Table open server received unkown request type " + std::to_string(msgType));
            //Reply anyway
            if (unlikely(zmsg_send(&msg, repSocket) == -1)) {
                logger.error("Communication error while trying to send table opener protocol error reply: " + std::string(zmq_strerror(errno)));
            }
        }
        //Destroy the message if it hasn't been sent or destroyed before
        if (msg != nullptr) {
            zmsg_destroy(&msg);
        }
    }
    logger.debug("Stopping table open server");
    //We received an exit msg, cleanup
    zsocket_destroy(context, repSocket);
}

COLD TableOpenServer::TableOpenServer(zctx_t* context, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) : context(context) {
    //We need to bind the inproc transport synchronously in the main thread because zmq_connect required that the endpoint has already been bound
    assert(context);
    void* repSocket = zsocket_new(context, ZMQ_REP);
    assert(repSocket);
    if (unlikely(zmq_bind(repSocket, tableOpenEndpoint))) {
        Logger logger(context, "Table open server");
        logger.critical("Table open server REP socket bind failed: " + std::string(zmq_strerror(errno)));
    }
    workerThread = new std::thread(tableOpenWorkerThread, context, repSocket, std::ref(databases), dbCompressionEnabled);
}

COLD TableOpenServer::~TableOpenServer() {
    //Create a temporary socket
    void* tempSocket = zsocket_new(context, ZMQ_REQ);
    zsocket_connect(tempSocket, tableOpenEndpoint);
    //Send an empty msg (signals the table open thread to stop)
    zmsg_t* stopMsg = zmsg_new();
    zframe_t* stopFrame = zframe_new_zero_copy(NULL, 0, doNothingFree, NULL);
    zmsg_add(stopMsg, stopFrame);
    zmsg_send(&stopMsg, tempSocket); //--> single zero byte frame
    //Receive the reply, ignore the data (--> thread has cleaned up & exited)
    zmsg_t* msg = zmsg_recv(tempSocket);
    if (likely(msg != NULL)) {
        zmsg_destroy(&msg);
    }
    //Cleanup
    zsocket_destroy(context, tempSocket);
    //The thread might take some time to exit, but we want to delete the thread ptr immediately
    //So, let it finish on his 
    workerThread->detach();
    //Delete the worker thread
    delete workerThread;
}

COLD TableOpenHelper::TableOpenHelper(zctx_t* context) : context(context), logger(context, "Table open client") {
    this->context = context;
    reqSocket = zsocket_new(context, ZMQ_REQ);
    if (unlikely(!reqSocket)) {
        logger.critical("Table open client REQ socket initialization failed: " + std::string(zmq_strerror(errno)));
    }
    if (unlikely(zmq_connect(reqSocket, tableOpenEndpoint))) {
        logger.critical("Table open client REQ socket connect failed: " + std::string(zmq_strerror(errno)));
    }
}

void COLD TableOpenHelper::openTable(uint32_t tableId,
        uint64_t lruCacheSize,
        uint64_t tableBlockSizeFrame,
        uint64_t writeBufferSize,
        uint64_t bloomFilterBitsPerKey,
        bool compressionEnabled) {
    TableOpenParameters parameters;
    parameters.lruCacheSize = lruCacheSize;
    parameters.tableBlockSize = tableBlockSizeFrame;
    parameters.writeBufferSize = writeBufferSize;
    parameters.bloomFilterBitsPerKey = bloomFilterBitsPerKey;
    parameters.compressionEnabled = compressionEnabled;
    //Just send a message containing the table index to the opener thread
    sendConstFrame("\x00", 1, reqSocket, logger, ZMQ_SNDMORE);
    sendBinary(tableId, reqSocket, logger, ZMQ_SNDMORE);
    sendFrame(&parameters, sizeof (TableOpenParameters), reqSocket, logger);
    //Wait for the reply (reply content is ignored)
    zmsg_t* msg = zmsg_recv(reqSocket); //Blocks until reply received
    if (msg != nullptr) {
        zmsg_destroy(&msg);
    }
}

void COLD TableOpenHelper::closeTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    zmsg_t* msg = zmsg_new();
    zmsg_addmem(msg, "\x01", 1);
    zmsg_addmem(msg, &index, sizeof (TableOpenHelper::IndexType));
    if (unlikely(zmsg_send(&msg, reqSocket) == -1)) {
        logger.critical("Close table message send failed: " + std::string(zmq_strerror(errno)));
    }
    //Wait for the reply (it's empty but that does not matter)
    msg = zmsg_recv(reqSocket); //Blocks until reply received
    if (unlikely(!msg)) {
        debugZMQError("Receive reply from table opener", errno);
    }
    zmsg_destroy(&msg);
}

void COLD TableOpenHelper::truncateTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    /**
     * Note: The reason this doesn't use CZMQ even if efficiency does not matter
     * is that repeated calls using CZMQ API cause SIGSEGV somewhere inside calloc.
     */
    sendConstFrame("\x02", 1, reqSocket, ZMQ_SNDMORE);
    sendFrame(&index, sizeof (IndexType), reqSocket);
    /*if (unlikely(zmsg_send(&msg, reqSocket) == -1)) {
        logger.critical("Truncate table message send failed: " + std::string(zmq_strerror(errno)));
    }*/
    //Wait for the reply (it's empty but that does not matter)
    recvAndIgnore(reqSocket);
    /*zmsg_t* msg = zmsg_recv(reqSocket); //Blocks until reply received
    if (unlikely(!msg)) {
        debugZMQError("Receive reply from table opener", errno);
    }
    zmsg_destroy(&msg);*/
}

COLD TableOpenHelper::~TableOpenHelper() {
    zsocket_destroy(context, reqSocket);
}
