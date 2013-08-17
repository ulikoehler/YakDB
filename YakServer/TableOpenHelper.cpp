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
#include <fstream>
#include <dirent.h>
#include <unistd.h>
#include <stdint.h>
#include "zutil.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "Tablespace.hpp"
#include "Logger.hpp"
#include "protocol.hpp"
#include "FileUtils.hpp"

using namespace std;

struct PACKED TableOpenParameters  {
    uint64_t lruCacheSize;
    uint64_t tableBlockSize;
    uint64_t writeBufferSize;
    uint64_t bloomFilterBitsPerKey;
    bool compressionEnabled;
};


/**
* If the table config file for the table exists,
* read it and save it in an options object.
* 
* If the values are not set to DEFAULT in the options parameter,
* they are not overwritten (--> users choice has precedence)
* 
* Compression is always set to the user-supplied value.
*/
static void readTableConfigFile(const std::string& tableDirName, TableOpenParameters& options) {
    string cfgFileName = tableDirName + ".cfg";
    if(fexists(cfgFileName)) {
        string line;
        ifstream fin(cfgFileName.c_str());
        while(fin.good()) {
            fin >> line;
            size_t sepIndex = line.find_first_of('=');
            assert(sepIndex != string::npos);
            string key = line.substr(0, sepIndex);
            string value = line.substr(sepIndex+1);
            if(key == "lruCacheSize") {
                if(options.lruCacheSize == UINT64_MAX) {
                    options.lruCacheSize = stoull(value);
                }
            } else if(key == "tableBlockSize") {
                if(options.lruCacheSize == UINT64_MAX) {
                    options.tableBlockSize = stoull(value);
                }
            } else if(key == "writeBufferSize") {
                if(options.lruCacheSize == UINT64_MAX) {
                    options.writeBufferSize = stoull(value);
                }
            } else if(key == "bloomFilterBitsPerKey") {
                if(options.lruCacheSize == UINT64_MAX) {
                    options.bloomFilterBitsPerKey = stoull(value);
                }
            } else {
                cerr << "ERRR : " << key << " --- " << value << endl;
            }
        }
        fin.close();
    }
}

/**
 * Write a table config file that is used to persistently store the table open options.
 */
static void writeTableConfigFile(const std::string& tableDirName, const TableOpenParameters& options) {
    string cfgFileName = tableDirName + ".cfg";
    //Don't open file if nothing would be written
    if(!(
        options.lruCacheSize != UINT64_MAX || 
        options.tableBlockSize != UINT64_MAX ||
        options.writeBufferSize != UINT64_MAX ||
        options.bloomFilterBitsPerKey != UINT64_MAX
    )) {
        return;
    }
    ofstream fout(cfgFileName.c_str());
    if(options.lruCacheSize != UINT64_MAX) {
        fout << "lruCacheSize=" << options.lruCacheSize << endl;
    }
    if(options.tableBlockSize != UINT64_MAX) {
        fout << "tableBlockSize=" << options.tableBlockSize << endl;
    }
    if(options.writeBufferSize != UINT64_MAX) {
        fout << "writeBufferSize=" << options.writeBufferSize << endl;
    }
    if(options.bloomFilterBitsPerKey != UINT64_MAX) {
        fout << "bloomFilterBitsPerKey=" << options.bloomFilterBitsPerKey << endl;
    }
    fout.close();
}

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
void HOT TableOpenServer::tableOpenWorkerThread() {
    logger.trace("Table open thread starting...");
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
        //Check for a STOP msg
        if (zframe_size(msgTypeFrame) == 0) {
            //Send back the message and exit the loop
            zmsg_send(&msg, repSocket);
            break;
        }
        //It's definitely no STOP frame, so there must be some data
        assert(zframe_size(msgTypeFrame) == 1);
        uint8_t msgType = (uint8_t) zframe_data(msgTypeFrame)[0];
        //Table ID
        zframe_t* tableIdFrame = zmsg_next(msg);
        assert(tableIdFrame);
        size_t frameSize = zframe_size(tableIdFrame);
        assert(frameSize == sizeof (TableOpenHelper::IndexType));
        uint32_t tableIndex = extractBinary<uint32_t>(tableIdFrame);
        if (msgType == 0x00) { //Open table
            //Extract parameters (this is NOT the parameter structure from 
            zframe_t* parametersFrame = zmsg_next(msg);
            assert(parametersFrame);
            TableOpenParameters* parameters = (TableOpenParameters*) zframe_data(parametersFrame);
            //Resize if neccessary
            if (databases.size() <= tableIndex) {
                databases.reserve(tableIndex + 16); //Avoid large vectors
            }
            //Create the table only if it hasn't been created yet, else just ignore the request
                if (databases[tableIndex] == NULL) {
                std::string tableDir = "tables/" + std::to_string(tableIndex);
                //Override default values with the last values from the table config file, if any
                readTableConfigFile(tableDir, *parameters);
                //Process the config options
                logger.info("Creating/opening table #" + std::to_string(tableIndex));
                leveldb::Options options;
                options.create_if_missing = true;
                options.compression = (parameters->compressionEnabled ? leveldb::kSnappyCompression : leveldb::kNoCompression);
                //Set optional parameters
                if (parameters->lruCacheSize != UINT64_MAX) {
                    //0 --> disable
                    if(parameters->lruCacheSize > 0) {
                        options.block_cache = leveldb::NewLRUCache(parameters->lruCacheSize);
                    }
                } else {
                    //Use a small LRU cache per default, because OS cache doesn't cache uncompressed data
                    // , so it's really slow in random-access-mode for uncompressed data
                    options.block_cache = leveldb::NewLRUCache(configParser.getDefaultTableBlockSize());
                }
                if (parameters->tableBlockSize != UINT64_MAX) {
                    options.block_size = parameters->tableBlockSize;
                } else { //Default table block size (= more than LevelDB default)
                    //256k, LevelDB default = 4k
                    options.block_size = configParser.getDefaultTableBlockSize(); 
                }
                if (parameters->writeBufferSize != UINT64_MAX) {
                    options.write_buffer_size = parameters->writeBufferSize;
                } else {
                    //To counteract slow writes on slow HDDs, we now use a WB per default
                    //The default is tuned not to use too much buffer memory at once
                    options.write_buffer_size = configParser.getDefaultWriteBufferSize(); //64 Mibibytes
                }
                if (parameters->bloomFilterBitsPerKey != UINT64_MAX) {
                    //0 --> disable
                    if(parameters->lruCacheSize > 0) {
                        options.filter_policy
                                = leveldb::NewBloomFilterPolicy(parameters->bloomFilterBitsPerKey);
                    }
                } else {
                    if(configParser.getDefaultBloomFilterBitsPerKey() > 0) {
                        options.filter_policy
                                = leveldb::NewBloomFilterPolicy(
                                    configParser.getDefaultBloomFilterBitsPerKey());
                    }
                }
                //Open the table
                leveldb::Status status = leveldb::DB::Open(options, tableDir.c_str(), &databases[tableIndex]);
                if (unlikely(!status.ok())) {
                    logger.error("Error while trying to open table #" + std::to_string(tableIndex) + " in directory " + tableDir + ": " + status.ToString());
                }
                //Write the persistent config data
                writeTableConfigFile(tableDir, *parameters);
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
            sendConstFrame(&responseCode, 1, repSocket, logger, "Truncate response");
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
    if(!zctx_interrupted) {
        logger.debug("Stopping table open server");
    }
    //We received an exit msg, cleanup
    zsocket_destroy(context, repSocket);
}

COLD TableOpenServer::TableOpenServer(zctx_t* context, 
                    ConfigParser& configParserParam,
                    std::vector<leveldb::DB*>& databasesParam,
                    bool dbCompressionEnabledParam)
: context(context),
logger(context, "Table open server"),
configParser(configParserParam),
dbCompressionEnabled(dbCompressionEnabledParam),
repSocket(zsocket_new_bind(context, ZMQ_REP, tableOpenEndpoint)),
databases(databasesParam) {
    //We need to bind the inproc transport synchronously in the main thread because zmq_connect required that the endpoint has already been bound
    assert(repSocket);
    //NOTE: The child thread will now own repSocket. It will destroy it on exit!
    workerThread = new std::thread(std::mem_fun(&TableOpenServer::tableOpenWorkerThread), this);
}

COLD TableOpenServer::~TableOpenServer() {
    //Logging after the context has been terminated would cause memleaks
    if(workerThread) {
        logger.debug("Table open server terminating");
    }
    //Terminate the thread
    terminate();
}

void COLD TableOpenServer::terminate() {
    if(workerThread) {
        //Create a temporary socket
        void* tempSocket = zsocket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
        assert(tempSocket);
        //Send an empty msg (signals the table open thread to stop)
        sendEmptyFrameMessage(tempSocket);
        //Receive the reply, ignore the data (--> thread has received msg and is cleaning up)
        zmsg_t* msg = zmsg_recv(tempSocket);
        zmsg_destroy(&msg);
        //Wait for the thread to finish
        workerThread->join();
        delete workerThread;
        workerThread = nullptr;
        //Cleanup
        zsocket_destroy(context, tempSocket);
        //Cleanup EVERYTHING zmq-related immediately
    }
    logger.terminate();
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
    sendConstFrame("\x00", 1, reqSocket, logger, "Table opener header msg", ZMQ_SNDMORE);
    sendBinary(tableId, reqSocket, logger, "Table ID", ZMQ_SNDMORE);
    sendFrame(&parameters, sizeof (TableOpenParameters), reqSocket, logger, "Table open parameters");
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
    sendConstFrame("\x02", 1, reqSocket, logger, "Truncate header", ZMQ_SNDMORE);
    sendFrame(&index, sizeof (IndexType), reqSocket, logger, "Table index");
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
