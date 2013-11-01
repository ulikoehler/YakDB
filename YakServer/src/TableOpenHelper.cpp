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

/*
 * Utility macros for error handling code dedup and
 * to make the code shorter.
 * These shall only be used inside a LogServer msg loop.
 * 
 * FIXME Currently these don't send replies, so the socket is not in the correct state.
 * This is being worked around by checking it in the first recv call, but
 * it would be cleaner if these macros would do it themselves.
 */
#define CHECK_MORE_FRAMES(msg, socket, description)\
    if(unlikely(!zmq_msg_more(&msg))) {\
        logger.critical("Only received " + std::string(description) + ", missing further frames");\
        continue;\
    }
#define CHECK_NO_MORE_FRAMES(msg, socket, description)\
    if(unlikely(zmq_msg_more(&msg))) {\
        logger.critical("Expected no more frames after " + std::string(description) + ", but MORE flag is set");\
        continue;\
    }
#define RECEIVE_CHECK_ERROR(msg, socket, description)\
    if(unlikely(zmq_msg_recv(&msg, socket, 0) == -1)) {\
        if(yak_interrupted) {\
            break;\
        } else {\
            logger.critical("Error while receiving "\
                + std::string(description)+ ": " + std::string(zmq_strerror(errno)));\
            continue;\
        }\
    }
//Check binary size of a frame.
#define CHECK_SIZE(frame, expectedSize)\
    if(unlikely(expectedSize != zmq_msg_size(&frame))) {\
        logger.critical(\
            "Received log level message of invalid size: expected size "\
            + std::to_string(expectedSize) + ", got size "\
            + std::to_string(zmq_msg_size(&frame)));\
        continue;\
    }

using namespace std;

struct PACKED TableOpenParameters  {
    uint64_t lruCacheSize;
    uint64_t tableBlockSize;
    uint64_t writeBufferSize;
    uint64_t bloomFilterBitsPerKey;
    bool compressionEnabled;
    
    /**
     * Convert this instance to a LevelDB table open parameter set
     */
    leveldb::Options getOptions(ConfigParser& configParser) {
        leveldb::Options options;
        if (lruCacheSize != std::numeric_limits<uint64_t>::max()) {
            if(lruCacheSize > 0) { //0 --> disable
                options.block_cache = leveldb::NewLRUCache(lruCacheSize);
            }
        } else {
            //Use a small LRU cache per default, because OS cache doesn't cache uncompressed data
            // , so it's really slow in random-access-mode for uncompressed data
            options.block_cache = leveldb::NewLRUCache(configParser.getDefaultTableBlockSize());
        }
        if (tableBlockSize != std::numeric_limits<uint64_t>::max()) {
            options.block_size = tableBlockSize;
        } else { //Default table block size (= more than LevelDB default)
            //256k, LevelDB default = 4k
            options.block_size = configParser.getDefaultTableBlockSize(); 
        }
        if (writeBufferSize != std::numeric_limits<uint64_t>::max()) {
            options.write_buffer_size = writeBufferSize;
        } else {
            //To counteract slow writes on slow HDDs, we now use a WB per default
            //The default is tuned not to use too much buffer memory at once
            options.write_buffer_size = configParser.getDefaultWriteBufferSize(); //64 Mibibytes
        }
        if (bloomFilterBitsPerKey != std::numeric_limits<uint64_t>::max()) {
            //0 --> disable
            if(lruCacheSize > 0) {
                options.filter_policy
                        = leveldb::NewBloomFilterPolicy(bloomFilterBitsPerKey);
            }
        } else {
            if(configParser.getDefaultBloomFilterBitsPerKey() > 0) {
                options.filter_policy
                        = leveldb::NewBloomFilterPolicy(
                            configParser.getDefaultBloomFilterBitsPerKey());
            }
        }
        options.create_if_missing = true;
        options.compression = (compressionEnabled ? leveldb::kSnappyCompression : leveldb::kNoCompression);
        return options;
    }
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

enum class TableOperationRequestType : uint8_t {
    StopServer = 0,
    OpenTable = 1,
    CloseTable = 2,
    TruncateTable = 3
};

/**
 * Send a table operation request
 */
static int sendTableOperationRequest(void* socket, TableOperationRequestType requestType, int flags = 0) {
    return zmq_send(socket, &requestType, sizeof(TableOperationRequestType), flags);
}

/**
 * Main function for table open worker thread.
 * 
 * Msg format: 
 *      - Frame 1: Single byte: a TableOperationRequestType instance
 *      - A 4-byte frame containing the binary ID
 *      - Optional: More frames 
 * 
 * A single-byte single-frame-message
 * is sent back after the request has been processed:
 *   Code \x00: Success, no error
 *   Code \x01: Success, no action neccessary
 *   Code \x10: Error, additional bytes contain error message
 *   Code \x11: Error, unknown request type
 */
void TableOpenServer::tableOpenWorkerThread() {
    logger.trace("Table open thread starting...");
    //Initialize frames to be received
    zmq_msg_t frame;
    zmq_msg_init(&frame);
    //Main worker event loop
    while (true) {
        //Receive the header frame == request type
        if(unlikely(zmq_msg_recv(&frame, repSocket, 0) == -1)) {
            if(yak_interrupted) {
                break;
            } else if(errno == EFSM) {
                //A previous error might have screwed the send/receive order.
                //Receiving failed, so we need to send to restore it.
                if (unlikely(zmq_send_const(repSocket, "\x11", 1, 0) == -1)) {
                    logMessageSendError("FSM restore state message (error recovery)", logger);
                }
            } else {
                logMessageRecvError("table operation request type", logger);
                continue;
            }
        }
        
        CHECK_SIZE(frame, sizeof(TableOperationRequestType));
        TableOperationRequestType requestType
            = extractBinary<TableOperationRequestType>(&frame);
        if(unlikely(requestType == TableOperationRequestType::StopServer)) {
            //The STOP sender waits for a reply. Send an empty one.
            zmq_send_const(repSocket, nullptr, 0, 0);
            break;
        }
        CHECK_MORE_FRAMES(frame, repSocket, "message type frame");
        //Receive table number frame
        RECEIVE_CHECK_ERROR(frame, repSocket, "table id frame");
        CHECK_SIZE(frame, sizeof(uint32_t));
        uint32_t tableIndex = extractBinary<uint32_t>(&frame);
        //Do the operation, depending on the request type
        if (requestType == TableOperationRequestType::OpenTable) { //Open table
            bool ok = true; //Set to false if error occurs
            CHECK_MORE_FRAMES(frame, repSocket, "table id frame");
            RECEIVE_CHECK_ERROR(frame, repSocket, "table open parameters frame");
            CHECK_SIZE(frame, sizeof(TableOpenParameters));
            CHECK_NO_MORE_FRAMES(frame, repSocket, "table parameters frame");
            //Extract parameters
            TableOpenParameters parameters;
            memcpy(&parameters, zmq_msg_data(&frame), sizeof(TableOpenParameters));
            //Resize tablespace if neccessary
            if (databases.size() <= tableIndex) {
                databases.reserve(tableIndex + 16); //Avoid too large vectors
            }
            //Create the table only if it hasn't been created yet, else just ignore the request
            if (databases[tableIndex] == nullptr) {
                std::string tableDir = "tables/" + std::to_string(tableIndex);
                //Override default values with the last values from the table config file, if any
                readTableConfigFile(tableDir, parameters);
                //Process the config options
                logger.info("Creating/opening table #" + std::to_string(tableIndex));
                leveldb::Options options = parameters.getOptions(configParser);
                //Open the table
                leveldb::Status status = leveldb::DB::Open(options, tableDir.c_str(), &databases[tableIndex]);
                if (unlikely(!status.ok())) {
                    std::string errorDescription = "Error while trying to open table #"
                        + std::to_string(tableIndex) + " in directory " + tableDir
                        + ": " + status.ToString();
                    logger.error(errorDescription);
                    //Send error reply
                    std::string errorReplyString = "\x10" + errorDescription;
                    if (unlikely(zmq_send(repSocket, errorReplyString.data(), errorReplyString.size(), 0) == -1)) {
                        logMessageSendError("table open error reply", logger);
                    }
                }
                //Add the cache to the map
                cacheMap[databases[tableIndex]] = options.block_cache;
                //Write the persistent config data
                writeTableConfigFile(tableDir, parameters);
                //Send ACK reply
                if(ok) {
                    if (unlikely(zmq_send_const(repSocket, "\x00", 1, 0) == -1)) {
                    logMessageSendError("table open (success) reply", logger);
                    }
                }
            } else {
                //Send "no action needed" reply
                if (unlikely(zmq_send_const(repSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table open (no action needed) reply", logger);
                }
            }
        } else if (requestType == TableOperationRequestType::CloseTable) { //Close table
            if (databases.size() <= tableIndex || databases[tableIndex] == nullptr) {
                if (unlikely(zmq_send_const(repSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table close reply", logger);
                }
            } else {
                leveldb::DB* db = databases[tableIndex];
                databases[tableIndex] = nullptr; //Erase map entry as early as possible
                delete db;
                //Delete the cache, if any
                leveldb::Cache* cache = cacheMap[db];
                cacheMap.erase(db);
                delete cache;
                if (unlikely(zmq_send_const(repSocket, "\x00", 1, 0) == -1)) {
                    logMessageSendError("table close (success) reply", logger);
                }
            }
            
        } else if (requestType == TableOperationRequestType::TruncateTable) { //Close & truncate
            uint8_t responseCode = 0x00;
            //Close if not already closed
            if (!(databases.size() <= tableIndex || databases[tableIndex] == nullptr)) {
                leveldb::DB* db = databases[tableIndex];
                databases[tableIndex] = nullptr;
                delete db;
            }
            /**
             * Truncate, based on the assumption LevelDB only creates files,
             * but no subdirectories.
             * 
             * We don't want to introduce a boost::filesystem dependency here,
             * so essentially rmr has been implemented by ourselves.
             */
            DIR *dir;
            string dirname = "tables/" + std::to_string(tableIndex);
            struct dirent *ent;
            if ((dir = opendir(dirname.c_str())) != nullptr) {
                while ((ent = readdir(dir)) != nullptr) {
                    //Skip . and ..
                    if (strcmp(".", ent->d_name) == 0 || strcmp("..", ent->d_name) == 0) {
                        continue;
                    }
                    string fullFileName = dirname + "/" + std::string(ent->d_name);
                    logger.trace(fullFileName);
                    unlink(fullFileName.c_str());
                }
                closedir(dir);
                responseCode = 0x00; //Success, no error
            } else {
                //For now we just assume, error means it does not exist
                logger.trace("Tried to truncate " + dirname + " but it does not exist");
                responseCode = 0x01; //Sucess, deletion not neccesary
            }
            
            //Now remove the table directory itself (it should be empty now)
            //Errors (e.g. for nonexistent dirs) do not exist
            rmdir(dirname.c_str());
            logger.debug("Truncated table in " + dirname);
            if (unlikely(zmq_send_const(repSocket, "\x00", 1, 0) == -1)) {
                logMessageSendError("table truncate (success) reply", logger);
            }
        } else {
            logger.error("Internal protocol error: Table open server received unkown request type: " + std::to_string((uint8_t)requestType));
            //Reply with 'unknown protocol' error code
            if (unlikely(zmq_send_const(repSocket, "\x11", 1, 0) == -1)) {
                logMessageSendError("request type unknown reply", logger);
            }
        }
    }
    //Cleanup
    zmq_msg_close(&frame);
    //if(!yak_interrupted) {
    logger.debug("Stopping table open server");
    //}
    //We received an exit msg, cleanup
    zmq_close(repSocket);
}

COLD TableOpenServer::TableOpenServer(void* context, 
                    ConfigParser& configParserParam,
                    std::vector<leveldb::DB*>& databasesParam)
: context(context),
logger(context, "Table open server"),
configParser(configParserParam),
repSocket(zmq_socket_new_bind(context, ZMQ_REP, tableOpenEndpoint)),
databases(databasesParam),
cacheMap() {
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
    //Delete all caches
    for(auto pair : cacheMap) {
        if(pair.second != nullptr) {
            delete pair.second;
        }
    }
    //Terminate the thread
    terminate();
}

void COLD TableOpenServer::terminate() {
    if(workerThread) {
        //Create a temporary socket
        void* tempSocket = zmq_socket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
        if(unlikely(tempSocket == nullptr)) {
            logger.error("Can't send STOP msg to table open server, because connection attempt failed: "
                         + std::string(zmq_strerror(errno)));
            return;
        }
        //Send a stop server msg (signals the table open thread to stop)
        if(sendTableOperationRequest(tempSocket, TableOperationRequestType::StopServer) == -1) {
            logMessageSendError("table server stop message", logger);
        }
        sendConstFrame("\x00", 1, tempSocket, logger, "Table open server STOP msg");
        //Receive the reply, ignore the data (--> thread has received msg and is terminating)
        receiveAndIgnoreFrame(tempSocket, logger, "Table open server STOP msg reply");
        //Wait for the thread to finish
        workerThread->join();
        delete workerThread;
        workerThread = nullptr;
        //Cleanup
        zmq_close(tempSocket);
        //Cleanup EVERYTHING zmq-related immediately
    }
    logger.terminate();
}

COLD TableOpenHelper::TableOpenHelper(void* context) : context(context), logger(context, "Table open client") {
    this->context = context;
    reqSocket = zmq_socket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
    if (unlikely(!reqSocket)) {
        logger.critical("Table open client REQ socket initialization failed: " + std::string(zmq_strerror(errno)));
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
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::OpenTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table open message", logger);
    }
    sendBinary(tableId, reqSocket, logger, "Table ID", ZMQ_SNDMORE);
    sendFrame(&parameters, sizeof (TableOpenParameters), reqSocket, logger, "Table open parameters");
    //Wait for the reply (reply content is ignored)
    zmq_recv(reqSocket, nullptr, 0, 0); //Blocks until reply received
}

void COLD TableOpenHelper::closeTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::CloseTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table close message", logger);
    }
    sendFrame(&index, sizeof (TableOpenHelper::IndexType), reqSocket, logger, "Close table request table index frame", ZMQ_SNDMORE);
    //Wait for the reply (it's empty but that does not matter)
    zmq_msg_t reply;
    zmq_msg_init(&reply);
    //Blocks until reply received == until request processed
    if (unlikely(zmq_msg_recv(&reply, reqSocket, 0) == -1)) {
        logger.error("Close table receive failed: " + std::string(zmq_strerror(errno)));
    }
    zmq_msg_close(&reply);
}

void COLD TableOpenHelper::truncateTable(TableOpenHelper::IndexType index) {
    /**
     * Note: The reason this doesn't use CZMQ even if efficiency does not matter
     * is that repeated calls using CZMQ API cause SIGSEGV somewhere inside calloc.
     */
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::TruncateTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table truncate message", logger);
    }
    sendFrame(&index, sizeof (IndexType), reqSocket, logger, "Table index");
    //Wait for the reply (it's empty but that does not matter)
    recvAndIgnore(reqSocket);
}

COLD TableOpenHelper::~TableOpenHelper() {
    zmq_close(reqSocket);
}
