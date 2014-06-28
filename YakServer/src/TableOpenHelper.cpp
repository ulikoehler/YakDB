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
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/cache.h>
#include <exception>
#include <fstream>
#include <dirent.h>
#include <unistd.h>
#include <stdint.h>
#include <map>
#include <memory>
#include "zutil.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "Tablespace.hpp"
#include "Logger.hpp"
#include "protocol.hpp"
#include "MergeOperators.hpp"
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

TableOpenParameters::TableOpenParameters(const ConfigParser& cfg) :
    lruCacheSize(cfg.defaultLRUCacheSize),
    tableBlockSize(cfg.defaultTableBlockSize),
    writeBufferSize(cfg.defaultWriteBufferSize),
    bloomFilterBitsPerKey(cfg.defaultBloomFilterBitsPerKey),
    compression(cfg.defaultCompression),
    mergeOperatorCode(cfg.defaultMergeOperator) {
}

void TableOpenParameters::parseFromParameterMap(std::map<std::string, std::string>& parameters) {
    if(parameters.count("LRUCacheSize")) {
        lruCacheSize = stoull(parameters["LRUCacheSize"]);
    }
    if(parameters.count("Blocksize")) {
        tableBlockSize = stoull(parameters["Blocksize"]);
    }
    if(parameters.count("WriteBufferSize")) {
        writeBufferSize = stoull(parameters["WriteBufferSize"]);
    }
    if(parameters.count("BloomFilterBitsPerKey")) {
        bloomFilterBitsPerKey = stoull(parameters["BloomFilterBitsPerKey"]);
    }
    if(parameters.count("CompressionMode")) {
        compression = compressionModeFromString(parameters["CompressionMode"]);
    }
    if(parameters.count("MergeOperator")) {
        mergeOperatorCode = parameters["MergeOperator"];
    }
}

void TableOpenParameters::getOptions(rocksdb::Options& options) {
    //For all numeric options: <= 0 --> disable / use default
    if(lruCacheSize > 0) {
        options.block_cache = rocksdb::NewLRUCache(lruCacheSize);
    }
    if (tableBlockSize > 0) {
        options.block_size = tableBlockSize;
    }
    if (writeBufferSize > 0) {
        options.write_buffer_size = writeBufferSize;
    }
    if(bloomFilterBitsPerKey > 0) {
        options.filter_policy = rocksdb::NewBloomFilterPolicy(bloomFilterBitsPerKey);
    }
    if(bloomFilterBitsPerKey > 0) {
        options.filter_policy = rocksdb::NewBloomFilterPolicy(bloomFilterBitsPerKey);
    }
    options.create_if_missing = true;
    //Compression. Default: Snappy
    options.compression = (rocksdb::CompressionType) compression;
    //Merge operator
    options.merge_operator = createMergeOperator(mergeOperatorCode);
}

void COLD TableOpenParameters::readTableConfigFile(const std::string& tableDir) {
    std::string cfgFileName = tableDir + ".cfg";
    if(fileExists(cfgFileName)) {
        string line;
        ifstream fin(cfgFileName.c_str());
        while(fin.good()) {
            fin >> line;
            size_t sepIndex = line.find_first_of('=');
            assert(sepIndex != string::npos);
            std::string key = line.substr(0, sepIndex);
            std::string value = line.substr(sepIndex+1);
            if(key == "LRUCacheSize") {
                lruCacheSize = stoull(value);
            } else if(key == "Blocksize") {
                tableBlockSize = stoull(value);
            } else if(key == "WriteBufferSize") {
                writeBufferSize = stoull(value);
            } else if(key == "BloomFilterBitsPerKey") {
                bloomFilterBitsPerKey = stoull(value);
            } else if(key == "CompressionMode") {
                compression = compressionModeFromString(value);
            } else {
                cerr << "Unknown key in table config file : " << key << " (= " << value << ")" << endl;
            }
        }
        fin.close();
    }
}

void COLD TableOpenParameters::writeToFile(const std::string& tableDir) {
    std::string cfgFileName = tableDir + ".cfg";
    ofstream fout(cfgFileName.c_str());
    if(lruCacheSize != std::numeric_limits<uint64_t>::max()) {
        fout << "LRUCacheSize=" << lruCacheSize << endl;
    }
    if(tableBlockSize != std::numeric_limits<uint64_t>::max()) {
        fout << "Blocksize=" << tableBlockSize << endl;
    }
    if(writeBufferSize != std::numeric_limits<uint64_t>::max()) {
        fout << "WriteBufferSize=" << writeBufferSize << endl;
    }
    if(bloomFilterBitsPerKey != std::numeric_limits<uint64_t>::max()) {
        fout << "BloomFilterBitsPerKey=" << bloomFilterBitsPerKey << endl;
    }
    fout << "CompressionMode=" << compressionModeToString(compression) << endl;
    fout << "MergeOperator=" << mergeOperatorCode << endl;
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
        //Parse the request type frame
        TableOperationRequestType requestType;
        if(!parseBinaryFrame(&requestType, sizeof(TableOperationRequestType), "table open request type frame", false, true)) {
            //Non-fatal errors
            if(yak_interrupted) {
                break;
            } else if(errno == EFSM) {
                //A previous error might have screwed the send/receive order.
                //Receiving failed, so we need to send to restore it.
                logger.warn("Internal FSM error, recovering by sending error frame");
                if (unlikely(zmq_send_const(processorInputSocket, "\x11", 1, 0) == -1)) {
                    logMessageSendError("FSM restore state message (error recovery)", logger);
                }
            }
            //The requester waits for a reply. This MIGHT lead to crashes,
            // but we prefer fail-fast here. Any crash that can be caused by
            // external sources is considered a bug.
            disposeRemainingMsgParts();
            zmq_send_const(processorInputSocket, nullptr, 0, 0);
            continue;
        }
        //Handle stop server requests
        if(unlikely(requestType == TableOperationRequestType::StopServer)) {
            //The STOP sender waits for a reply. Send an empty one.
            //NOTE: processorInputSocket and processorOutputSocket are the same!
            disposeRemainingMsgParts();
            zmq_send_const(processorInputSocket, nullptr, 0, 0);
            break;
        }
        //Receive table number frame
        uint32_t tableIndex;
        if(!parseUint32Frame(tableIndex, "table id frame", false)) {
                //See above for detailed comment on err handling here
            disposeRemainingMsgParts();
            zmq_send_const(processorInputSocket, nullptr, 0, 0);
            continue;
        }
        //Do the operation, depending on the request type
        if (requestType == TableOperationRequestType::OpenTable) { //Open table
            bool ok = true; //Set to false if error occurs
            //Extract parameters
            TableOpenParameters parameters(configParser); //Initialize with defaults == unset
            map<string, string> parameterMap;
            if(!receiveMap(parameterMap, "table open parameter map", false)) {
                //See above for detailed comment on err handling here
                disposeRemainingMsgParts();
                zmq_send_const(processorInputSocket, nullptr, 0, 0);
                continue;
            }
            //NOTE: We can't actually insert the config from parameterMap into
            // parameters, because it needs to take precedence to the table config file
            //Resize tablespace if neccessary
            if (databases.size() <= tableIndex) {
                databases.reserve(tableIndex + 16); //Avoid scaling superlineary
            }
            //Open the table only if it hasn't been opened yet, else just ignore the request
            if (databases[tableIndex] == nullptr) {
                std::string tableDir = configParser.tableSaveFolder + std::to_string(tableIndex);
                //Override default values with the last values from the table config file, if any
                parameters.readTableConfigFile(tableDir);
                //Override default + config with custom open parameters, if any
                parameters.parseFromParameterMap(parameterMap);
                //Process the config options
                logger.info("Creating/opening table #" + std::to_string(tableIndex));
                logger.trace("Opened table #" + std::to_string(tableIndex) + " with compression mode " + compressionModeToString(parameters.compression));
                //NOTE: Any option that has not been set up until now is now used from the config default
                rocksdb::Options options;
                parameters.getOptions(options);
                //Open the table
                rocksdb::Status status = rocksdb::DB::Open(options, tableDir.c_str(), &databases[tableIndex]);
                if (unlikely(!status.ok())) {
                    std::string errorDescription = "Error while trying to open table #"
                        + std::to_string(tableIndex) + " in directory " + tableDir
                        + ": " + status.ToString();
                    logger.error(errorDescription);
                    //Send error reply
                    std::string errorReplyString = "\x10" + errorDescription;
                    if (unlikely(zmq_send(processorInputSocket, errorReplyString.data(), errorReplyString.size(), 0) == -1)) {
                        logMessageSendError("table open error reply", logger);
                    }
                }
                //Write the persistent config data
                parameters.writeToFile(tableDir);
                //Send ACK reply
                if(ok) {
                    if (unlikely(zmq_send_const(processorInputSocket, "\x00", 1, 0) == -1)) {
                        logMessageSendError("table open (success) reply", logger);
                    }
                }
            } else {
                //Send "no action needed" reply
                if (unlikely(zmq_send_const(processorInputSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table open (no action needed) reply", logger);
                }
            }
        } else if (requestType == TableOperationRequestType::CloseTable) { //Close table
            if (databases.size() <= tableIndex || databases[tableIndex] == nullptr) {
                if (unlikely(zmq_send_const(processorInputSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table close reply", logger);
                }
            } else {
                rocksdb::DB* db = databases[tableIndex];
                databases[tableIndex] = nullptr; //Erase map entry as early as possible
                delete db;
                if (unlikely(zmq_send_const(processorInputSocket, "\x00", 1, 0) == -1)) {
                    logMessageSendError("table close (success) reply", logger);
                }
            }

        } else if (requestType == TableOperationRequestType::TruncateTable) { //Close & truncate
            uint8_t responseCode = 0x00;
            //Close if not already closed
            if (!(databases.size() <= tableIndex || databases[tableIndex] == nullptr)) {
                rocksdb::DB* db = databases[tableIndex];
                databases[tableIndex] = nullptr;
                delete db;
            }
            /**
             * Truncate, based on the assumption RocksDB only creates files,
             * but no subdirectories.
             *
             * We don't want to introduce a boost::filesystem dependency here,
             * so this essentially rm -rf, with no support for nested dirs.
             */
            DIR *dir;
            string dirname = configParser.tableSaveFolder + std::to_string(tableIndex);
            struct dirent *ent;
            if ((dir = opendir(dirname.c_str())) != nullptr) {
                while ((ent = readdir(dir)) != nullptr) {
                    //Skip . and ..
                    if (strcmp(".", ent->d_name) == 0 || strcmp("..", ent->d_name) == 0) {
                        continue;
                    }
                    std::string fullFileName = dirname + "/" + std::string(ent->d_name);
                    logger.trace("Truncating DB: Deleting " + fullFileName);
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
            if (unlikely(zmq_send_const(processorInputSocket, "\x00", 1, 0) == -1)) {
                logMessageSendError("table truncate (success) reply", logger);
            }
        } else {
            logger.error("Internal protocol error: Table open server received unkown request type: " + std::to_string((uint8_t)requestType));
            //Reply with 'unknown protocol' error code
            if (unlikely(zmq_send_const(processorInputSocket, "\x11", 1, 0) == -1)) {
                logMessageSendError("request type unknown reply", logger);
            }
        }
    }
    //Cleanup
    zmq_msg_close(&frame);
    //if(!yak_interrupted) {
    logger.debug("Stopping table open server");
    //}
    //We received an exit msg, cleanupzmq_bind(
    zmq_close(processorInputSocket);
}

COLD TableOpenServer::TableOpenServer(void* context,
                    ConfigParser& configParserParam,
                    std::vector<rocksdb::DB*>& databasesParam)
: AbstractFrameProcessor(context, ZMQ_REP, "Table open server"),
configParser(configParserParam),
databases(databasesParam) {
    //Bind socket to internal endpoint
    if(zmq_bind(processorInputSocket, tableOpenEndpoint) == -1) {
    }
    //We need to bind the inproc transport synchronously in the main thread because zmq_connect required that the endpoint has already been bound
    assert(processorInputSocket);
    //NOTE: The child thread will now own processorInputSocket. It will destroy it on exit!
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
        void* tempSocket = zmq_socket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
        if(unlikely(tempSocket == nullptr)) {
            logOperationError("trying to connect to table open server", logger);
            return;
        }
        //Send a stop server msg (signals the table open thread to stop)
        if(sendTableOperationRequest(tempSocket, TableOperationRequestType::StopServer) == -1) {
            logMessageSendError("table server stop message", logger);
        }
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

COLD TableOpenHelper::TableOpenHelper(void* context, ConfigParser& cfg) :
    context(context), cfg(cfg), logger(context, "Table open client") {
    reqSocket = zmq_socket_new_connect(context, ZMQ_REQ, tableOpenEndpoint);
    if (unlikely(!reqSocket)) {
        logger.critical("Table open client REQ socket initialization failed: " + std::string(zmq_strerror(errno)));
    }
}

void COLD TableOpenHelper::openTable(uint32_t tableId, void* paramSrcSock) {
    //Note that the socket could be NULL <=> no parameters
    //Just send a message containing the table index to the opener thread
    if(sendTableOperationRequest(reqSocket, TableOperationRequestType::OpenTable, ZMQ_SNDMORE) == -1) {
        logMessageSendError("table open header message", logger);
    }
    //Determine if there are ANY key/value parameters to be sent
    bool haveParameters = paramSrcSock != nullptr && socketHasMoreFrames(paramSrcSock);
    //Send table no -- last frame if there are no parameters
    sendBinary(tableId, reqSocket, logger, "Table ID", haveParameters ? ZMQ_SNDMORE : 0);
    //Send parameters if there is any socket to proxy them from
    if(haveParameters) {
        if(unlikely(zmq_proxy_single(paramSrcSock, reqSocket) == -1)) {
            logger.critical("Table open client parameter transfer failed: " + std::string(zmq_strerror(errno)));
        }
    }
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
    recvAndIgnore(reqSocket, logger);
}

COLD TableOpenHelper::~TableOpenHelper() {
    zmq_close(reqSocket);
}
