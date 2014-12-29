#include "TableOpenServer.hpp"

#include <fstream>
#include <dirent.h>
#include <unistd.h>
#include <stdint.h>
#include <iostream>
#include <map>
#include <string>
#include <zmq.h>

#include "TableOpenHelper.hpp"
#include "MergeOperators.hpp"
#include "macros.hpp"
#include "endpoints.hpp"
#include "zutil.hpp"

using namespace std;

COLD TableOpenServer::TableOpenServer(void* context,
                    ConfigParser& configParserParam,
                    Tablespace& tablespaceParam)
: AbstractFrameProcessor(context, ZMQ_REP, "Table open server"),
configParser(configParserParam),
tablespace(tablespaceParam) {
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
            //Extract parameters
            TableOpenParameters parameters(configParser); //Initialize with defaults == unset
            std::map<std::string, std::string> parameterMap;
            if(!receiveMap(parameterMap, "table open parameter map", false)) {
                //See above for detailed comment on err handling here
                disposeRemainingMsgParts();
                zmq_send_const(processorInputSocket, nullptr, 0, 0);
                continue;
            }
            //NOTE: We can't actually insert the config from parameterMap into
            // parameters, because it needs to take precedence to the table config file
            tablespace.ensureSize(tableIndex);
            //Open the table only if it hasn't been opened yet, else just ignore the request
            if (!tablespace.isTableOpen(tableIndex)) {
                std::string tableDir = configParser.getTableDirectory(tableIndex);
                //Override default values with the last values from the table config file, if any
                parameters.readTableConfigFile(configParser, tableIndex);
                //Override default + config with custom open parameters, if any
                parameters.parseFromParameterMap(parameterMap);
                //NOTE: Any option that has not been set up until now is now used from the config default
                rocksdb::Options options;
                options.allow_mmap_reads = configParser.useMMapReads;
                options.allow_mmap_writes = configParser.useMMapWrites;
                parameters.getOptions(options);
                //Open the table
                rocksdb::Status status = rocksdb::DB::Open(options, tableDir.c_str(),
                    tablespace.getTablePointer(tableIndex));
                if (likely(status.ok())) {
                    //Write the persistent config data
                    parameters.writeToFile(configParser, tableIndex);
                    //Send ACK reply
                    if (unlikely(zmq_send_const(processorInputSocket, "\x00", 1, 0) == -1)) {
                        logMessageSendError("table open (success) reply", logger);
                    }
                    //Log success
                    logger.info(std::string("Opened table #")
                        + std::to_string(tableIndex)
                        + " compression mode = "
                        + compressionModeToString(parameters.compression)
                        + " using merge operator "
                        + options.merge_operator->Name());
                    //Set merge operator trivial flag
                    tablespace.setMergeRequired(tableIndex,
                        !isReplaceMergeOperator(parameters.mergeOperatorCode));
                } else { //status == not ok
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
            } else { // Table already open
                //Send "no action needed" reply
                if (unlikely(zmq_send_const(processorInputSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table open (no action needed) reply", logger);
                }
            }
        } else if (requestType == TableOperationRequestType::CloseTable) { //Close table
            //No need to close if table is not open
            if (tablespace.isTableOpen(tableIndex)) {
                if (unlikely(zmq_send_const(processorInputSocket, "\x01", 1, 0) == -1)) {
                    logMessageSendError("table close reply", logger);
                }
            } else { //Table is open --> need to close
                delete tablespace.eraseAndGetTableEntry(tableIndex);
                if (unlikely(zmq_send_const(processorInputSocket, "\x00", 1, 0) == -1)) {
                    logMessageSendError("table close (success) reply", logger);
                }
            }

        } else if (requestType == TableOperationRequestType::TruncateTable) { //Close & truncate
            uint8_t responseCode = 0x00;
            //Close if not already closed
            if (tablespace.isTableOpen(tableIndex)) {
                delete tablespace.eraseAndGetTableEntry(tableIndex);
            }
            /**
             * Truncate, based on the assumption RocksDB only creates files,
             * but no subdirectories.
             *
             * We don't want to introduce a boost::filesystem dependency here,
             * so this essentially rm -rf, with no support for nested dirs.
             */
            DIR *dir;
            std::string dirname = configParser.getTableDirectory(tableIndex);
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
            if (unlikely(zmq_send_const(processorInputSocket, (char*)&responseCode, 1, 0) == -1)) {
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
