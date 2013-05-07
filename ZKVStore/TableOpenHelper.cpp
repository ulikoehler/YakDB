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
#include <exception>
#include "zutil.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "Tablespace.hpp"
#include "Log.hpp"

using namespace std;

/**
 * Main function for table open worker thread.
 * 
 * Msg format:c
 *      - STOP THREAD msg: One empty frame
 *      - OPEN/CLOSE TABLE msg:
 *          - A 1-byte frame with content \x00 for open table and \x01 for close table
 *          - A 4-byte frame containing the binary ID
 */
static void tableOpenWorkerThread(zctx_t* context, void* repSocket, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) {
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
        //Check for a STOP msg
        if (frameSize == 0) {
            //Send back the message and exit the loop
            zmsg_send(&msg, repSocket);
            break;
        }
        //If it's not null, it must have the appropriate size
        assert(frameSize == sizeof (TableOpenHelper::IndexType));
        TableOpenHelper::IndexType index = *((TableOpenHelper::IndexType*)zframe_data(tableIdFrame));
        if (msgType == 0x00) { //Open table
            //Resize if neccessary
            if (databases.size() <= index) {
                databases.reserve(index + 16); //Avoid large vectors
            }
            //Create the table only if it hasn't been created yet, else just ignore the request
            if (databases[index] == NULL) {
                logger.info("Creating/opening table " + std::to_string(index));
                leveldb::Options options;
                options.create_if_missing = true;
                options.compression = (dbCompressionEnabled ? leveldb::kSnappyCompression : leveldb::kNoCompression);
                std::string tableName = "tables/" + std::to_string(index);
                leveldb::Status status = leveldb::DB::Open(options, tableName.c_str(), &databases[index]);
                if (unlikely(!status.ok())) {
                    logger.error("Error while trying to open database in " + tableName + ": " + status.ToString());
                }
            }
            //In order to improve performance, we reuse the existing frame, we only modify the first byte (additional bytes shall be ignored)
            zframe_data(tableIdFrame)[0] = 0x00; //0x00 == acknowledge, no error
            if (unlikely(zmsg_send(&msg, repSocket) == -1)) {
                logger.error("Communication error while trying to send table open reply: " + std::string(zmq_strerror(errno)));
            }
        } else if (msgType == 0x01) { //Close table
            if (databases.size() <= index || databases[index] == NULL) {
                zframe_data(tableIdFrame)[0] = 0x01; //0x01 == Table already closed
            } else {
                leveldb::DB* db = databases[index];
                databases[index] = NULL;
                delete db;
                zframe_data(tableIdFrame)[0] = 0x00; //0x00 == acknowledge, no error
            }
            if (zmsg_send(&msg, repSocket) == -1) {
                logger.error("Communication error while trying to send table open reply: " + std::string(zmq_strerror(errno)));
            }
        } else {
            logger.error("Internal protocol error: Table open server received unkown request type " + std::to_string(msgType));
        }
    }
    cout << "Stopping table open server" << endl;
    //We received an exit msg, cleanup
    zsocket_destroy(context, repSocket);
}

TableOpenServer::TableOpenServer(zctx_t* context, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled) : context(context) {
    //We need to bind the inproc transport synchronously in the main thread because zmq_connect required that the endpoint has already been bound
    assert(context);
    void* repSocket = zsocket_new(context, ZMQ_REP);
    assert(repSocket);
    if (unlikely(zmq_bind(repSocket, tableOpenEndpoint))) {
        Logger logger(context, "Table open server");
        logger.critical("Table open server REP socket bind failed: " + std::to_string(zmq_strerror(errno)));
    }
    workerThread = new std::thread(tableOpenWorkerThread, context, repSocket, std::ref(databases), dbCompressionEnabled);
}

TableOpenServer::~TableOpenServer() {
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

TableOpenHelper::TableOpenHelper(zctx_t* context) {
    this->context = context;
    reqSocket = zsocket_new(context, ZMQ_REQ);
    if (!reqSocket) {
        debugZMQError("Create table opener request socket", errno);
    }
    if (zmq_connect(reqSocket, tableOpenEndpoint)) {
        debugZMQError("Connect table opener request socket", errno);
    }
}

void TableOpenHelper::openTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    zmsg_t* msg = zmsg_new();
    zmsg_addmem(msg, "\x00", 1);
    zmsg_addmem(msg, &index, sizeof (TableOpenHelper::IndexType));
    if (unlikely(zmsg_send(&msg, reqSocket) == -1)) {
        debugZMQError("Sending table open request msg", errno);
    }
    //Wait for the reply (it's empty but that does not matter)
    msg = zmsg_recv(reqSocket); //Blocks until reply received
    if (msg != NULL) {
        zmsg_destroy(&msg);
    }
}

void TableOpenHelper::closeTable(TableOpenHelper::IndexType index) {
    //Just send a message containing the table index to the opener thread
    zmsg_t* msg = zmsg_new();
    zmsg_addmem(msg, "\x01", 1);
    zmsg_addmem(msg, &index, sizeof (TableOpenHelper::IndexType));
    if (zmsg_send(&msg, reqSocket) == -1) {
        debugZMQError("Sending table close request msg", errno);
    }
    //Wait for the reply (it's empty but that does not matter)
    msg = zmsg_recv(reqSocket); //Blocks until reply received
    if (unlikely(!msg)) {
        debugZMQError("Receive reply from table opener", errno);
    }
    zmsg_destroy(&msg);
}

TableOpenHelper::~TableOpenHelper() {
    zsocket_destroy(context, reqSocket);
}
