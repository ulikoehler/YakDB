/* 
 * File:   UpdateWorker.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 10:35
 */

#include "ReadWorker.hpp"
#include <czmq.h>
#include <string>
#include <iostream>
#include "TableOpenHelper.hpp"
#include "Tablespace.hpp"
#include "protocol.hpp"
#include "zutil.hpp"
#include "endpoints.hpp"

using namespace std;
#define DEBUG_READ

/**
 * Read request handler. Shall be called
 * 
 * The original message is modified to contain the response.
 * 
 * Envelope + Read request Header + Table ID[what zmsg_next() must return] + Payload
 */
static zmsg_t* handleCountRequest(Tablespace& tables, zmsg_t* msg, zframe_t* headerFrame, TableOpenHelper& openHelper) {
    static const char* ackReply = "\x31\x01\x11\x00"; //--> No error
    static const char* errorReply = "\x31\x01\x11\x10"; //--> No error
    //Parse the table id
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    //Parse the start/end frame
    //zmsg_next returns a non-NULL frame when calling zmsg_next after the last frame has been next'ed, so we'll have to perform additional checks here
    zframe_t* rangeStartFrame = zmsg_next(msg);
    zframe_t* rangeEndFrame = (rangeStartFrame == NULL ? NULL : zmsg_next(msg));
    bool haveRangeStart = !(rangeStartFrame == NULL || zframe_size(rangeStartFrame) == 0);
    bool haveRangeEnd = !(rangeEndFrame == NULL || zframe_size(rangeEndFrame) == 0);
    std::string rangeStart = (haveRangeStart ? string((char*) zframe_data(rangeStartFrame), zframe_size(rangeStartFrame)) : "");
    std::string rangeEnd = (haveRangeEnd ? string((char*) zframe_data(rangeEndFrame), zframe_size(rangeEndFrame)) : "");
    //We don't need the range frames any more
    if (rangeStartFrame) {
        zmsg_remove_destroy(msg, &rangeStartFrame);
    }
    if (rangeEndFrame) {
        zmsg_remove_destroy(msg, &rangeEndFrame);
    }
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value; //Where the value will be placed
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    if (haveRangeStart) {
        it->Seek(rangeStart);
    } else {
        it->SeekToFirst();
    }
    uint64_t count = 0;
    //Iterate over all key-values in the range
    for (; it->Valid(); it->Next()) {
        count++;
        string key = it->key().ToString();
        if (haveRangeEnd && key >= rangeEnd) {
            break;
        }
    }
    if (!it->status().ok()) { // Check for any errors found during the scan
        zframe_reset(headerFrame, errorReply, 4);
        //Reuse the table id frame to store the error message
        string status = it->status().ToString();
        zframe_reset(tableIdFrame, status.c_str(), status.length());
    } else { //Nothing happened wrong
        //Set th reply code to success if nothing happened wrong
        zframe_reset(headerFrame, ackReply, 4);
        //Reuse the table id frame to store the count
        zframe_reset(tableIdFrame, &count, sizeof (uint64_t));
    }
    //Cleanup
    delete it;
}

/**
 * Read request handler. Shall be called for Read requests only!
 * 
 * The original message is modified to contain the response.
 * 
 * Envelope + Read request Header + Table ID[what zmsg_next() must return] + Payload
 */
static void handleReadRequest(Tablespace& tables, zmsg_t* msg, zframe_t* headerFrame, TableOpenHelper& openHelper) {
    //Parse the table id
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    //Parse the 
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    string value;
    //Read each read request
    zframe_t* keyFrame = NULL;
    while ((keyFrame = zmsg_next(msg)) != NULL) {
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        status = db->Get(readOptions, key, &value);
        if (status.IsNotFound()) {
            //Empty value
            zframe_reset(keyFrame, "", 0);
        } else {
            zframe_reset(keyFrame, value.c_str(), value.length());
        }
    }
    //Rewrite the header frame (--> ack message)
    zframe_reset(headerFrame, "\x31\x01\x10\x00", 4);
    //Now we can remove the table ID frame from the message (doing so before would confuse zmsg_next())
    zmsg_remove(msg, tableIdFrame);
    zframe_destroy(&tableIdFrame);
}

/**
 * Read request handler. Shall be called for Read requests only!
 * 
 * The original message is modified to contain the response.
 * 
 * Envelope + Read request Header + Table ID[what zmsg_next() must return] + Payload
 */
static void handleExistsRequest(Tablespace& tables, zmsg_t* msg, zframe_t* headerFrame, TableOpenHelper& openHelper) {
    //Parse the table id
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    //Parse the 
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    string value;
    //Read each read request
    zframe_t* keyFrame = NULL;
    while ((keyFrame = zmsg_next(msg)) != NULL) {
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        status = db->Get(readOptions, key, &value);
        if (status.IsNotFound()) {
            //Empty value
            zframe_reset(keyFrame, "\x00", 1);
        } else {
            zframe_reset(keyFrame, "\x01", 1);
        }
    }
    //Rewrite the header frame (--> ack message)
    zframe_reset(headerFrame, "\x31\x01\x10\x00", 4);
    //Now we can remove the table ID frame from the message (doing so before would confuse zmsg_next())
    zmsg_remove(msg, tableIdFrame);
    zframe_destroy(&tableIdFrame);
}

/**
 * The main function for the read worker thread.
 * 
 * This function parses the header, calls the appropriate handler function
 * and sends the response for PARTSYNC requests
 */
static void readWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace) {
    //Create the socket that is used to proxy requests to the external req/rep socket
    void* replyProxySocket = zsocket_new(ctx, ZMQ_PUSH);
    if (zsocket_connect(replyProxySocket, externalRequestProxyEndpoint)) {
        debugZMQError("Connect reply proxy socket", errno);
    }
    assert(replyProxySocket);
    //Create the socket we receive requests from
    void* workPullSocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_connect(workPullSocket, readWorkerThreadAddr);
    assert(workPullSocket);
    //Create the table open helper (creates a socket that sends table open requests)
    TableOpenHelper tableOpenHelper(ctx);
    //Create the data structure with all info for the poll handler
    while (true) {
        zmsg_t* msg = zmsg_recv(workPullSocket);
        assert(msg);
        assert(zmsg_size(msg) >= 1);
        //Parse the header
        //Contrary to the update message handling, we assume (checked by the request router) that
        // the message contains an envelope. Read requests without envelope (--> without return path)
        // don't really make sense.
        zframe_t* routingFrame = zmsg_first(msg); //Envelope ID
        assert(routingFrame);
        zframe_t* delimiterFrame = zmsg_next(msg); //Envelope delimiter
        assert(delimiterFrame);
        zframe_t* headerFrame = zmsg_next(msg);
        assert(headerFrame);
        assert(isHeaderFrame(headerFrame));
        //Get the request type
        RequestType requestType = getRequestType(headerFrame);
        //Process the rest of the frame
        cout << "Isize " << zmsg_size(msg) << endl;
        if (requestType == ReadRequest) {
            handleReadRequest(tablespace, msg, headerFrame, tableOpenHelper);
        } else if (requestType == CountRequest) {
            handleCountRequest(tablespace, msg, headerFrame, tableOpenHelper);
        } else {
            cerr << "Internal routing error: request type " << requestType << " routed to update worker thread!" << endl;
        }
        //Send reply (the handler function rewrote the original message to contain the reply)
        assert(msg);
        assert(zmsg_size(msg) >= 3); //2 Envelope + 1 Response header (corner case: Nothing to be read)
        cout << "Sentit" << zmsg_size(msg) << endl;
        zmsg_send(&msg, replyProxySocket);
    }
    printf("Stopping update processor\n");
    zsocket_destroy(ctx, workPullSocket);
    zsocket_destroy(ctx, replyProxySocket);
}

ReadWorkerController::ReadWorkerController(zctx_t* context, Tablespace& tablespace) : context(context), tablespace(tablespace), numThreads(3) {
    //Initialize the push socket
    workerPushSocket = zsocket_new(context, ZMQ_PUSH);
    zsocket_bind(workerPushSocket, readWorkerThreadAddr);

}

void ReadWorkerController::start() {
    threads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = new std::thread(readWorkerThreadFunction, context, std::ref(tablespace));
    }
}

ReadWorkerController::~ReadWorkerController() {

    //Send an empty STOP message for each read worker thread (use a temporary socket)
    void* tempSocket = zsocket_new(context, ZMQ_PUSH); //Create a temporary socket
    zsocket_connect(tempSocket, readWorkerThreadAddr);
    for (int i = 0; i < numThreads; i++) {
        //Send an empty msg (signals the table open thread to stop)
        sendEmptyFrameMessage(tempSocket);
    }
    //Cleanup
    zsocket_destroy(context, &tempSocket);
    //Wait for each thread to exit
    for (int i = 0; i < numThreads; i++) {
        threads[i]->join();
        delete threads[i];
    }
    //Free the array
    if (numThreads > 0) {
        delete[] threads;
    }
}

void ReadWorkerController::send(zmsg_t** msg) {
    zmsg_send(msg, workerPushSocket);
}
