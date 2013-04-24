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
static void handleReadRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& openHelper) {
#ifdef DEBUG_READ
    printf("Starting to handle read request of size %d\n", (uint32_t) zmsg_size(msg) - 3);
    fflush(stdout);
#endif
    //Parse the table id
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (uint32_t));
    uint32_t tableId = *((uint32_t*) zframe_data(tableIdFrame));
    //The response has doesn't have the table frame, so
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value; //Where the value will be placed
    leveldb::Status status;
    //Read each read request
    zframe_t* keyFrame = NULL;
    while ((keyFrame = zmsg_next(msg)) != NULL) {
        //Build a slice of the key (zero-copy)
        string keystr((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        leveldb::Slice key((char*) zframe_data(keyFrame), zframe_size(keyFrame));
#ifdef DEBUG_READ
        printf("Reading key %s from table %d\n", key.ToString().c_str(), tableId);
#endif
        status = db->Get(readOptions, key, &value);
        if (status.IsNotFound()) {
#ifdef DEBUG_READ
            cout << "Could not find value for key " << key.ToString() << "in table " << tableId << endl;
#endif
            //Empty value
            zframe_reset(keyFrame, "", 0);
        } else {
#ifdef DEBUG_READ
            cout << "Read " << value << " (key = " << key.ToString() << ") --> " << value << endl;
#endif
            zframe_reset(keyFrame, value.c_str(), value.length());
        }
    }
    //Now we can remove the table ID frame from the message (doing so before would confuse zmsg_next())
    zmsg_remove(msg, tableIdFrame);
    zframe_destroy(&tableIdFrame);
#ifdef DEBUG_READ
    cout << "Final reply msg size: " << zmsg_size(msg) << endl;
#endif
}

/**
 * The main function for the update worker thread.
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
        if (requestType == ReadRequest) {
            handleReadRequest(tablespace, msg, tableOpenHelper);
        } else if (requestType == CountRequest) {
            cerr << "Count request TBD - WIP!" << endl;
        } else {
            cerr << "Internal routing error: request type " << requestType << " routed to update worker thread!" << endl;
        }
        //Send reply (the handler function rewrote the original message to contain the reply)
        assert(msg);
        assert(zmsg_size(msg) >= 3); //2 Envelope + 1 Response header (corner case: Nothing to be read)
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
