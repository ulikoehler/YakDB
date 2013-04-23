/* 
 * File:   UpdateWorker.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 10:35
 */

#include "UpdateWorker.hpp"
#include <czmq.h>
#include "zutil.hpp"

const char* updateWorkerThreadAddr = "inproc://updateWorkerThreads";

/**
 * The main function for the update worker thread.
 * 
 * This function parses the header, calls the appropriate handler function
 * and sends the response for PARTSYNC requests
 */
static void updateWorkerThreadFunction(zctx_t* ctx, KVServer* serverInfo) {
    void* workPullSocket = zsocket_new(ctx, ZMQ_PULL);
    zsocket_connect(workPullSocket, updateWorkerThreadAddr);
    //Create the table open helper (creates a socket that sends table open requests)
    TableOpenHelper tableOpenHelper(ctx);
    //Create the data structure with all info for the poll handler
    while (true) {
        zmsg_t* msg = zmsg_recv(workPullSocket);
        assert(zmsg_size(msg) >= 1);
        //Parse the header
        //At this point it is unknown if
        // 1) the msg contains an envelope (--> received from the main ROUTER) or
        // 2) the msg does not contain an envelope (--> received from PULL, SUB etc.
        //As the frame after the header may not be empty under any circumstances for
        // request types processed by the update worker threads, we can distiguish these cases
        zframe_t* firstFrame = zmsg_first(msg);
        zframe_t* secondFrame = zmsg_next(msg);
        zframe_t* headerFrame;
        zframe_t* routingFrame = NULL; //Set to non-null if there is an envelope
        if (zframe_size(secondFrame) == 0) { //Msg contains envelope
            headerFrame = zmsg_next(msg);
            routingFrame = zmsg_unwrap(msg);
        } else {
            //We need to reset the current frame ptr
            // (because the handlers may call zmsg_next()),
            // so we can't just use firstFrame as header
            headerFrame = zmsg_first(msg);
        }
        assert(isHeaderFrame(headerFrame));
        //Get the request type
        RequestType requestType = getRequestType(headerFrame);
        //Process the flags
        uint8_t flags = getWriteFlags(headerFrame);
        bool partsync = isPartsync(flags); //= Send reply after written to backend
        bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
        //Process the rest of the frame
        if (requestType == PutRequest) {
            handleUpdateRequest(serverInfo->tables, msg, tableOpenHelper, fullsync);
        } else if (requestType == DeleteRequest) {
            cerr << "Delete request TBD - WIP!" << endl;
        } else {
            cerr << "Internal routing error: request type " << requestType << " routed to update worker thread!" << endl;
        }
        //Cleanup
        zmsg_destroy(&msg);
        //If partsync is disabled, the main thread already sent the response.
        //Else, we need to create & send the response now.
        //Routing 
        if (partsync) {
            assert(routingFrame); //If this fails, someone disobeyed the protocol specs and sent PARTSYNC over a non-REQ-REP-cominbation.
            //Send acknowledge message
            msg = zmsg_new();
            zmsg_wrap(msg, routingFrame);
            zmsg_addmem(msg, "\x31\x01\x20\x00", 4); //Send response code 0x00 (ack)
            zmsg_send(&msg, serverInfo->externalRepSocket);
        }
    }
    printf("Stopping update processor\n");
    zsocket_destroy(ctx, workPullSocket);
}

UpdateWorkerController::UpdateWorkerController(zctx_t* context, KVServer* serverInfo) : context(context) {
    //Initialize the push socket
    workerPushSocket = zsocket_new(context, ZMQ_PUSH);
    zsocket_bind(workerPushSocket, updateWorkerThreadAddr);
    //Start the threads
    numThreads = 3; //Default
    threads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = new std::thread(updateWorkerThreadFunction, context, serverInfo);
    }
}

UpdateWorkerController::~UpdateWorkerController() {

    //Send an empty STOP message for each update thread (use a temporary socket)
    void* tempSocket = zsocket_new(context, ZMQ_PUSH); //Create a temporary socket
    zsocket_connect(tempSocket, updateWorkerThreadAddr);
    for (int i = 0; i < numThreads; i++) {
        //Send an empty msg (signals the table open thread to stop)
        sendEmptyFrameMessage(tempSocket);
    }
    //Cleanup
    zsocket_destroy(tempSocket, context);
    //Wait for each thread to exit
    for (int i = 0; i < numThreads; i++) {
        threads[i]->join();
        delete threads[i];
    }
    //Free the array
    if (numThreads > 0) {
        delete[] numThreads;
    }
}


