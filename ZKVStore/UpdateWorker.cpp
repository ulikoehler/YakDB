/* 
 * File:   UpdateWorker.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 10:35
 */

#include "UpdateWorker.hpp"
#include <czmq.h>
#include <iostream>
#include <cassert>
#include <leveldb/write_batch.h>
#include <functional>
#include "Tablespace.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"

using namespace std;

static void handleUpdateRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, bool synchronousWrite) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = synchronousWrite;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    uint32_t tableId = parseTableId(msg);
    //Get the table
    leveldb::DB* db = tables.getTable(tableId, helper);
    //The entire update is processed in one batch
    leveldb::WriteBatch batch;
    while (true) {
        //The next two frames contain 
        zframe_t* keyFrame = zmsg_next(msg);
        if (!keyFrame) {
            break;
        }
        zframe_t* valueFrame = zmsg_next(msg);
        assert(valueFrame); //if this fails there is an odd number of data frames --> illegal (see protocol spec)

        leveldb::Slice keySlice((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        leveldb::Slice valueSlice((char*) zframe_data(valueFrame), zframe_size(valueFrame));

#ifdef DEBUG_UPDATES
        printf("Insert '%s' = '%s'\n", keySlice.ToString().c_str(), valueSlice.ToString().c_str());
        fflush(stdout);
#endif

        batch.Put(keySlice, valueSlice);
    }
    //Commit the batch
#ifdef DEBUG_UPDATES
    printf("Commit update batch\n");
#endif
    db->Write(writeOptions, &batch);
    //The memory occupied by the message is free'd in the thread loop
}

/**
 * Handle compact requests. Note that, in contrast to Update/Delete requests,
 * performance doesn't really matter here because compacts are incredibly time-consuming.
 * In many cases they need to rewrite almost the entire databases, especially in the common
 * use caseof compacting the entire database
 * @param tables
 * @param msg
 * @param helper
 * @param headerFrame
 */
static void handleCompactRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, zframe_t* headerFrame) {
    //Parse the table ID
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (Tablespace::IndexType));
    Tablespace::IndexType tableId = *((Tablespace::IndexType*) zframe_data(tableIdFrame));
    //Get the table
    leveldb::DB* db = tables.getTable(index, helper);
    //Parse the start/end frame
    //zmsg_next returns a non-NULL frame when calling zmsg_next after the last frame has been next'ed, so we'll have to perform additional checks here
    zframe_t* rangeStartFrame = zmsg_next(msg);
    zframe_t* rangeEndFrame = (rangeStartFrame == NULL ? NULL : zmsg_next(msg));
    bool haveRangeStart = !(rangeStartFrame == NULL || zframe_size(rangeStartFrame) == 0);
    bool haveRangeEnd = !(rangeEndFrame == NULL || zframe_size(rangeEndFrame) == 0);
    leveldb::Slice* rangeStart = (haveRangeStart ? new leveldb::Slice((char*) zframe_data(rangeStartFrame), zframe_size(rangeStartFrame)) : nullptr);
    leveldb::Slice* rangeEnd = (haveRangeEnd ? new leveldb::Slice((char*) zframe_data(rangeEndFrame), zframe_size(rangeEndFrame)) : nullptr);
    db->CompactRange(rangeStart, rangeEnd);
    //Cleanup
    delete rangeStart;
    delete rangeEnd;
    //Remove and destroy the range frames, because they're not used in the response
    zmsg_remove_destroy(msg, rangeStartFrame);
    zmsg_remove_destroy(msg, rangeEndFrame);
    zframe_reset(headerFrame, "\x31\x01\x03\x00", 4);
}

static void handleTableOpenRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, zframe_t* headerFrame) {
    //Parse the table ID
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (Tablespace::IndexType));
    Tablespace::IndexType tableId = *((Tablespace::IndexType*) zframe_data(tableIdFrame));
    //Open the table
    helper.openTable(tableId);
    //Remove the table ID frame from the message
    zmsg_remove_destroy(msg, &tableIdFrame);
    //Rewrite the header frame for the response
    zframe_reset(headerFrame, "\x31\x01\x01\x00", 4);
}

static void handleTableCloseRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, zframe_t* headerFrame) {
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (Tablespace::IndexType));
    Tablespace::IndexType tableId = *((Tablespace::IndexType*) zframe_data(tableIdFrame));
    //Close the table
    tables.closeTable(tableId);
    //Remove the table ID frame from the message
    zmsg_remove_destroy(msg, &tableIdFrame);
    //Rewrite the header frame for the response
    zframe_reset(headerFrame, "\x31\x01\x02\x00", 4);
}

static void handleDeleteRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, bool synchronousWrite) {
    leveldb::Status status;
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = synchronousWrite;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(zframe_size(tableIdFrame) == sizeof (Tablespace::IndexType));
    Tablespace::IndexType tableId = *((Tablespace::IndexType*) zframe_data(tableIdFrame));
    //Get the table
    leveldb::DB* db = tables.getTable(tableId, helper);
    //The entire update is processed in one batch
    leveldb::WriteBatch batch;
    while (true) {
        //The next two frames contain 
        zframe_t* keyFrame = zmsg_next(msg);
        if (!keyFrame) { //last key frame already read --> keyFrame == nullptr
            break;
        }
        leveldb::Slice keySlice((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        batch.Delete(keySlice);
    }
    //Commit the batch
    db->Write(writeOptions, &batch);
    //The memory occupied by the message is free'd in the thread loop
}

/**
 * The main function for the update worker thread.
 * 
 * This function parses the header, calls the appropriate handler function
 * and sends the response for PARTSYNC requests
 */
static void updateWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace) {
    //Create the socket that is used to proxy requests to the external req/rep socket
    void* replyProxySocket = zsocket_new(ctx, ZMQ_PUSH);
    zsocket_connect(replyProxySocket, externalRequestProxyEndpoint);
    //Create the socket that is used by the send() member function
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
        zframe_t* headerFrame = nullptr;
        zframe_t* routingFrame = nullptr; //Set to non-null if there is an envelope
        if (zframe_size(secondFrame) == 0) { //Msg contains envelope
            headerFrame = zmsg_next(msg);
            routingFrame = firstFrame;
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
            handleUpdateRequest(tablespace, msg, tableOpenHelper, fullsync);
        } else if (requestType == DeleteRequest) {
            handleDeleteRequest(tablespace, msg, tableOpenHelper, fullsync);
        } else if (requestType == OpenTableRequest) {
            handleTableOpenRequest(tablespace, msg, tableOpenHelper)
            //Set partsync to force the program to respond after finished
            partsync = true;
        } else if (requestType == CloseTableRequest) {
            handleTableCloseRequest(tablespace, msg, tableOpenHelper, headerFrame);
            //Set partsync to force the program to respond after finished
            partsync = true;
        } else if (requestType == CompactTableRequest) {
            handleCompactRequest(tablespace, msg, tableOpenHelper, headerFrame);
            //Set partsync to force the code to respond after finishing
            partsync = true;
        } else {
            cerr << "Internal routing error: request type " << requestType << " routed to update worker thread!" << endl;
        }
        //Cleanup
        if (routingFrame) { //If there is routing info available, we can reuse the frame
            zmsg_remove(msg, routingFrame);
        }
        zmsg_destroy(&msg);
        //If partsync is disabled, the main thread already sent the response.
        //Else, we need to create & send the response now.
        //Routing 
        if (partsync) {
            //If no response is desired regardless of the message content,
            // the first byte of the header message shall be set to 0x00 (instead of the magic byte 0x31)
            byte* data = zframe_data(headerFrame);
            if (data[0] == 0x31) {
                assert(routingFrame); //If this fails, someone disobeyed the protocol specs and sent PARTSYNC over a non-REQ-REP-cominbation.
                //Send acknowledge message
                msg = zmsg_new();
                zmsg_wrap(msg, routingFrame);
                zmsg_addmem(msg, "\x31\x01\x20\x00", 4); //Send response code 0x00 (ack)
                zmsg_send(&msg, replyProxySocket);
            }
        }
    }
    printf("Stopping update processor\n");
    zsocket_destroy(ctx, workPullSocket);
    zsocket_destroy(ctx, replyProxySocket);
}

UpdateWorkerController::UpdateWorkerController(zctx_t* context, Tablespace& tablespace) : context(context), numThreads(3), tablespace(tablespace) {
    //Initialize the push socket
    workerPushSocket = zsocket_new(context, ZMQ_PUSH);
    zsocket_bind(workerPushSocket, updateWorkerThreadAddr);
}

void UpdateWorkerController::start() {
    threads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = new std::thread(updateWorkerThreadFunction, context, std::ref(tablespace));
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

void UpdateWorkerController::send(zmsg_t** msg) {
    zmsg_send(msg, workerPushSocket);
}
