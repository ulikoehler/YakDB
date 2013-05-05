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
#include <string>
#include <leveldb/write_batch.h>
#include <functional>
#include "Tablespace.hpp"
#include "Log.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"
#include "macros.hpp"

using namespace std;

static zmsg_t* handleUpdateRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, LogSource& log, bool synchronousWrite, bool noResponse) {
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = synchronousWrite;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
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
        batch.Put(keySlice, valueSlice);
    }
    //Commit the batch
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (unlikely(!status.ok())) {
        std::string err = status.ToString();
        log.error("Database error while processing update request: " + err);
        if (!noResponse) {
            zmsg_t* response = zmsg_new();
            zmsg_addmem(response, "\x31\x01\x20\x10", 4);
            zmsg_addmem(response, err.c_str(), err.size());
            return response;
        }
    }
    //The memory occupied by the message is free'd in the thread loop
    //Create the response if neccessary
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x00", 4);
    }
    return response;
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
static zmsg_t* handleCompactRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, LogSource& log, zframe_t* headerFrame, bool noResponse) {
    //Parse the table ID
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    //Get the table
    leveldb::DB* db = tables.getTable(tableId, helper);
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
    //Create the response if neccessary
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x03\x00", 4);
    }
    return response;
}

static zmsg_t* handleTableOpenRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, LogSource& log, zframe_t* headerFrame, bool noResponse) {
    //Parse the table ID
    uint32_t tableId = extractBinary<uint32_t>(zmsg_next(msg));
    //
    //Parse the flags from the header frame
    //
    uint64_t lruCache = extractBinary<uint64_t>(zmsg_next(msg));
    uint64_t tableBlocksize = extractBinary<uint64_t>(zmsg_next(msg));
    uint64_t writeCache = extractBinary<uint64_t>(zmsg_next(msg));
    //Open the table
    helper.openTable(tableId);
    //Rewrite the header frame for the response
    //Create the response if neccessary
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x01", 4);
    }
    return response;
}

static zmsg_t* handleTableCloseRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, LogSource& log, zframe_t* headerFrame, bool noResponse) {
    uint32_t tableId = extractBinary<uint32_t>(zmsg_next(msg));
    //Close the table
    tables.closeTable(tableId);
    //Create the response
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x01", 4);
    }
    return response;
}

static zmsg_t* handleDeleteRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, LogSource& log, bool synchronousWrite, bool noResponse) {
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = synchronousWrite;
    //Parse the table id
    //This function requires that the given message has the table id in its first frame
    zframe_t* tableIdFrame = zmsg_next(msg);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
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
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (unlikely(!status.ok())) {
        std::string err = status.ToString();
        log.error("Database error while processing delete request: " + err);
        if (!noResponse) {
            zmsg_t* response = zmsg_new();
            zmsg_addmem(response, "\x31\x01\x20\x10", 4);
            zmsg_addmem(response, err.c_str(), err.size());
            return response;
        }
    }
    //The memory occupied by the message is free'd in the thread loop
    //Create the response
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x00", 4);
    }
    return response;
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
    //Create a thread local log source
    LogSource logSource(ctx, "Update worker");
    //Create the data structure with all info for the poll handler
    while (true) {
        zmsg_t* msg = zmsg_recv(workPullSocket);
        if (unlikely(!msg)) {
            if (errno != ETERM && errno != EINTR) {
                debugZMQError("Receive TableOpenServer message", errno);
            }
            break;
        }
        assert(zmsg_size(msg) >= 1);
        //Parse the header
        //At this point it is unknown if
        // 1) the msg contains an envelope (--> received from the main ROUTER) or
        // 2) the msg does not contain an envelope (--> received from PULL, SUB etc.)
        //As the frame after the header may not be empty under any circumstances for
        // request types processed by the update worker threads, we can distiguish these cases
        zframe_t* firstFrame = zmsg_first(msg);
        zframe_t* secondFrame = zmsg_next(msg);
        zframe_t* headerFrame = nullptr;
        zframe_t* routingFrame = nullptr; //Set to non-null if there is an envelope
        if (zframe_size(secondFrame) == 0) { //Msg contains envelope
            headerFrame = zmsg_next(msg);
            routingFrame = firstFrame;
        } else { //No envelope
            //We need to reset the current frame ptr
            // (because the handlers may call zmsg_next()),
            // so we can't just use firstFrame as header
            headerFrame = zmsg_first(msg);
        }
        assert(isHeaderFrame(headerFrame));
        //According to the protocol specs, the magic byte shall be set to 0x00 if the message was received
        // over a socket that doesn't support responses (PULL and SUB)
        bool responseUnsupported = (zframe_data(headerFrame)[0] == 0x00);
        //Get the request type
        RequestType requestType = getRequestType(headerFrame);
        //Process the flags
        uint8_t flags = getWriteFlags(headerFrame);
        bool partsync = isPartsync(flags); //= Send reply after written to backend
        bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
        //Process the rest of the frame
        zmsg_t* response;
        if (requestType == PutRequest) {
            response = handleUpdateRequest(tablespace, msg, tableOpenHelper, logSource, fullsync, responseUnsupported);
        } else if (requestType == DeleteRequest) {
            response = handleDeleteRequest(tablespace, msg, tableOpenHelper, logSource, fullsync, responseUnsupported);
        } else if (requestType == OpenTableRequest) {
            response = handleTableOpenRequest(tablespace, msg, tableOpenHelper, logSource, headerFrame, responseUnsupported);
            //Set partsync to force the program to respond after finished
            partsync = true;
        } else if (requestType == CloseTableRequest) {
            response = handleTableCloseRequest(tablespace, msg, tableOpenHelper, logSource, headerFrame, responseUnsupported);
            //Set partsync to force the program to respond after finished
            partsync = true;
        } else if (requestType == CompactTableRequest) {
            response = handleCompactRequest(tablespace, msg, tableOpenHelper, logSource, headerFrame, responseUnsupported);
            //Set partsync to force the code to respond after finishing
            partsync = true;
        } else {
            logSource.error(std::string("Internal routing error: request type ") + std::to_string(requestType) + " routed to update worker thread!");
            continue;
        }
        //Cleanup
        if (routingFrame) { //If there is routing info available, we can reuse the frame (it's always the first, so pop'ing works)
            routingFrame = zmsg_pop(msg);
        }
        //If partsync is disabled, the main thread already sent the response.
        //Else, we need to create & send the response now.
        //If no response is desired regardless of the message content (--> PULL or SUB socket),
        // the first byte of the header message shall be set to 0x00 (instead of the magic byte 0x31)
        if (partsync && zframe_data(headerFrame)[0] == 0x31) {
            assert(routingFrame); //If this fails, someone disobeyed the protocol specs and sent PARTSYNC over a non-REQ-REP-cominbation.
            //Send acknowledge message
            zmsg_t* responseMsg = zmsg_new();
            zmsg_wrap(responseMsg, routingFrame);
            //The handler functions rewrite the header frame to contain the correct response
            zmsg_add(responseMsg, headerFrame);
            //Send the response
            zmsg_send(&responseMsg, replyProxySocket);
        }
        //Destroy the original message (after replying, reduces delay, even if only by microseconds)
        zmsg_destroy(&msg);
    }
    logSource.info("Stopping update processor");
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
    zsocket_destroy(context, tempSocket);
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
