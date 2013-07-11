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
#include <bitset>
#include "Tablespace.hpp"
#include "Logger.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"
#include "macros.hpp"

using namespace std;

static zmsg_t* handleTableOpenRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, Logger& log, zframe_t* headerFrame, bool noResponse) {
    //Parse the table ID and advanced parameter frames and check their length
    //TODO convert asserts to protocol errors
    zframe_t* tableIdFrame = zmsg_next(msg);
    assert(tableIdFrame);
    assert(zframe_size(tableIdFrame) == 4);
    zframe_t* lruCacheSizeFrame = zmsg_next(msg);
    assert(lruCacheSizeFrame);
    assert(zframe_size(lruCacheSizeFrame) == 0 || zframe_size(lruCacheSizeFrame) == 8);
    zframe_t* blockSizeFrame = zmsg_next(msg);
    assert(blockSizeFrame);
    assert(zframe_size(blockSizeFrame) == 0 || zframe_size(blockSizeFrame) == 8);
    zframe_t* writeBufferSizeFrame = zmsg_next(msg);
    assert(writeBufferSizeFrame);
    assert(zframe_size(writeBufferSizeFrame) == 0 || zframe_size(writeBufferSizeFrame) == 8);
    zframe_t* bloomFilterBitsPerKeyFrame = zmsg_next(msg);
    assert(bloomFilterBitsPerKeyFrame);
    assert(zframe_size(bloomFilterBitsPerKeyFrame) == 0 || zframe_size(bloomFilterBitsPerKeyFrame) == 8);
    //In order to reuse the frames, the must not be deallocated
    //when the msg parameter is deallocated, so we need to remove it
    zmsg_remove(msg, tableIdFrame);
    zmsg_remove(msg, lruCacheSizeFrame);
    zmsg_remove(msg, blockSizeFrame);
    zmsg_remove(msg, writeBufferSizeFrame);
    zmsg_remove(msg, bloomFilterBitsPerKeyFrame);
    //
    //Parse the flags from the header frame
    //
    //Open the table
    helper.openTable(tableIdFrame, lruCacheSizeFrame, blockSizeFrame, writeBufferSizeFrame, bloomFilterBitsPerKeyFrame);
    //Rewrite the header frame for the response
    //Create the response if neccessary
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x01", 4);
    }
    return response;
}

static zmsg_t* handleTableCloseRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, Logger& log, zframe_t* headerFrame, bool noResponse) {
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

static zmsg_t* handleTableTruncateRequest(Tablespace& tables, zmsg_t* msg, TableOpenHelper& helper, Logger& log, zframe_t* headerFrame, bool noResponse) {
    uint32_t tableId = extractBinary<uint32_t>(zmsg_next(msg));
    //Close the table
    helper.truncateTable(tableId);
    //Create the response
    zmsg_t* response = nullptr;
    if (!noResponse) {
        response = zmsg_new();
        zmsg_addmem(response, "\x31\x01\x20\x01", 4);
    }
    return response;
}

UpdateWorker::UpdateWorker(zctx_t* ctx, Tablespace& tablespace) :
context(ctx),
replyProxySocket(zsocket_new(ctx, ZMQ_PUSH)),
workPullSocket(zsocket_new(ctx, ZMQ_PULL)),
tableOpenHelper(ctx),
tablespace(tablespace),
logger(ctx, "Update worker") {
    //Connect the socket that is used to proxy requests to the external req/rep socket
    zsocket_connect(replyProxySocket, externalRequestProxyEndpoint);
    //Connect the socket that is used by the send() member function
    zsocket_connect(workPullSocket, updateWorkerThreadAddr);
    logger.debug("Update worker thread starting");
}

UpdateWorker::~UpdateWorker() {
    zsocket_destroy(context, replyProxySocket);
    zsocket_destroy(context, workPullSocket);
}

bool UpdateWorker::processNextMessage() {
    /**
     * Parse the header
     * At this point it is unknown if
     *  1) the msg contains an envelope (--> received from the main ROUTER) or
     *  2) the msg does not contain an envelope (--> received from PULL, SUB etc.)
     * Case 2) also handles cases where the main router did not request a reply.
     */
    zmq_msg_t haveReplyAddrFrame, routingFrame, delimiterFrame, headerFrame;
    zmq_msg_init(&haveReplyAddrFrame);
    receiveExpectMore(&haveReplyAddrFrame, workPullSocket, logger);
    char haveReplyAddrFrameContent = ((char*) zmq_msg_data(haveReplyAddrFrame))[0];
    zmq_msg_close(haveReplyAddrFrame);
    if (haveReplyAddrFrameContent == '\xFF') {
        break;
    }
    //OK, it's a processable message, not a stop message
    bool haveReplyAddr = (haveReplyAddrFrameContent == 0);
    /**
     * If there is routing info, there will be an reply.
     * We can start to write the routing info to the output socket immediately,
     * the handler function will write the remaining frames, even in case of errors.
     */
    if (haveReplyAddr) {
        //Read routing info
        zmq_msg_init(&routingFrame);
        receiveLogError(&routingFrame, workPullSocket, logger);
        zmq_msg_init(&delimiterFrame);
        receiveLogError(&delimiterFrame, workPullSocket, logger);
        //Write routing info
        zmq_msg_send(&routingFrame, replyProxySocket, ZMQ_SNDMORE);
        zmq_msg_send(&routingFrame, replyProxySocket, ZMQ_SNDMORE);
    }
    //The router ensures the header frame is correct, so a (crashing) assert works here
    zmq_msg_init(&headerFrame);
    receiveLogError(&headerFrame, workPullSocket, logger);
    assert(isHeaderFrame(&headerFrame));
    //Parse the request type
    RequestType requestType = getRequestType(headerFrame);
    /*
     * Route the request to the appropriate function
     * 
     * All functions have the responsibility to destroy the header frame
     * if not used anymore.
     * All functions must send at least one frame (without SNDMORE) if the last
     * argument is true.
     */
    if (requestType == PutRequest) {
        handleUpdateRequest(headerFrame, haveReplyAddr);
    } else if (requestType == DeleteRequest) {
        response = handleDeleteRequest(headerFrame, haveReplyAddr);
    } else if (requestType == OpenTableRequest) {
        response = handleTableOpenRequest(tablespace, msg, tableOpenHelper, logger, headerFrame, haveReplyAddr);
        //Set partsync to force the program to respond after finished
        partsync = true;
    } else if (requestType == CloseTableRequest) {
        response = handleTableCloseRequest(tablespace, msg, tableOpenHelper, logger, headerFrame, haveReplyAddr);
        //Set partsync to force the program to respond after finished
        partsync = true;
    } else if (requestType == CompactTableRequest) {
        response = handleCompactRequest(tablespace, msg, tableOpenHelper, logger, headerFrame, haveReplyAddr);
        //Set partsync to force the code to respond after finishing
        partsync = true;
    } else if (requestType == TruncateTableRequest) {
        response = handleTableTruncateRequest(tablespace, msg, tableOpenHelper, logger, headerFrame, haveReplyAddr);
        //Set partsync to force the code to respond after finishing
        partsync = true;
    } else {
        logger.error(std::string("Internal routing error: request type ") + std::to_string(requestType) + " routed to update worker thread!");
        continue;
    }
}

bool UpdateWorker::parseTableId(uint32_t& tableIdDst, bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(workPullSocket))) {
        logger.warn("Trying to read table ID frame, but no frame was available");
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, replyProxySocket, logger, ZMQ_SNDMORE);
            std::string errmsg = "Protocol error: Did not find table ID frame!";
            sendFrame(errmsg, replyProxySocket, logger);
        }
        return false;
    }
    //Parse table ID, release frame immediately
    zmq_msg_t tableIdFrame;
    zmq_msg_init(&tableIdFrame);
    receiveLogError(&tableIdFrame, workPullSocket, logger);
    tableIdDst = extractBinary<uint32_t>(tableIdFrame);
    zmq_msg_close(&tableIdFrame);
    return true;
}

bool UpdateWorker::expectNextFrame(const char* errString, bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!socketHasMoreFrames(workPullSocket))) {
        logger.warn(errString);
        if (generateResponse) {
            sendFrame(errorResponseCode, 4, replyProxySocket, logger, ZMQ_SNDMORE);
            sendFrame(errString, strlen(errString), replyProxySocket, logger);
        }
        return false;
    }
}

bool UpdateWorker::checkLevelDBStatus(const leveldb::Status& status, const char* errString, bool generateResponse, const char* errorResponseCode) {
    if (unlikely(!status.ok())) {
        std::string statusErr = status.ToString();
        std::string completeErrorString = std::string(errString) + statusErr;
        logger.error(completeErrorString);
        if (generateResponse) {
            //Send DB error code
            sendFrame(errorResponseCode, 4, replyProxySocket, logger, ZMQ_SNDMORE);
            sendFrame(completeErrorString, replyProxySocket, logger);
            return;
        }
    }
}

void UpdateWorker::handleUpdateRequest(zmq_msg_t* headerFrame, bool fullsync, bool generateResponse) {
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseTableId(tableId, generateResponse, "\x31\x01\x20\x10")) {
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //If this point is reached in the control flow, header frame will not be reused
    zmq_msg_close(headerFrame);
    //The entire update is processed in one batch. Empty batches are allowed.
    bool haveMoreData = socketHasMoreFrames(workPullSocket);
    zmq_msg_t keyFrame, valueFrame;
    zmq_msg_init(keyFrame);
    zmq_msg_init(valueFrame);
    leveldb::WriteBatch batch;
    while (haveMoreData) {
        //The next two frames contain key and value
        receiveLogError(&keyFrame, workPullSocket, logger);
        //Check if there is a key but no value
        if (!expectNextFrame("Protocol error: Found key frame, but no value frame. They must occur in pairs!", generateResponse, "\x31\x01\x20\x01")) {
            return;
        }
        receiveLogError(&valueFrame, workPullSocket, logger);
        //Convert to LevelDB
        leveldb::Slice keySlice((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        leveldb::Slice valueSlice((char*) zframe_data(valueFrame), zframe_size(valueFrame));
        batch.Put(keySlice, valueSlice);
        //Cleanup
        zmq_msg_close(keyFrame);
        zmq_msg_close(valueFrame);
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&valueFrame);
    }
    //Commit the batch
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if(!checkLevelDBStatus(status,
            "Database error while processing update request: ",
            generateResponse,
            "\x31\x01\x20\x02")) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendConstFrame("\x31\x01\x20\x00", 4, replyProxySocket, logger);
    }
}

void UpdateWorker::handleDeleteRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID, release frame immediately
    zmq_msg_t tableIdFrame;
    zmq_msg_init(&tableIdFrame);
    receiveLogError(&tableIdFrame, workPullSocket, logger);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    bool haveMoreData = zmq_msg_more(tableIdFrame);
    zmq_msg_close(&tableIdFrame);
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //If this point is reached in the control flow, header frame will not be reused
    zmq_msg_close(headerFrame);
    //The entire update is processed in one batch
    zmq_msg_t keyFrame;
    zmq_msg_init(keyFrame);
    leveldb::WriteBatch batch;
    while (haveMoreData) {
        //The next two frames contain key and value
        receiveLogError(&keyFrame, workPullSocket, logger);
        //Convert to LevelDB
        leveldb::Slice keySlice((char*) zframe_data(keyFrame), zframe_size(keyFrame));
        batch.Delete(keySlice);
        //Cleanup
        zmq_msg_close(keyFrame);
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&keyFrame);
    }
    //Commit the batch
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if(!checkLevelDBStatus(status,
            "Database error while processing delete request: ",
            generateResponse,
            "\x31\x01\x20\x02")) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendConstFrame("\x31\x01\x20\x00", 4, replyProxySocket, logger);
    }
}

/**
 * Handle compact requests. Note that, in contrast to Update/Delete requests,
 * performance doesn't really matter here because compacts are incredibly time-consuming.
 * In many cases they need to rewrite almost the entire databases, especially in the common
 * usecas eof compacting the entire database.
 * 
 * It also shouldn't matter that the compact request blocks the thread (at least not for now).
 * Compact request shouldn't happen too often, in the worst case a lot of work
 * for the current thread piles up.
 * @param tables
 * @param msg
 * @param helper
 * @param headerFrame
 */
void UpdateWorker::handleCompactRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    //Parse table ID, release frame immediately
    zmq_msg_t tableIdFrame;
    zmq_msg_init(&tableIdFrame);
    receiveLogError(&tableIdFrame, workPullSocket, logger);
    uint32_t tableId = extractBinary<uint32_t>(tableIdFrame);
    bool haveMoreFrames = zmq_msg_more(tableIdFrame);
    zmq_msg_close(&tableIdFrame);
    if (!haveMoreFrames) {
        logger.warn("Compact request has only one frame");
        if (generateResponse) {
            //Send DB error code
            sendConstFrame("\x31\x01\x03\x01", 4, replyProxySocket, logger, ZMQ_SNDMORE);
            sendFrame("Only header found in compact request, range missing", replyProxySocket, logger, 0);
            return;
        }
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
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

/**
 * Pretty stubby update thread loop.
 * This is what should contain the scheduler client code in the future.
 */
static void updateWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace) {
    UpdateWorker updateWorker(ctx, tablespace);
    while (true) {
        updateWorker.processNextMessage();
    }
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
