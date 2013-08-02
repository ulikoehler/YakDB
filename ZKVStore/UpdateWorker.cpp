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

UpdateWorker::UpdateWorker(zctx_t* ctx, Tablespace& tablespace) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Update worker"),
tableOpenHelper(ctx),
tablespace(tablespace) {
    //Connect the socket that is used to proxy requests to the external req/rep socket
    zsocket_connect(processorOutputSocket, externalRequestProxyEndpoint);
    //Connect the socket that is used by the send() member function
    zsocket_connect(processorInputSocket, updateWorkerThreadAddr);
    logger.debug("Update worker thread starting");
}

UpdateWorker::~UpdateWorker() {
    logger.debug("Update worker thread stopping...");
    zsocket_destroy(context, processorOutputSocket);
    zsocket_destroy(context, processorInputSocket);
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
    receiveExpectMore(&haveReplyAddrFrame, processorInputSocket, logger);
    char haveReplyAddrFrameContent = ((char*) zmq_msg_data(&haveReplyAddrFrame))[0];
    zmq_msg_close(&haveReplyAddrFrame);
    if (haveReplyAddrFrameContent == '\xFF') {
        return true;
    }
    //OK, it's a processable message, not a stop message
    bool haveReplyAddr = (haveReplyAddrFrameContent == 1);
    /**
     * If there is routing info, there will be an reply.
     * We can start to write the routing info to the output socket immediately,
     * the handler function will write the remaining frames, even in case of errors.
     */
    if (haveReplyAddr) {
        //Read routing info
        zmq_msg_init(&routingFrame);
        receiveExpectMore(&routingFrame, processorInputSocket, logger);
        zmq_msg_init(&delimiterFrame);
        receiveExpectMore(&delimiterFrame, processorInputSocket, logger);
        //Write routing info
        zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
        zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
    }
    //The router ensures the header frame is correct, so a (crashing) assert works here
    zmq_msg_init(&headerFrame);
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in update worker thread", "\x31\x01\xFF", haveReplyAddr))) {
        return true;
    }
    //The header-ness of the header frame shall be checked by the main router
    if (unlikely(!isHeaderFrame(&headerFrame))) {
        logger.error("Internal malfunction: Frame of size "
                + std::to_string(zmq_msg_size(&headerFrame))
                + ", which was expected to be a header frame, is none: "
                + describeMalformedHeaderFrame(&headerFrame));
        disposeRemainingMsgParts();
        return true;
    }
    //Parse the request type
    RequestType requestType = getRequestType(&headerFrame);
    /*
     * Route the request to the appropriate function
     * 
     * All functions have the responsibility to destroy the header frame
     * if not used anymore.
     * All functions must send at least one frame (without SNDMORE) if the last
     * argument is true.
     */
    if (requestType == PutRequest) {
        handleUpdateRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == DeleteRequest) {
        logger.trace("DR");
        handleDeleteRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == OpenTableRequest) {
        handleTableOpenRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == CloseTableRequest) {
        handleTableCloseRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == CompactTableRequest) {
        handleCompactRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == TruncateTableRequest) {
        handleTableTruncateRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == DeleteRangeRequest) {
        handleDeleteRangeRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == LimitedDeleteRangeRequest) {
        handleLimitedDeleteRangeRequest(&headerFrame, haveReplyAddr);
    } else {
        logger.error(std::string("Internal routing error: request type ")
                + std::to_string(requestType) + " routed to update worker thread!");
    }
    /**
     * In some cases (especially errors) the msg part input queue is clogged
     * up with frames that have not yet been processed.
     * Clear them
     */
    disposeRemainingMsgParts();
    return true;
}

void UpdateWorker::handleUpdateRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x20\x01";
    static const char* ackResponse = "\x31\x01\x20\x00";
    assert(isHeaderFrame(headerFrame));
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse, errorResponse)) {
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //If this point is reached in the control flow, header frame will not be reused
    zmq_msg_close(headerFrame);
    //The entire update is processed in one batch. Empty batches are allowed.
    bool haveMoreData = socketHasMoreFrames(processorInputSocket);
    zmq_msg_t keyFrame, valueFrame;
    leveldb::WriteBatch batch;
    while (haveMoreData) {
        zmq_msg_init(&keyFrame);
        zmq_msg_init(&valueFrame);
        //The next two frames contain key and value
        receiveLogError(&keyFrame, processorInputSocket, logger);
        //Check if there is a key but no value
        if (!expectNextFrame("Protocol error: Found key frame, but no value frame. They must occur in pairs!", generateResponse, "\x31\x01\x20\x01")) {
            return;
        }
        receiveLogError(&valueFrame, processorInputSocket, logger);
        //Convert to LevelDB
        leveldb::Slice keySlice((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        leveldb::Slice valueSlice((char*) zmq_msg_data(&valueFrame), zmq_msg_size(&valueFrame));
        batch.Put(keySlice, valueSlice);
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&valueFrame);
        //Cleanup
        zmq_msg_close(&keyFrame);
        zmq_msg_close(&valueFrame);
    }
    //Commit the batch
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkLevelDBStatus(status,
            "Database error while processing update request: ",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleDeleteRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x21\x01";
    static const char* ackResponse = "\x31\x01\x21\x00";
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse, errorResponse)) {
        return;
    }
    bool haveMoreData = socketHasMoreFrames(processorInputSocket);
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //If this point is reached in the control flow, header frame will not be reused
    zmq_msg_close(headerFrame);
    //The entire update is processed in one batch
    zmq_msg_t keyFrame;
    leveldb::WriteBatch batch;
    while (haveMoreData) {
        zmq_msg_init(&keyFrame);
        //The next two frames contain key and value
        if (unlikely(!receiveMsgHandleError(&keyFrame,
                "Receive deletion key frame", errorResponse, generateResponse))) {
            return;
        }
        //Convert to LevelDB
        leveldb::Slice keySlice((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        batch.Delete(keySlice);
        logger.trace(keySlice.ToString());
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&keyFrame);
        //Cleanup
        zmq_msg_close(&keyFrame);
    }
    //Commit the batch
    leveldb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkLevelDBStatus(status,
            "Database error while processing delete request: ",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
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
    static const char* errorResponse = "\x31\x01\x03\x01";
    static const char* ackResponse = "\x31\x01\x03\x00";
    zmq_msg_close(headerFrame);
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse, "\x31\x01\x20\x10")) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in compact request, range missing",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr,
            rangeEndStr,
            "Compact request compact range parsing",
            errorResponse,
            generateResponse);
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Do the compaction (takes LONG)
    leveldb::Slice rangeStart(rangeStartStr);
    leveldb::Slice rangeEnd(rangeEndStr);
    db->CompactRange((haveRangeStart ? &rangeStart : nullptr),
            (haveRangeEnd ? &rangeEnd : nullptr));
    //Create the response if neccessary
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleDeleteRangeRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x22\x01";
    static const char* ackResponse = "\x31\x01\x22\x00";
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    zmq_msg_close(headerFrame);
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse, errorResponse)) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in compact request, range missing",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr,
            rangeEndStr,
            "Compact request compact range parsing",
            errorResponse,
            generateResponse);
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Convert the str to a slice, to compare the iterator slice in-place
    leveldb::Slice rangeEndSlice(rangeEndStr);
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    //All deletes are applied in one batch
    // This also avoids construct like deleting while iterating
    leveldb::WriteBatch batch;
    if (haveRangeStart) {
        it->Seek(rangeStartStr);
    } else {
        it->SeekToFirst();
    }
    uint64_t count = 0;
    //Iterate over all key-values in the range
    for (; it->Valid(); it->Next()) {
        count++;
        leveldb::Slice key = it->key();
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        batch.Delete(key);
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(), "LevelDB error while processing delete request", true, errorResponse)) {
        delete it;
        return;
    }
    delete it;
    //Apply the batch
    status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkLevelDBStatus(status,
            "Database error while processing delete request: ",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Create the response if neccessary
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleLimitedDeleteRangeRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x23\x01";
    static const char* ackResponse = "\x31\x01\x23\x00";
    //Process the flags
    uint8_t flags = getWriteFlags(headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to LevelDB
    leveldb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    zmq_msg_close(headerFrame);
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse, errorResponse)) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in compact request, range missing",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Get the table
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //
    //Parse the from range and the limit
    //
    zmq_msg_t rangeStartMsg;
    zmq_msg_init(&rangeStartMsg);
    if (!receiveMsgHandleError(&rangeStartMsg, "Receive limited scan range start frame", errorResponse, true)) {
        return;
    }
    if (!expectNextFrame("Only range start frame found in limited scan request, limit frame missing", true, errorResponse)) {
        return;
    }
    uint64_t scanLimit;
    if (!parseUint64Frame(scanLimit, "Receive limited scan range start frame", true, errorResponse)) {
        return;
    }
    bool haveRangeStart = (zmq_msg_size(&rangeStartMsg) != 0);
    //Convert the range start frame to a slice
    leveldb::Slice rangeStartSlice((char*) zmq_msg_data(&rangeStartMsg), zmq_msg_size(&rangeStartMsg));
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    //All deletes are applied in one batch
    // This also avoids construct like deleting while iterating
    leveldb::WriteBatch batch;
    if (haveRangeStart) {
        it->Seek(rangeStartSlice);
    } else {
        it->SeekToFirst();
    }
    //Iterate over all key-values in the range
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        //Check if we have to stop here
        if (scanLimit <= 0) {
            break;
        }
        scanLimit--;
        batch.Delete(key);
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(),
            "LevelDB error while processing limited range delete request",
            true, errorResponse)) {
        delete it;
        return;
    }
    delete it;
    //Apply the batch
    status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkLevelDBStatus(status,
            "Database error while processing delete request: ",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Create the response if neccessary
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleTableOpenRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x01\x01";
    static const char* ackResponse = "\x31\x01\x01\x00";
    /*
     * This method performs frame correctness check, the table open server
     * serializes everything in a single struct.
     */
    //Extract flags from header
    uint8_t flags = ((uint8_t*) zmq_msg_data(headerFrame))[3];
    bool compressionEnabled = (flags & 0x01) == 0;
    //Cleanup header frame
    zmq_msg_close(headerFrame);
    //Extract numeric parameters
    uint32_t tableId;
    if (!parseUint32Frame(tableId,
            "Table ID frame",
            generateResponse,
            errorResponse)) {
        return;
    }
    uint64_t lruCacheSize;
    if (!parseUint64FrameOrAssumeDefault(lruCacheSize,
            UINT64_MAX,
            "LRU cache size frame",
            generateResponse,
            errorResponse)) {
        return;
    }
    uint64_t blockSize;
    if (!parseUint64FrameOrAssumeDefault(blockSize,
            UINT64_MAX,
            "Table block size frame",
            generateResponse,
            errorResponse)) {
        return;
    }
    uint64_t writeBufferSize;
    if (!parseUint64FrameOrAssumeDefault(writeBufferSize,
            UINT64_MAX,
            "Write buffer size frame",
            generateResponse,
            errorResponse)) {
        return;
    }
    uint64_t bloomFilterBitsPerKey;
    if (!parseUint64FrameOrAssumeDefault(bloomFilterBitsPerKey,
            UINT64_MAX,
            "Bits per key bloom filter size frame",
            generateResponse,
            errorResponse)) {
        return;
    }
    //
    //Parse the flags from the header frame
    //
    //Open the table
    tableOpenHelper.openTable(tableId,
            lruCacheSize,
            blockSize,
            writeBufferSize,
            bloomFilterBitsPerKey,
            compressionEnabled);
    //Rewrite the header frame for the response
    //Create the response if neccessary
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleTableCloseRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x02\x01";
    static const char* ackResponse = "\x31\x01\x02\x00";
    zmq_msg_close(headerFrame);
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse,
            errorResponse)) {
        return;
    }
    //Close the table
    tableOpenHelper.closeTable(tableId);
    //Create the response
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

void UpdateWorker::handleTableTruncateRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x03\x01";
    static const char* ackResponse = "\x31\x01\x03s\x00";
    zmq_msg_close(headerFrame);
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse,
            errorResponse)) {
        return;
    }
    //Close the table
    tableOpenHelper.truncateTable(tableId);
    //Create the response
    zmsg_t* response = nullptr;
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger);
    }
}

/**
 * Pretty stubby update thread loop.
 * This is what should contain the scheduler client code in the future.
 */
static void updateWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace) {
    UpdateWorker updateWorker(ctx, tablespace);
    while (true) {
        if (!updateWorker.processNextMessage()) {
            break;
        }
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
