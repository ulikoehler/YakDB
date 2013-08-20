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

UpdateWorker::UpdateWorker(zctx_t* ctx, Tablespace& tablespace, ConfigParser& configParser) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Update worker"),
tableOpenHelper(ctx),
tablespace(tablespace) {
    //Set HWM
    zsocket_set_hwm(processorInputSocket, configParser.getInternalHWM());
    //Connect the socket that is used to proxy requests to the external req/rep socket
    if(zsocket_connect(processorOutputSocket, externalRequestProxyEndpoint) == -1) {
        logOperationError("Connect Update worker processor output socket", logger);
    }
    //Connect the socket that is used by the send() member functions
    if(zsocket_connect(processorInputSocket, updateWorkerThreadAddr)) {
        logOperationError("Connect Update worker processor input socket", logger);
    }
    logger.debug("Update worker thread starting");
}

UpdateWorker::~UpdateWorker() {
    logger.debug("Update worker thread terminating...");
    //Sockets are cleaned up in AbstractFrameProcessor
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
    if(!receiveMsgHandleError(&haveReplyAddrFrame, "Have reply addr frame", nullptr, false)) {
        return true;
    }
    //Empty frame means: Stop immediately
    if(unlikely(zmq_msg_size(&haveReplyAddrFrame) == 0)) {
        zmq_msg_close(&haveReplyAddrFrame);
        logger.trace("Update worker thread terminating");
        return false;
    }
    char haveReplyAddrFrameContent = ((char*) zmq_msg_data(&haveReplyAddrFrame))[0];
    zmq_msg_close(&haveReplyAddrFrame);
    //If it's not a stop msg frame, we expect a header frame
    if(!expectNextFrame("Expecting frame after reply addr frame", false, nullptr)) {
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
        if(receiveExpectMore(&routingFrame, processorInputSocket, logger, "Routing frame") == -1) {
            return true;
        }
        zmq_msg_init(&delimiterFrame);
        if(receiveExpectMore(&delimiterFrame, processorInputSocket, logger, "Delimiter frame") == -1) {
            return true;
        }
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
    if (likely(requestType == PutRequest)) {
        handlePutRequest(&headerFrame, haveReplyAddr);
    } else if (requestType == DeleteRequest) {
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

void UpdateWorker::handlePutRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    /**
     * IMPORTANT: We stopped using batches because they are *incredibly*
     * inefficient! They just append to a std::string on each operation.
     * This is so inefficient the worker thread sometimes needs
     * several *seconds* for a single 10k batch!
     */
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
    uint64_t elementCounter = 0;
    while (haveMoreData) {
        zmq_msg_init(&keyFrame);
        zmq_msg_init(&valueFrame);
        //The next two frames contain key and value
        if (unlikely(!receiveMsgHandleError(&keyFrame,
                "Receive put key frame", errorResponse, generateResponse))) {
            break;
        }
        //Check if there is a key but no value
        if (!expectNextFrame("Protocol error: Found key frame, but no value frame. They must occur in pairs!", generateResponse, "\x31\x01\x20\x01")) {
            return;
        }
        if (unlikely(!receiveMsgHandleError(&valueFrame,
                "Receive put value frame", errorResponse, generateResponse))) {
            break;
        }
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&valueFrame);
        //Ignore frame pair if both are empty
        size_t keySize = zmq_msg_size(&keyFrame);
        size_t valueSize = zmq_msg_size(&valueFrame);
        if(keySize == 0 && valueSize == 0) {
            continue;
        }
        //Convert to LevelDB
        leveldb::Slice keySlice((char*) zmq_msg_data(&keyFrame), keySize);
        leveldb::Slice valueSlice((char*) zmq_msg_data(&valueFrame), valueSize);
        leveldb::Status status = db->Put(writeOptions, keySlice, valueSlice);
        if (!checkLevelDBStatus(status,
                "Database error while processing update request: ",
                generateResponse,
                errorResponse)) {
            break;
        }
        //batch.Put(keySlice, valueSlice);
        //Statistics
        elementCounter++;
        //Cleanup
        zmq_msg_close(&keyFrame);
        zmq_msg_close(&valueFrame);
    }
    //If something went wrong, send an error response
    //Send success code
    if (generateResponse) {
        //Send success code
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
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
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
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
    //Do the compaction (takes LONG, so log it before)
    logger.debug("Compacting table " + std::to_string(tableId));
    leveldb::Slice rangeStart(rangeStartStr);
    leveldb::Slice rangeEnd(rangeEndStr);
    db->CompactRange((haveRangeStart ? &rangeStart : nullptr),
            (haveRangeEnd ? &rangeEnd : nullptr));
    logger.trace("Finished compacting table " + std::to_string(tableId));
    //Create the response if neccessary
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
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
    if (!expectNextFrame("Only table ID frame found in delete rangee request, ĺimit frame missing",
            generateResponse,
            errorResponse)) {
        return;
    }
    //Parse limit frame. For now we just assume UINT64_MAX is close enough to infinite
    uint64_t scanLimit;
    if (!parseUint64FrameOrAssumeDefault(scanLimit,
                std::numeric_limits<uint64_t>::max(),
                "Receive scan limit frame",
                true, errorResponse)) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in delete range request, range missing",
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
        //Check limit
        if (scanLimit <= 0) {
            break;
        }
        scanLimit--;
        //Check range end
        leveldb::Slice key = it->key();
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        //Both checks passed, delete it
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
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
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
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
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
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
    }
}

void UpdateWorker::handleTableTruncateRequest(zmq_msg_t* headerFrame, bool generateResponse) {
    static const char* errorResponse = "\x31\x01\x04\x01";
    static const char* ackResponse = "\x31\x01\x04\x00";
    zmq_msg_close(headerFrame);
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse,
            errorResponse)) {
        return;
    }
    //Close the table
    tableOpenHelper.truncateTable(tableId);
    //Create the response
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
    }
}

/**
 * Pretty stubby update thread loop.
 * This is what should contain the scheduler client code in the future.
 */
static void updateWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace, ConfigParser& configParser) {
    UpdateWorker updateWorker(ctx, tablespace, configParser);
    while (true) {
        if (!updateWorker.processNextMessage()) {
            break;
        }
    }
}


UpdateWorkerController::UpdateWorkerController(zctx_t* context, Tablespace& tablespace, ConfigParser& configParserArg)
: tablespace(tablespace),
numThreads(3),
context(context),
configParser(configParserArg)
 {
    //Initialize the push socket
    workerPushSocket = zsocket_new(context, ZMQ_PUSH);
    zsocket_set_hwm(workerPushSocket, configParser.getInternalHWM());
    zsocket_bind(workerPushSocket, updateWorkerThreadAddr);
}

void UpdateWorkerController::start() {
    threads = new std::thread*[numThreads];
    for (unsigned int i = 0; i < numThreads; i++) {
        threads[i] = new std::thread(updateWorkerThreadFunction,
                                     context,
                                     std::ref(tablespace),
                                     std::ref(configParser)
                                    );
    }
}

void COLD UpdateWorkerController::terminateAll() {
    if(workerPushSocket) {
        //Send an empty STOP message for each update thread
        for (unsigned int i = 0; i < numThreads; i++) {
            //Send an empty msg (signals the table open thread to stop)
            sendEmptyFrameMessage(workerPushSocket);
        }
        //Wait for each thread to exit
        for (unsigned int i = 0; i < numThreads; i++) {
            threads[i]->join();
            delete threads[i];
        }
        numThreads = 0;
        //Destroy the sockets, if any
        zsocket_destroy(context, workerPushSocket);
        workerPushSocket = nullptr;
    }
}

UpdateWorkerController::~UpdateWorkerController() {
    //Gracefully terminate any update worker that is left
    terminateAll();
    //Free the threadlist
    delete[] threads;
}

void UpdateWorkerController::send(zmsg_t** msg) {
    zmsg_send(msg, workerPushSocket);
}
