/* 
 * File:   UpdateWorker.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 10:35
 */

#include "UpdateWorker.hpp"
#include <zmq.h>
#include <iostream>
#include <cassert>
#include <string>
#include <rocksdb/write_batch.h>
#include <functional>
#include <bitset>
#include "Tablespace.hpp"
#include "Logger.hpp"
#include "zutil.hpp"
#include "protocol.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "ThreadUtil.hpp"

using namespace std;

UpdateWorker::UpdateWorker(void* ctx, Tablespace& tablespace, ConfigParser& configParser) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Update worker"),
tableOpenHelper(ctx, configParser),
tablespace(tablespace),
cfg(configParser) {
    //Set HWM
    setHWM(processorInputSocket, configParser.internalRCVHWM,
            configParser.internalRCVHWM, logger);
    //Connect the socket that is used to proxy requests to the external req/rep socket
    if(zmq_connect(processorOutputSocket, externalRequestProxyEndpoint) == -1) {
        logOperationError("Connect Update worker processor output socket", logger);
    }
    //Connect the socket that is used by the send() member functions
    if(zmq_connect(processorInputSocket, updateWorkerThreadAddr)) {
        logOperationError("Connect Update worker processor input socket", logger);
    }
    logger.trace("Update worker thread starting");
}

UpdateWorker::~UpdateWorker() {
    logger.trace("Update worker thread terminating");
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
    zmq_msg_t haveReplyAddrFrame, routingFrame, delimiterFrame;
    zmq_msg_init(&haveReplyAddrFrame);
    requestExpectedSize = 3;
    if(!receiveMsgHandleError(&haveReplyAddrFrame, "Have reply addr frame", false)) {
        return true;
    }
    //Empty frame means: Stop immediately
    if(unlikely(zmq_msg_size(&haveReplyAddrFrame) == 0)) {
        zmq_msg_close(&haveReplyAddrFrame);
        return false;
    }
    char haveReplyAddrFrameContent = ((char*) zmq_msg_data(&haveReplyAddrFrame))[0];
    zmq_msg_close(&haveReplyAddrFrame);
    //If it's not a stop msg frame, we expect a header frame
    if(!expectNextFrame("Expecting frame after reply addr frame", false)) {
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
    errorResponse = "\x31\x01\xFF\xFF";
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in update worker thread", haveReplyAddr))) {
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
    if (likely(requestType == RequestType::PutRequest)) {
        handlePutRequest(haveReplyAddr);
    } else if (requestType == RequestType::DeleteRequest) {
        handleDeleteRequest(haveReplyAddr);
    } else if (requestType == RequestType::OpenTableRequest) {
        handleTableOpenRequest(haveReplyAddr);
    } else if (requestType == RequestType::CloseTableRequest) {
        handleTableCloseRequest(haveReplyAddr);
    } else if (requestType == RequestType::CompactTableRequest) {
        handleCompactRequest(haveReplyAddr);
    } else if (requestType == RequestType::TruncateTableRequest) {
        handleTableTruncateRequest(haveReplyAddr);
    } else if (requestType == RequestType::DeleteRangeRequest) {
        handleDeleteRangeRequest(haveReplyAddr);
    } else if (requestType == RequestType::CopyRangeRequest) {
        handleCopyRangeRequest(haveReplyAddr);
    } else {
        logger.error(std::string("Internal routing error: request type ")
                + std::to_string((uint8_t)requestType) + " routed to update worker thread!");
    }
    //General cleanup
    zmq_msg_close(&headerFrame);
    /**
     * In some cases (especially errors) the msg part input queue is clogged
     * up with frames that have not yet been processed.
     * Clear them
     */
    disposeRemainingMsgParts();
    return true;
}

void UpdateWorker::handlePutRequest(bool generateResponse) {
    /**
     * IMPORTANT: We stopped using batches because they are *incredibly*
     * inefficient! They just append to a std::string on each operation.
     * This is so inefficient the worker thread sometimes needs
     * several *seconds* for a single >10k batch!
     * 
     * Instead, we re-batch into fixed-size batches. Individual puts
     * are too slow and lock too much.
     * 
     * NOTE regarding request IDs: The request has 4 bytes, instead of the default 3.
     * This needs to be passed to functions generating the response
     * header.
     */
    errorResponse = "\x31\x01\x20\x01";
    static const char* ackResponse = "\x31\x01\x20\x00";
    assert(isHeaderFrame(&headerFrame));
    //Process the flags
    uint8_t flags = getWriteFlags(&headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to RocksDB
    rocksdb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    //Get the table
    rocksdb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    rocksdb::WriteBatch batch;
    const uint32_t maxBatchSize = cfg.putBatchSize;
    uint32_t currentBatchSize = 0;
    //Check if we need to use Merge instead of Put (i.e. if we have a non-REPLACE merge operator)
    bool mergeRequired = tablespace.isMergeRequired(tableId);
    //The entire update is processed in one batch. Empty batches are allowed.
    bool haveMoreData = socketHasMoreFrames(processorInputSocket);
    zmq_msg_t keyFrame, valueFrame;
    uint64_t elementCounter = 0;
    while (haveMoreData) {
        zmq_msg_init(&keyFrame);
        zmq_msg_init(&valueFrame);
        //The next two frames contain key and value
        if (unlikely(!receiveMsgHandleError(&keyFrame,
                "Receive put key frame", errorResponse))) {
            break;
        }
        //Check if there is a key but no value
        if (!expectNextFrame("Protocol error: Found key frame, but no value frame. They must occur in pairs!",
                             generateResponse)) {
            return;
        }
        if (unlikely(!receiveMsgHandleError(&valueFrame, "Receive put value frame", generateResponse))) {
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
        //Write into batch
        rocksdb::Slice keySlice((char*) zmq_msg_data(&keyFrame), keySize);
        rocksdb::Slice valueSlice((char*) zmq_msg_data(&valueFrame), valueSize);
        if(mergeRequired) {
            batch.Merge(keySlice, valueSlice);
        } else { //A simple put is enough (REPLACE merge operator)
            batch.Put(keySlice, valueSlice);
        }
        currentBatchSize++;
        //If batch is full, write to db
        if(currentBatchSize >= maxBatchSize) {
            rocksdb::Status status = db->Write(writeOptions, &batch);
            if (!checkRocksDBStatus(status,
                    "Database error while processing update request: ",
                    generateResponse)) {
                return;
            }
            batch.Clear();
            currentBatchSize = 0;
        }
        //Statistics
        elementCounter++;
        //Cleanup
        zmq_msg_close(&keyFrame);
        zmq_msg_close(&valueFrame);
    }
    //Write last batch part
    rocksdb::Status status = db->Write(writeOptions, &batch);
    if (!checkRocksDBStatus(status, "Database error while processing update request: ", generateResponse)) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendResponseHeader(ackResponse);
    }
}

void UpdateWorker::handleDeleteRequest(bool generateResponse) {
    /*
    * NOTE regarding request IDs: The request has 4 bytes, instead of the default 3.
    * This needs to be passed to functions generating the response
    * header.
    */
    errorResponse = "\x31\x01\x21\x01";
    static const char* ackResponse = "\x31\x01\x21\x00";
    requestExpectedSize = 4;
    //Process the flags
    uint8_t flags = getWriteFlags(&headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to RocksDB
    rocksdb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    bool haveMoreData = socketHasMoreFrames(processorInputSocket);
    //Get the table
    rocksdb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //The entire update is processed in one batch. This seems reasonable because
    // delete batch requests
    zmq_msg_t keyFrame;
    rocksdb::WriteBatch batch;
    while (haveMoreData) {
        zmq_msg_init(&keyFrame);
        //The next two frames contain key and value
        if (unlikely(!receiveMsgHandleError(&keyFrame, "Receive deletion key frame", generateResponse))) {
            return;
        }
        //Convert to RocksDB
        rocksdb::Slice keySlice((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        batch.Delete(keySlice);
        //Check if we have more frames
        haveMoreData = zmq_msg_more(&keyFrame);
        //Cleanup
        zmq_msg_close(&keyFrame);
    }
    //Commit the batch
    rocksdb::Status status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkRocksDBStatus(status, "Database error while processing delete request: ", generateResponse)) {
        return;
    }
    //Send success code
    if (generateResponse) {
        //Send success code
        sendResponseHeader(ackResponse);
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
void UpdateWorker::handleCompactRequest(bool generateResponse) {
    static const char* ackResponse = "\x31\x01\x03\x00";
    //Parse table ID
    uint32_t tableId;
    errorResponse = "\x31\x01\x03\x10";
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    errorResponse = "\x31\x01\x03\x01";
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in compact request, range missing", generateResponse)) {
        return;
    }
    //Get the table
    rocksdb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr, rangeEndStr, "Compact request compact range parsing", errorResponse);
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Do the compaction (takes LONG, so log it before)
    logger.debug("Compacting table " + std::to_string(tableId));
    rocksdb::Slice rangeStart(rangeStartStr);
    rocksdb::Slice rangeEnd(rangeEndStr);

    //Perform compaction
    rocksdb::CompactRangeOptions options;
    db->CompactRange(options, (haveRangeStart ? &rangeStart : nullptr),
            (haveRangeEnd ? &rangeEnd : nullptr));
    logger.trace("Finished compacting table " + std::to_string(tableId));
    //Create the response if neccessary
    if (generateResponse) {
        sendResponseHeader(ackResponse);
    }
}

void UpdateWorker::handleDeleteRangeRequest(bool generateResponse) {
    /*
    * NOTE regarding request IDs: The request has 4 bytes, instead of the default 3.
    * This needs to be passed to functions generating the response
    * header.
    */
    errorResponse = "\x31\x01\x22\x01";
    static const char* ackResponse = "\x31\x01\x22\x00";
    //Process the flags
    uint8_t flags = getWriteFlags(&headerFrame);
    bool fullsync = isFullsync(flags); //= Send reply after flushed to disk
    //Convert options to RocksDB
    rocksdb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in delete range request, ĺimit frame missing", generateResponse)) {
        return;
    }
    //Parse limit frame. For now we just assume UINT64_MAX is close enough to infinite
    uint64_t scanLimit;
    if (!parseUint64FrameOrAssumeDefault(scanLimit,
                std::numeric_limits<uint64_t>::max(),
                "Receive scan limit frame",
                true)) {
        return;
    }
    //Check if there is a range frames
    if (!expectNextFrame("Only table ID frame found in delete range request, range missing",
            generateResponse)) {
        return;
    }
    //Get the table
    rocksdb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr,
            rangeEndStr, "Parsing delete range request key range frames");
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Convert the str to a slice, to compare the iterator slice in-place
    rocksdb::Slice rangeEndSlice(rangeEndStr);
    //Create the response object
    rocksdb::ReadOptions readOptions;
    rocksdb::Status status;
    //Create the iterator
    rocksdb::Iterator* it = db->NewIterator(readOptions);
    //All deletes are applied in one batch
    // This also avoids construct like deleting while iterating
    rocksdb::WriteBatch batch;
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
        rocksdb::Slice key = it->key();
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        //Both checks passed, delete it
        batch.Delete(key);
    }
    //Check if any error occured during iteration
    if (!checkRocksDBStatus(it->status(), "RocksDB error while processing delete request", true)) {
        delete it;
        return;
    }
    delete it;
    //Apply the batch
    status = db->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkRocksDBStatus(status,
            "Database error while processing delete request: ", generateResponse)) {
        return;
    }
    //Create the response if neccessary
    if (generateResponse) {
        sendResponseHeader(ackResponse);
    }
}

void UpdateWorker::handleCopyRangeRequest(bool generateResponse) {
    /*
    * NOTE regarding request IDs: The request has 4 bytes, instead of the default 3.
    * This needs to be passed to functions generating the response
    * header.
    */
    errorResponse = "\x31\x01\x24\x01";
    static const char* ackResponse = "\x31\x01\x24\x00";
    //Process the flags
    uint8_t writeFlags = getWriteFlags(&headerFrame);
    uint8_t copyFlags = getCopyFlags(&headerFrame);
    bool fullsync = isFullsync(writeFlags); //= Send reply after flushed to disk
    bool synchronousDelete = isSynchronousDelete(copyFlags); //= Send reply after flushed to disk
    //Convert options to RocksDB
    rocksdb::WriteOptions writeOptions;
    writeOptions.sync = fullsync;
    //Parse table IDs
    uint32_t sourceTableId;
    if (!parseUint32Frame(sourceTableId, "Source table ID frame", generateResponse)) {
        return;
    }
    uint32_t targetTableId;
    if (!parseUint32Frame(targetTableId, "Target table ID frame", generateResponse)) {
        return;
    }
    //Check if the limit frame is present at all
    if (!expectNextFrame("Only table ID frame found in copy range request, ĺimit frame missing", generateResponse)) {
        return;
    }
    //Parse limit frame. For now we just assume UINT64_MAX is close enough to infinite
    uint64_t scanLimit;
    if (!parseUint64FrameOrAssumeDefault(scanLimit,
                std::numeric_limits<uint64_t>::max(),
                "Receive scan limit frame",
                true)) {
        return;
    }
    //Check if there are range frames
    if (!expectNextFrame("Only table ID frame found in delete range request, range missing",
            generateResponse)) {
        return;
    }
    //Get the table
    rocksdb::DB* sourceTable = tablespace.getTable(sourceTableId, tableOpenHelper);
    rocksdb::DB* targetTable = tablespace.getTable(targetTableId, tableOpenHelper);
    bool mergeRequired = tablespace.isMergeRequired(targetTableId);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr,
            rangeEndStr, "Parsing delete range request key range frames");
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Convert the str to a slice, to compare the iterator slice in-place
    rocksdb::Slice rangeEndSlice(rangeEndStr);
    //Create the response object
    rocksdb::ReadOptions readOptions;
    rocksdb::Status status;
    /**
     * Perform synchronous deletion if required
     */
    if(synchronousDelete) {
        rocksdb::Iterator* it = targetTable->NewIterator(readOptions);
        // This also avoids construct like deleting while iterating
        rocksdb::WriteBatch batch;
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
            rocksdb::Slice key = it->key();
            if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
                break;
            }
            //Both checks passed, delete it
            batch.Delete(key);
        }
        //Check if any error occured during iteration
        if (!checkRocksDBStatus(it->status(), "RocksDB error while processing delete request", true)) {
            delete it;
            return;
        }
        delete it;
        //Apply the batch
        status = targetTable->Write(writeOptions, &batch);
    }
    /**
     * Perform copying of data
     */
    rocksdb::Iterator* srcIterator = targetTable->NewIterator(readOptions);
    rocksdb::WriteBatch batch;
    const uint32_t maxBatchSize = cfg.putBatchSize;
    uint32_t currentBatchSize = 0;
    //The entire update is processed in one batch. Empty batches are allowed.
    bool haveMoreData = socketHasMoreFrames(processorInputSocket);
    uint64_t elementCounter = 0;
    for (; srcIterator->Valid(); srcIterator->Next()) {
        //Check limit
        if (scanLimit <= 0) {
            break;
        }
        scanLimit--;
        //Check range end
        rocksdb::Slice key = srcIterator->key();
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        //Write into batch
        if(mergeRequired) {
            batch.Merge(srcIterator->key(), srcIterator->value());
        } else { //A simple put is enough (REPLACE merge operator)
            batch.Put(srcIterator->key(), srcIterator->value());
        }
        currentBatchSize++;
        //If batch is full, write to db
        if(currentBatchSize >= maxBatchSize) {
            rocksdb::Status status = targetTable->Write(writeOptions, &batch);
            if (!checkRocksDBStatus(status,
                    "Database error while processing copy table request (put batch subrequest): ",
                    generateResponse)) {
                return;
            }
            batch.Clear();
            currentBatchSize = 0;
        }
        //Statistics
        elementCounter++;
    }
    //Perform last write
    status = targetTable->Write(writeOptions, &batch);
    //If something went wrong, send an error response
    if (!checkRocksDBStatus(status,
            "Database error while processing delete request: ", generateResponse)) {
        return;
    }
    //Create the response if neccessary
    if (generateResponse) {
        sendResponseHeader(ackResponse);
    }
}

void UpdateWorker::handleTableOpenRequest(bool generateResponse) {
    /*
    * NOTE regarding request IDs: The request has 4 bytes, instead of the default 3.
    * This needs to be passed to functions generating the response
    * header.
    */
    errorResponse = "\x31\x01\x01\x01";
    static const char* ackResponse = "\x31\x01\x01\x00";
    /*
     * This method performs frame correctness check, the table open server
     * serializes everything in a single struct.
     */
    //Extract numeric parameters
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    //Open the table (consumes remaining frames)
    std::string ret =
        tableOpenHelper.openTable(tableId, processorInputSocket);
    //Parse the flags from the header frame
    //Create the response if neccessary
    if (generateResponse) {
        //Assemble & send response header
        unsigned char responseHeader[4] = {0x31, 0x01, 0x01, 0xFF};
        responseHeader[3] = ret.data()[0];
        bool isErrorResponse = (responseHeader[3] != 0x00);
        sendResponseHeader((const char*) responseHeader, (isErrorResponse ? ZMQ_SNDMORE : 0));
        //Send error description if response code indicates error
        if(isErrorResponse) {
            std::string errDesc = ret.substr(1);
            sendMsgHandleError(errDesc, 0, nullptr, false);
        }
    }
}

void UpdateWorker::handleTableCloseRequest(bool generateResponse) {
    errorResponse = "\x31\x01\x02\x01";
    static const char* ackResponse = "\x31\x01\x02\x00";
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    //Close the table
    tableOpenHelper.closeTable(tableId);
    //Create the response
    if (generateResponse) {
        sendConstFrame(ackResponse, 4, processorOutputSocket, logger, "ACK response");
    }
}

void UpdateWorker::handleTableTruncateRequest(bool generateResponse) {
    errorResponse = "\x31\x01\x04\x01";
    static const char* ackResponse = "\x31\x01\x04\x00";
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame", generateResponse)) {
        return;
    }
    //Close the table
    tableOpenHelper.truncateTable(tableId);
    //Create the response
    if (generateResponse) {
        sendResponseHeader(ackResponse);
    }
}

/**
 * Pretty stubby update thread loop.
 * This is what should contain the scheduler client code in the future.
 */
static void updateWorkerThreadFunction(void* ctx, Tablespace& tablespace, ConfigParser& configParser) {
    setCurrentThreadName("Yak upd worker");
    UpdateWorker updateWorker(ctx, tablespace, configParser);
    while (true) {
        if (!updateWorker.processNextMessage()) {
            break;
        }
    }
}


UpdateWorkerController::UpdateWorkerController(void* context, Tablespace& tablespace, ConfigParser& configParserArg)
: tablespace(tablespace),
numThreads(3),
context(context),
logger(context, "Update worker controller"),
configParser(configParserArg)
 {
    //Initialize the push socket
    workerPushSocket = zmq_socket_new_bind_hwm(context, ZMQ_PUSH,
        updateWorkerThreadAddr, configParser.internalRCVHWM,
        configParser.internalRCVHWM, logger);
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
        zmq_close(workerPushSocket);
        workerPushSocket = nullptr;
    }
}

UpdateWorkerController::~UpdateWorkerController() {
    //Gracefully terminate any update worker that is left
    terminateAll();
    //Free the threadlist
    delete[] threads;
}