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
#include "Logger.hpp"
#include "endpoints.hpp"
#include "macros.hpp"

using namespace std;

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

void ReadWorkerController::send(zmsg_t** msg) {
    zmsg_send(msg, workerPushSocket);
}

ReadWorker::ReadWorker(zctx_t* ctx, Tablespace& tablespace) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Update worker"),
tableOpenHelper(ctx),
tablespace(tablespace) {
    //Connect the socket that is used to proxy requests to the external req/rep socket
    zsocket_connect(processorOutputSocket, externalRequestProxyEndpoint);
    //Connect the socket that is used by the send() member function
    zsocket_connect(processorInputSocket, readWorkerThreadAddr);
    logger.debug("Update worker thread starting");
}

ReadWorker::~ReadWorker() {
    logger.debug("Read worker thread stopping...");
    zsocket_destroy(context, processorOutputSocket);
    zsocket_destroy(context, processorInputSocket);
}

void ReadWorker::handleExistsRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x12\x01";
    static const char* ackResponse = "\x31\x01\x12\x00";
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId,
            "Table ID frame in exists request",
            true,
            errorResponse)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    string value;
    //If there are no keys at all, just send ACK without SNDMORE, else with SNDMORE
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        sendConstFrame(ackResponse, 4, processorOutputSocket);
    } else {
        sendConstFrame(ackResponse, 4, processorOutputSocket, ZMQ_SNDMORE);
    }
    //Read each read request
    zmq_msg_t keyFrame;
    zmq_msg_t previousResponse; //Needed to only send last frame without SNDMORE
    bool havePreviousResponse = false;
    while (socketHasMoreFrames(processorInputSocket)) {
        zmq_msg_init(&keyFrame);
        if (unlikely(!receiveMsgHandleError(&keyFrame, "Receive exists key frame", errorResponse, true))) {
            return false;
        }
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        status = db->Get(readOptions, key, &value);
        zmq_msg_close(&keyFrame);
        if (!checkLevelDBStatus(status(), "LevelDB error while checking key for existence", true, errorResponse)) {
            zmq_msg_close(&keyFrame);
            return;
        }
        //Send the previous response, if any
        if (havePreviousResponse) {
            if (unlikely(!sendMsgHandleError(&previousResponse, ZMQ_SNDMORE, "ZMQ error while sending exists reply (not last)", true, errorResponse))) {
                return;
            }
        }
        //Generate the response for the current read key
        if (status.IsNotFound()) {
            //Empty value
            zmq_msg_init_data(&previousResponse, "", 0, nullptr, nullptr);
        } else {
            //Found sth, return value
            zmq_msg_init_size(&previousResponse, value.size());
            memcpy(zmq_msg_data(&previousResponse), value.c_str(), value.size());
        }
    }
    //Send the last response, if any (last msg, without MORE)
    if (havePreviousResponse) {
        if (unlikely(!sendMsgHandleError(&previousResponse,
                0,
                "ZMQ error while sending last exists reply",
                true,
                errorResponse))) {
            return;
        }
    }
}

void ReadWorker::handleReadRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x10\x01";
    static const char* ackResponse = "\x31\x01\x10\x00";
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId,
            "Table ID frame in read request",
            true,
            errorResponse)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    string value;
    //If there are no keys at all, just send ACK without SNDMORE, else with SNDMORE
    if (unlikely(!socketHasMoreFrames(processorInputSocket))) {
        sendConstFrame(ackResponse, 4, processorOutputSocket);
    } else {
        sendConstFrame(ackResponse, 4, processorOutputSocket, ZMQ_SNDMORE);
    }
    //Read each read request
    zmq_msg_t keyFrame;
    zmq_msg_t previousResponse; //Needed to only send last frame without SNDMORE
    bool havePreviousResponse = false;
    while (socketHasMoreFrames(processorInputSocket)) {
        zmq_msg_init(&keyFrame);
        if (unlikely(!receiveMsgHandleError(&keyFrame, "Receive read key frame", errorResponse, true))) {
            return false;
        }
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        status = db->Get(readOptions, key, &value);
        zmq_msg_close(&keyFrame);
        if (!checkLevelDBStatus(status(), "LevelDB error while reading key", true, errorResponse)) {
            zmq_msg_close(&keyFrame);
            return;
        }
        //Send the previous response, if any
        if (havePreviousResponse) {
            if (unlikely(!sendMsgHandleError(&previousResponse, 0, "ZMQ error while sending read reply (not last)", true, errorResponse))) {
                return;
            }
        }
        //Generate the response for the current read key
        havePreviousResponse = true;
        if (status.IsNotFound()) {
            //Empty value
            zmq_msg_init_data(&previousResponse, "", 0, nullptr, nullptr);
        } else {
            //Found sth, return value
            zmq_msg_init_size(&previousResponse, value.size());
            memcpy(zmq_msg_data(&previousResponse), value.c_str(), value.size());
        }
    }
    //Send the last response, if any (last msg, without MORE)
    if (havePreviousResponse) {
        if (unlikely(!sendMsgHandleError(&previousResponse, 0, "ZMQ error while sending last read reply", true, errorResponse))) {
            return;
        }
    }
}

void ReadWorker::handleScanRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x13\x01";
    static const char* ackResponse = "\x31\x01\x13\x00";
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame in scan request", true, errorResponse)) {
        return;
    }
    //Check if there is a range frame at all
    if (!expectNextFrame("Only table ID frame found in scan request, range missing", true, errorResponse)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Parse the from-to range
    leveldb::Slice* rangeStart, rangeEnd;
    parseLevelDBRange(&rangeStart, &rangeEnd, "Scan request scan range parsing", errorResponse, true);
    bool haveRangeStart = (rangeStart != NULL);
    bool haveRangeEnd = (rangeEnd != NULL);
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    if (haveRangeStart) {
        it->Seek(rangeStart);
    } else {
        it->SeekToFirst();
    }
    //Send ACK and count
    sendConstFrame(ackResponse, 4, processorOutputSocket, ZMQ_SNDMORE);
    //Iterate over all key-values in the range
    zmq_msg_t keyMsg, valueMsg;
    bool haveLastValueMsg = false; //Needed to send only last frame without SNDMORE
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        leveldb::Slice value = it->value();
        //Send the previous value msg, if any
        if (haveLastValueMsg) {
            if (unlikely(!sendMsgHandleError(&valueMsg, ZMQ_SNDMORE, "ZMQ error while sending read reply (not last)", true, errorResponse))) {
                zmq_msg_close(&valueMsg);
                delete it;
                return;
            }
        }
        //Convert the slices into msgs and send them
        haveLastValueMsg = true;
        zmq_msg_init_size(&keyMsg, key.size());
        zmq_msg_init_size(&valueMsg, value.size());
        memcpy(zmq_msg_data(&keyMsg), key.data(), key.size());
        memcpy(zmq_msg_data(&valueMsg), value.data(), value.size());
        if (unlikely(!sendMsgHandleError(&keyMsg, ZMQ_SNDMORE, "ZMQ error while sending read reply (not last)", true, errorResponse))) {
            zmq_msg_close(&valueMsg);
            delete it;
            return;
        }
        //Check if we have to stop here
        if (haveRangeEnd && key >= rangeEnd) {
            break;
        }
    }
    //Send the previous value msg, if any
    if (haveLastValueMsg) {
        if (unlikely(!sendMsgHandleError(&valueMsg, 0, "ZMQ error while sending read reply (not last)", true, errorResponse))) {
            delete it;
            return;
        }
    }
    //Remove and destroy the range slices
    if (rangeStart != nullptr) {
        delete rangeStart;
    }
    if (rangeEnd != nullptr) {
        delete rangeEnd;
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(),
            "LevelDB error while scanning",
            true,
            errorResponse)) {
        delete it;
        return;
    }
    delete it;
}

void ReadWorker::handleCountRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x11\x01";
    static const char* ackResponse = "\x31\x01\x11\x00";
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame in count request", true, errorResponse)) {
        return;
    }
    //Check if there is a range frame at all
    if (!expectNextFrame("Only table ID frame found in count request, range missing", true, errorResponse)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tables.getTable(tableId, openHelper);
    //Parse the from-to range
    leveldb::Slice* rangeStart, rangeEnd;
    parseLevelDBRange(&rangeStart, &rangeEnd, "Compact request compact range parsing", errorResponse, true);
    bool haveRangeStart = (rangeStart != NULL);
    bool haveRangeEnd = (rangeEnd != NULL);
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
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
        if (haveRangeEnd && key >= *rangeEnd) {
            break;
        }
    }
    //Remove and destroy the range slices
    if (rangeStart != nullptr) {
        delete rangeStart;
    }
    if (rangeEnd != nullptr) {
        delete rangeEnd;
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(), "LevelDB error while counting", true, errorResponse)) {
        delete it;
        return;
    }
    delete it;
    //Send ACK and count
    sendConstFrame(ackResponse, 4, processorOutputSocket, ZMQ_SNDMORE);
    sendBinary<uint64_t>(count, processorOutputSocket, logger);
}

bool ReadWorker::processNextRequest() {
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    //Read routing info
    zmq_msg_init(&routingFrame);
    receiveLogError(&routingFrame, processorInputSocket, logger);
    //Empty frame means: Stop thread
    if (zmq_msg_size(&routingFrame) == 0) {
        zmq_msg_close(&routingFrame);
        return true;
    }
    //If it isn't empty, we expect to see the delimiter frame
    if (!expectNextFrame("Received nonempty routing frame, but no delimiter frame", false, "\x31\x01\xFF\xFF")) {
        zmq_msg_close(&routingFrame);
    }
    zmq_msg_init(&delimiterFrame);
    receiveExpectMore(&delimiterFrame, processorInputSocket, logger);
    //Write routing info to the output socket immediately
    zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
    zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
    //Receive the header frame
    zmq_msg_init(&headerFrame);
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in read worker thread", "\x31\x01\xFF\xFF", true))) {
        return false;
    }
    assert(isHeaderFrame(&headerFrame));
    //Get the request type
    RequestType requestType = getRequestType(headerFrame);
    //Process the rest of the frame
    if (requestType == ReadRequest) {
        handleReadRequest(headerFrame);
    } else if (requestType == CountRequest) {
        handleCountRequest(headerFrame);
    } else if (requestType == ExistsRequest) {
        handleExistsRequest(headerFrame);
    } else {
        std::string errstr = "Internal routing error: request type " + std::to_string(requestType) + " routed to read worker thread!";
        logger.error(errstr);
        sendConstFrame("\x31\x01\xFF", 3, processorOutputSocket, ZMQ_SNDMORE);
        sendFrame(errstr, processorOutputSocket);
    }
    return true;
}

/**
 * The main function for the read worker thread.
 * 
 * This function parses the header, calls the appropriate handler function
 * and sends the response for PARTSYNC requests
 */
static void readWorkerThreadFunction(zctx_t* ctx, Tablespace& tablespace) {
    ReadWorker readWorker(ctx, tablespace);
    while (true) {
        if (!readWorker.processNextMessage()) {
            break;
        }
    }
}