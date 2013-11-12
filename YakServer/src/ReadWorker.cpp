/* 
 * File:   UpdateWorker.cpp
 * Author: uli
 * 
 * Created on 23. April 2013, 10:35
 */

#include "ReadWorker.hpp"
#include <zmq.h>
#include <string>
#include <iostream>
#include "TableOpenHelper.hpp"
#include "Tablespace.hpp"
#include "protocol.hpp"
#include "zutil.hpp"
#include "Logger.hpp"
#include "endpoints.hpp"
#include "macros.hpp"
#include "ThreadUtil.hpp"

/**
 * The main function for the read worker thread.
 */
static void readWorkerThreadFunction(void* ctx, Tablespace& tablespace) {
    setCurrentThreadName("Yak read worker");
    ReadWorker readWorker(ctx, tablespace);
    //Process requests until stop msg is encountered
    while (readWorker.processNextRequest()) {
    }
}

using namespace std;

ReadWorkerController::ReadWorkerController(void* context, Tablespace& tablespace) :  tablespace(tablespace), numThreads(3), context(context) {
    //Initialize the push socket
    workerPushSocket = zmq_socket_new_bind(context, ZMQ_PUSH, readWorkerThreadAddr);
}

void ReadWorkerController::start() {
    threads = new std::thread*[numThreads];
    for (int i = 0; i < numThreads; i++) {
        threads[i] = new std::thread(readWorkerThreadFunction, context, std::ref(tablespace));
    }
}

void COLD ReadWorkerController::terminateAll() {
    //Send an empty STOP message for each read worker threadt
    for (int i = 0; i < numThreads; i++) {
        //Send an empty msg (signals the table open thread to stop)
        sendEmptyFrameMessage(workerPushSocket);
    }
    //Wait for each thread to exit
    for (int i = 0; i < numThreads; i++) {
        threads[i]->join();
        delete threads[i];
    }
    numThreads = 0;
    //Destroy the sockets, if any
    if(workerPushSocket) {
        zmq_close(workerPushSocket);
        workerPushSocket = nullptr;
    }
}

ReadWorkerController::~ReadWorkerController() {
    //Gracefully terminate all workers
    terminateAll();
    //Free the threadlist
    delete[] threads;
}

void ReadWorkerController::send(zmsg_t** msg) {
    zmsg_send(msg, workerPushSocket);
}

ReadWorker::ReadWorker(void* ctx, Tablespace& tablespace) :
AbstractFrameProcessor(ctx, ZMQ_PULL, ZMQ_PUSH, "Read worker"),
tablespace(tablespace),
tableOpenHelper(ctx) {
    //Connect the socket that is used to proxy requests to the external req/rep socket
    zmq_connect(processorOutputSocket, externalRequestProxyEndpoint);
    //Connect the socket that is used by the send() member function
    zmq_connect(processorInputSocket, readWorkerThreadAddr);
    logger.debug("Read worker thread starting");
}

ReadWorker::~ReadWorker() {
    logger.debug("Read worker thread stopping...");
    //Sockets are cleaned up in AbstractFrameProcessor
}

void ReadWorker::handleExistsRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x12\x01";
    static const char* ackResponse = "\x31\x01\x12\x00";
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId,
            "Table ID frame in exists request",
            true,
            errorResponse, headerFrame)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    string value;
    //If there are no keys at all, just send ACK without SNDMORE, else with SNDMORE
    bool dataFramesAvailable = socketHasMoreFrames(processorInputSocket);
    sendResponseHeader(headerFrame, ackResponse, (dataFramesAvailable ? ZMQ_SNDMORE : 0));
    //Read each read request
    zmq_msg_t keyFrame;
    zmq_msg_t previousResponse; //Needed to only send last frame without SNDMORE
    bool havePreviousResponse = false;
    while (socketHasMoreFrames(processorInputSocket)) {
        zmq_msg_init(&keyFrame);
        if (unlikely(!receiveMsgHandleError(&keyFrame, "Receive exists key frame", errorResponse, true, headerFrame))) {
            return;
        }
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));

        leveldb::Status status = db->Get(readOptions, key, &value);
        if (unlikely(!checkLevelDBStatus(status, "LevelDB error while checking key for existence", true, errorResponse, headerFrame))) {
            logger.trace("The key that caused the previous error was " + std::string((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame)));
            zmq_msg_close(&keyFrame);
            return;
        }
        zmq_msg_close(&keyFrame);
        //Send the previous response, if any
        if (havePreviousResponse) {
            if (unlikely(!sendMsgHandleError(&previousResponse, ZMQ_SNDMORE, "ZMQ error while sending exists reply (not last)", errorResponse, headerFrame))) {
                return;
            }
        }
        //Generate the response for the current key
        havePreviousResponse = true;
        if (status.IsNotFound()) {
            zmq_msg_init_data(&previousResponse, (void*) "\x00", 1, nullptr, nullptr);
        } else {
            zmq_msg_init_data(&previousResponse, (void*) "\x01", 1, nullptr, nullptr);
        }
    }
    //Send the last response, if any (last msg, without MORE)
    if (havePreviousResponse) {
        if (unlikely(!sendMsgHandleError(&previousResponse,
                0, "ZMQ error while sending last exists reply",
                errorResponse, headerFrame))) {
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
            true, errorResponse, headerFrame)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    string value;
    //If there are no keys at all, just send ACK without SNDMORE, else with SNDMORE
    bool dataFramesAvailable = socketHasMoreFrames(processorInputSocket);
    sendResponseHeader(headerFrame, ackResponse, (dataFramesAvailable ? ZMQ_SNDMORE : 0));
    //Read each read request
    zmq_msg_t keyFrame;
    zmq_msg_t previousResponse; //Needed to only send last frame without SNDMORE
    bool havePreviousResponse = false;
    while (socketHasMoreFrames(processorInputSocket)) {
        zmq_msg_init(&keyFrame);
        if (unlikely(!receiveMsgHandleError(&keyFrame, "Receive read key frame", errorResponse, headerFrame))) {
            return;
        }
        //Build a slice of the key (zero-copy)
        leveldb::Slice key((char*) zmq_msg_data(&keyFrame), zmq_msg_size(&keyFrame));
        status = db->Get(readOptions, key, &value);
        zmq_msg_close(&keyFrame);
        if (unlikely(!checkLevelDBStatus(status, "LevelDB error while reading key", true, errorResponse, headerFrame))) {
            logger.trace("The key that caused the error was " + key.ToString());
            zmq_msg_close(&keyFrame);
            return;
        }
        //Send the previous response, if any
        if (havePreviousResponse) {
            if (unlikely(!sendMsgHandleError(&previousResponse,
                                             ZMQ_SNDMORE,
                                             "ZMQ error while sending read reply (not last)",
                                             errorResponse,
                                             headerFrame))) {
                return;
            }
        }
        //Generate the response for the current read key
        havePreviousResponse = true;
        if (status.IsNotFound()) {
            //Empty value
            zmq_msg_init_data(&previousResponse, (void*) "", 0, nullptr, nullptr);
        } else {
            //Found sth, return value
            zmq_msg_init_size(&previousResponse, value.size());
            memcpy(zmq_msg_data(&previousResponse), value.c_str(), value.size());
        }
    }
    //Send the last response, if any (last msg, without MORE)
    if (havePreviousResponse) {
        if (unlikely(!sendMsgHandleError(&previousResponse, 0, "ZMQ error while sending last read reply", errorResponse))) {
            return;
        }
    }
}

void ReadWorker::handleScanRequest(zmq_msg_t* headerFrame) {
    static const char* errorResponse = "\x31\x01\x13\x01";
    static const char* ackResponse = "\x31\x01\x13\x00";
    //Parse scan flags
    if (!expectExactFrameSize(headerFrame, 4, "scan request header frame", errorResponse, true, headerFrame, 4)) {
        return;
    }
    uint8_t scanFlags = ((char*)zmq_msg_data(headerFrame))[3];
    bool invertScanDirection = (scanFlags & ScanFlagInvertDirection) != 0;
    //Parse table ID
    uint32_t tableId;
    if (!parseUint32Frame(tableId, "Table ID frame in scan request", true, errorResponse, headerFrame, 4)) {
        return;
    }
    //Check if there is a range frame at all
    if (!expectNextFrame("Only table ID frame found in scan request, range missing", true, errorResponse, headerFrame, 4)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse limit frame. For now we just assume UINT64_MAX is close enough to infinite
    uint64_t scanLimit;
    if (!parseUint64FrameOrAssumeDefault(scanLimit,
                std::numeric_limits<uint64_t>::max(),
                "Receive scan limit frame",
                true, errorResponse, headerFrame, 4)) {
        return;
    }
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    if (!parseRangeFrames(rangeStartStr, rangeEndStr, "Scan request scan range parsing", errorResponse, true, headerFrame, 4)) {
        return;
    }
    bool haveRangeStart = !(rangeStartStr.empty());
    bool haveRangeEnd = !(rangeEndStr.empty());
    //Parse the filter frames
    std::string keyFilterStr = "";
    std::string valueFilterStr = "";
    if(!expectNextFrame("Expected key filter frame", true, errorResponse, headerFrame, 4)) {
        return;
    }
    if(!receiveStringFrame(keyFilterStr, "Error while receiveing key filter string", errorResponse, true, headerFrame, 4)) {
        return;
    }
    if(!expectNextFrame("Expected value filter frame", true, errorResponse, headerFrame, 4)) {
        return;
    }
    if(!receiveStringFrame(valueFilterStr, "Error while receiveing key filter string", errorResponse, true, headerFrame, 4)) {
        return;
    }
    //Create the boyer moore searchers (unexpensive for empty strings)
    bool haveKeyFilter = !(keyFilterStr.empty());
    bool haveValueFilter = !(valueFilterStr.empty());
    BoyerMooreHorspoolSearcher keyFilter(keyFilterStr);
    BoyerMooreHorspoolSearcher valueFilter(valueFilterStr);
    //Convert the str to a slice, to compare the iterator slice in-place
    leveldb::Slice rangeEndSlice(rangeEndStr);
    //Do the compaction (takes LONG)
    //Create the response object
    leveldb::ReadOptions readOptions;
    leveldb::Status status;
    //Create the iterator
    leveldb::Iterator* it = db->NewIterator(readOptions);
    if (haveRangeStart) {
        it->Seek(rangeStartStr);
    } else if(invertScanDirection) {
        it->SeekToLast();
    } else { //Non-inverted scan
        it->SeekToFirst();
    }
    //If the range is empty, the header needs to be sent w/out MORE,
    // so we can't send it right away
    bool sentHeader = false;
    //Iterate over all key-values in the range
    zmq_msg_t keyMsg, valueMsg;
    bool haveLastValueMsg = false; //Needed to send only last frame without SNDMORE
    for (; it->Valid(); (invertScanDirection ? it->Prev() : it->Next())) {
        leveldb::Slice key = it->key();
        const char* keyData = key.data();
        size_t keySize = key.size();
        //Check scan limit
        if (scanLimit <= 0) {
            break;
        }
        scanLimit--;
        //Check if we have to stop here
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        leveldb::Slice value = it->value();
        const char* valueData = value.data();
        size_t valueSize = value.size();
        //Check key / value filters, if any
        if(haveKeyFilter && keyFilter.find(keyData, keySize) == -1) {
            scanLimit++; //Revert decrement from above
            continue; //Next key/value, if any
        }
        if(haveValueFilter && valueFilter.find(valueData, valueSize) == -1) {
            scanLimit++; //Revert decrement from above
            continue; //Next key/value, if any
        }
        //Send the previous value msg, if any
        if (!sentHeader) {
            sendResponseHeader(headerFrame, ackResponse, ZMQ_SNDMORE);
            sentHeader = true;
        }
        if (haveLastValueMsg) {
            if (unlikely(!sendMsgHandleError(&valueMsg, ZMQ_SNDMORE, "ZMQ error while sending read reply (not last)", errorResponse))) {
                delete it;
                return;
            }
        }
        //Convert the slices into msgs and send them
        haveLastValueMsg = true;
        zmq_msg_init_size(&keyMsg, keySize);
        zmq_msg_init_size(&valueMsg, valueSize);
        memcpy(zmq_msg_data(&keyMsg), keyData, keySize);
        memcpy(zmq_msg_data(&valueMsg), valueData, valueSize);
        if (unlikely(!sendMsgHandleError(&keyMsg, ZMQ_SNDMORE, "ZMQ error while sending scan reply (not last)", errorResponse))) {
            zmq_msg_close(&valueMsg);
            delete it;
            return;
        }
    }
    //Send the previous value msg, if any
    if (haveLastValueMsg) {
        if (unlikely(!sendMsgHandleError(&valueMsg, 0, "ZMQ error while sending last scan reply", errorResponse))) {
            delete it;
            return;
        }
    }
    //If the scanned range is empty, the header has not been sent yet
    if (!sentHeader) {
        sendResponseHeader(headerFrame, ackResponse);
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(),
            "LevelDB error while scanning",
            true,
            errorResponse, headerFrame)) {
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
    if (!parseUint32Frame(tableId, "Table ID frame in count request", true, errorResponse, headerFrame)) {
        return;
    }
    //Check if there is a range frame at all
    if (!expectNextFrame("Only table ID frame found in count request, range missing", true, errorResponse, headerFrame)) {
        return;
    }
    //Get the table to read from
    leveldb::DB* db = tablespace.getTable(tableId, tableOpenHelper);
    //Parse the from-to range
    std::string rangeStartStr;
    std::string rangeEndStr;
    parseRangeFrames(rangeStartStr, rangeEndStr, "Count request compact range parsing", errorResponse, headerFrame);
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
    if (haveRangeStart) {
        it->Seek(rangeStartStr);
    } else {
        it->SeekToFirst();
    }
    uint64_t count = 0;
    //Iterate over all key-values in the range
    for (; it->Valid(); it->Next()) {
        leveldb::Slice key = it->key();
        if (haveRangeEnd && key.compare(rangeEndSlice) >= 0) {
            break;
        }
        count++;
    }
    //Check if any error occured during iteration
    if (!checkLevelDBStatus(it->status(), "LevelDB error while counting", true, errorResponse, headerFrame)) {
        delete it;
        return;
    }
    delete it;
    //Send ACK and count
    sendResponseHeader(headerFrame, ackResponse, ZMQ_SNDMORE);
    sendBinary<uint64_t>(count, processorOutputSocket, logger);
}

bool ReadWorker::processNextRequest() {
    zmq_msg_t routingFrame, delimiterFrame, headerFrame;
    //Read routing info
    zmq_msg_init(&routingFrame);
    if(receiveLogError(&routingFrame, processorInputSocket, logger, "Routing frame") == -1) {
        return true;
    }
    //Empty frame means: Stop thread
    if (zmq_msg_size(&routingFrame) == 0) {
        zmq_msg_close(&routingFrame);
        return false;
    }
    //If it isn't empty, we expect to see the delimiter frame
    if (!expectNextFrame("Received nonempty routing frame, but no delimiter frame", false, "\x31\x01\xFF\xFF")) {
        zmq_msg_close(&routingFrame);
        return true;
    }
    zmq_msg_init(&delimiterFrame);
    if(receiveExpectMore(&delimiterFrame, processorInputSocket, logger, "Delimiter frame") == -1) {
        return true;
    }
    //Write routing info to the output socket immediately
    zmq_msg_send(&routingFrame, processorOutputSocket, ZMQ_SNDMORE);
    zmq_msg_send(&delimiterFrame, processorOutputSocket, ZMQ_SNDMORE);
    //Receive the header frame
    zmq_msg_init(&headerFrame);
    if (unlikely(!receiveMsgHandleError(&headerFrame, "Receive header frame in read worker thread", "\x31\x01\xFF\xFF", true))) {
        return true;
    }
    assert(isHeaderFrame(&headerFrame));
    //Get the request type
    RequestType requestType = getRequestType(&headerFrame);
    //Process the rest of the frame
    if (requestType == ReadRequest) {
        handleReadRequest(&headerFrame);
    } else if (requestType == CountRequest) {
        handleCountRequest(&headerFrame);
    } else if (requestType == ExistsRequest) {
        handleExistsRequest(&headerFrame);
    } else if (requestType == ScanRequest) {
        handleScanRequest(&headerFrame);
    } else {
        std::string errstr = "Internal routing error: request type " + std::to_string((int) requestType) + " routed to read worker thread!";
        logger.error(errstr);
        sendConstFrame("\x31\x01\xFF", 3, processorOutputSocket, logger, "Internal routing error header", ZMQ_SNDMORE);
        sendFrame(errstr, processorOutputSocket, logger, "Internal routing error error message");
    }
    /**
     * In some cases (especially errors) the msg part input queue is clogged
     * up with frames that have not yet been processed.
     * Clear them
     */
    disposeRemainingMsgParts();
    return true;
}
