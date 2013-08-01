#include <zmq.h>
#include "MetaRequests.hpp"
#include "../include/zmq_utils.hpp"

static void TableOpenRequest::sendRequest(void* socket, uint32_t tableNo,
        uint64_t lruCacheSize,
        uint64_t tableBlockSize,
        uint64_t writeBufferSize,
        uint64_t bloomFilterSize,
        bool enableCompression) {
    char header[] = "\x31\x01\x01\x00";
    if (enableCompression) {
        header[3] = '\x01';
    }
    sendBinaryFrame(socket, header, 4, ZMQ_SNDMORE);
    sendBinaryFrame(socket, &tableNo, sizeof (uint32_t), ZMQ_SNDMORE);
    //LRU cache size
    if (lruCacheSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &lruCacheSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Table block size
    if (tableBlockSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &tableBlockSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Write buffer size
    if (writeBufferSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, &writeBufferSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Bloom filter size
    if (bloomFilterSize == UINT64_MAX) {
        sendEmptyFrame(socket, 0);
    } else {
        sendBinaryFrame(socket, &bloomFilterSize, sizeof (uint64_t), 0);
    }
    return 0;
}

static int TableOpenRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

static void TableCloseRequest::sendRequest(void* socket, uint32_t tableNum) {
    sendBinaryFrame(socket, "\x31\x01\x02\x00", 4, 0);
}

static int TableCloseRequest::receiveResponse(void* socket, std::string& errorString) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (rc == -1) {
        return zmq_errno();
    }
    zmq_msg_close(&msg);
    if (zmq_msg_data(&msg)[3] != 0) {
        //Server indicated error
        receiveStringFrame(socket, errorString);
        return -1;
    }
    return 0;
}

static int CompactRequest::sendRequest(void* socket, uint32_t tableNum, const std::string& startKey, const std::string& endKey) {
    sendBinaryFrame(socket, "\x31\x01\x03\x00", 4, ZMQ_SNDMORE);
    //If the strings are empty, zero-length frames are generated automatically
    sendUint32Frame(socket, tableNum, ZMQ_SNDMORE);
    sendStringFrame(socket, startKey, ZMQ_SNDMORE);
    sendStringFrame(socket, endKey);
}

static int CompactRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

static int CompactRequest::sendRequest(void* socket, uint32_t tableNum, const std::string& startKey, const std::string& endKey) {
    sendBinaryFrame(socket, "\x31\x01\x03\x00", 4, ZMQ_SNDMORE);
    //If the strings are empty, zero-length frames are generated automatically
    sendUint32Frame(socket, tableNum, 0);
}

static int CompactRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}