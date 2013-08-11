#include <zmq.h>
#include <cstdint>  
#include "MetaRequests.hpp"
#include "../include/zeromq_utils.hpp"

int ServerInfoRequest::sendRequest(void* socket) {
    sendBinaryFrame(socket, "\x31\x01\x00", 3);
}

int ServerInfoRequest::receiveFeatureFlags(void* socket, uint64_t& flags) {
    zmq_msg_t msg;
    zmq_msg_recv(&msg, socket, 0);
    if (zmq_msg_size(&msg) < (3 + 8)) {
        return errno; //Response size does not match
    }
    char* data = (char*) zmq_msg_data(&msg);
    if (data[0] != 0x31 || data[1] != 0x01 || data[2] != 0x00) {
        return 1; //Magic byte mismatch
    }
    flags = ((uint64_t*) (data + 3))[0];
    zmq_msg_close(&msg);
    return 0; //OK
}

int ServerInfoRequest::receiveVersion(void* socket, std::string& serverVersion) {
    zmq_msg_t msg;
    int rc = zmq_msg_recv(&msg, socket, 0);
    if (!rc) {
        return rc;
    }
    serverVersion = std::string((char*) zmq_msg_data(&msg), zmq_msg_size(&msg));
    zmq_msg_close(&msg);
    return 0; //OK
}


int TableOpenRequest::sendRequest(void* socket, uint32_t tableNo,
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
    sendBinaryFrame(socket, (char*)&tableNo, sizeof (uint32_t), ZMQ_SNDMORE);
    //LRU cache size
    if (lruCacheSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, (char*)&lruCacheSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Table block size
    if (tableBlockSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, (char*)&tableBlockSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Write buffer size
    if (writeBufferSize == UINT64_MAX) {
        sendEmptyFrame(socket, ZMQ_SNDMORE);
    } else {
        sendBinaryFrame(socket, (char*)&writeBufferSize, sizeof (uint64_t), ZMQ_SNDMORE);
    }
    //Bloom filter size
    if (bloomFilterSize == UINT64_MAX) {
        sendEmptyFrame(socket, 0);
    } else {
        sendBinaryFrame(socket, (char*)&bloomFilterSize, sizeof (uint64_t), 0);
    }
    return 0;
}

int TableOpenRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

int TableCloseRequest::sendRequest(void* socket, uint32_t tableNum) {
    sendBinaryFrame(socket, "\x31\x01\x02\x00", 4, ZMQ_SNDMORE);
}

int TableCloseRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

int CompactRequest::sendRequest(void* socket, uint32_t tableNum, const std::string& startKey, const std::string& endKey) {
    sendBinaryFrame(socket, "\x31\x01\x03\x00", 4, ZMQ_SNDMORE);
    //If the strings are empty, zero-length frames are generated automatically
    sendUint32Frame(socket, tableNum, ZMQ_SNDMORE);
    sendStringFrame(socket, startKey, ZMQ_SNDMORE);
    sendStringFrame(socket, endKey);
}

int CompactRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

int TruncateRequest::sendRequest(void* socket, uint32_t tableNum) {
    if(sendConstFrame(socket, "\x31\x01\x04\x00", 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    //If the strings are empty, zero-length frames are generated automatically
    return sendUint32Frame(socket, tableNum);
}

int TruncateRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}