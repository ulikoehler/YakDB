#include <zmq.h>
#include <string>
#include <cstdint>
#include "yakclient/ReadRequests.hpp"
#include "yakclient/zeromq_utils.hpp"

int ReadRequest::sendHeader(void* socket, uint32_t table) {
    if (zmq_send_const(socket, "\x31\x01\x10", 3, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

int ReadRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {
    return zmq_send(socket, key.data(), key.size(), (last ? 0 : ZMQ_SNDMORE));
}

int ReadRequest::sendKey(void* socket,
        const char* key,
        bool last) {
    return zmq_send(socket, key, strlen(key), (last ? 0 : ZMQ_SNDMORE));
}

int ReadRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {
    return zmq_send(socket, key, keyLength, (last ? 0 : ZMQ_SNDMORE));
}

int ReadRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {
    return receiveSimpleResponse(socket, errorMessage);
}

int ReadRequest::receiveResponseValue(void* socket, std::string& target) {
    return receiveStringFrame(socket, target);
}

int CountRequest::sendHeader(void* socket, uint32_t table) {
    if (zmq_send_const(socket, "\x31\x01\x11", 3, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

int CountRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {
    return zmq_send(socket, key.data(), key.size(), (last ? 0 : ZMQ_SNDMORE));
}

int CountRequest::sendKey(void* socket,
        const char* key,
        bool last) {
    return zmq_send(socket, key, strlen(key), (last ? 0 : ZMQ_SNDMORE));
}

int CountRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {
    return zmq_send(socket, key, keyLength, (last ? 0 : ZMQ_SNDMORE));
}

int CountRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {
    return receiveSimpleResponse(socket, errorMessage);
}

int CountRequest::receiveResponseValue(void* socket, std::string& target) {
    return receiveStringFrame(socket, target);
}

int ExistsRequest::sendHeader(void* socket, uint32_t table) {
    if (zmq_send_const(socket, "\x31\x01\x12", 3, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

int ExistsRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {
    return zmq_send(socket, key.data(), key.size(), (last ? 0 : ZMQ_SNDMORE));
}

int ExistsRequest::sendKey(void* socket,
        const char* key,
        bool last) {
    return zmq_send(socket, key, strlen(key), (last ? 0 : ZMQ_SNDMORE));
}

int ExistsRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {
    return zmq_send(socket, key, keyLength, (last ? 0 : ZMQ_SNDMORE));
}

int ExistsRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {
    return receiveSimpleResponse(socket, errorMessage);
}

int ExistsRequest::receiveResponseValue(void* socket) {
    return receiveBooleanFrame(socket);
}

int ScanRequest::sendRequest(void* socket, uint32_t tableNum,
        uint64_t limit,
        const std::string& startKey,
        const std::string& endKey,
        const std::string& keyFilter,
        const std::string& valueFilter,
        bool invertDirection,
        uint64_t skip
        ) {
    if (zmq_send_const(socket, (invertDirection ? "\x31\x01\x13\x01" : "\x31\x01\x13\x00"), 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if (sendUint32Frame(socket, tableNum, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if (sendUint64Frame(socket, limit, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if(sendRange(socket, startKey, endKey, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if(zmq_send(socket, keyFilter.data(), keyFilter.size(), ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if(zmq_send(socket, valueFilter.data(), valueFilter.size(), ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint64Frame(socket, skip, 0);
}

int ScanRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {
    return receiveSimpleResponse(socket, errorMessage);
}

int ScanRequest::receiveResponseValue(void* socket, std::string& keyTarget, std::string& valueTarget) {
    return receiveKeyValue(socket, keyTarget, valueTarget);
}
