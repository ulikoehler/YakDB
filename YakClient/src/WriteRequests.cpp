#include <zmq.h>
#include <string>
#include <cstdint>
#include "yakclient/WriteRequests.hpp"
#include "yakclient/zeromq_utils.hpp"

int PutRequest::sendHeader(void* socket, uint32_t table, uint8_t flags) {
    char data[] = "\x31\x01\x20\x00";
    data[3] = flags;
    //Can't use zero-copy here because of stack alloc
    if (zmq_send(socket, data, 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

int PutRequest::sendKeyValue(void* socket,
        const std::string& key,
        const std::string& value,
        bool last) {
    return sendKeyValue(socket, key.data(), key.size(), value.data(), value.size(), last);
}

int PutRequest::sendKeyValue(void* socket,
        const char* key,
        const char* value,
        bool last) {
    return sendKeyValue(socket, key, strlen(key), value, strlen(value), last);
}

int PutRequest::sendKeyValue(void* socket,
        const char* key,
        size_t keyLength,
        const char* value,
        size_t valueLength,
        bool last) {
    if(zmq_send(socket, key, keyLength, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return zmq_send(socket, value, valueLength, (last ? 0 : ZMQ_SNDMORE));
}

int PutRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}

int DeleteRequest::sendHeader(void* socket, uint32_t table, uint8_t flags) {
    char data[] = "\x31\x01\x21\x00";
    data[3] = flags;
    //Can't use zero-copy here because of stack alloc
    if (zmq_send(socket, data, 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

int DeleteRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {
    return zmq_send(socket, key.data(), key.size(), (last ? 0 : ZMQ_SNDMORE));
}

int DeleteRequest::sendKey(void* socket,
        const char* key,
        bool last) {
    return zmq_send(socket, key, strlen(key), (last ? 0 : ZMQ_SNDMORE));
}

int DeleteRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {
    return zmq_send(socket, key, keyLength, (last ? 0 : ZMQ_SNDMORE));
}

int DeleteRangeRequest::sendRequest(void* socket, uint32_t tableNum,
        const std::string& startKey,
        const std::string& endKey) {
    if (zmq_send_const(socket, "\x31\x01\x22\x00", 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    if (sendUint32Frame(socket, tableNum, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendRange(socket, startKey, endKey, 0);
}

int DeleteRequest::receiveResponse(void* socket, std::string& errorString) {
    return receiveSimpleResponse(socket, errorString);
}
