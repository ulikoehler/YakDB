#include <zmq.h>
#include "WriteRequests.hpp"
#include "zeromq_utils.hpp"


static void PutRequest::sendHeader(void* socket, uint32_t table, uint8_t flags) {
    char data[] = "\x31\x01\x20\x00";
    data[3] = flags;
    //Can't use zero-copy here because of stack alloc
    if(sendBinaryData(socket, data, 4, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}

static int PutRequest::sendKeyValue(void* socket,
        const std::string& key,
        const std::string& value,
        bool last = false) {
    return sendKeyValue(socket, key, value, last);
}

static int PutRequest::sendKeyValue(void* socket,
        const char* key,
        const char* value,
        bool last = false) {
    return sendKeyValue(socket, key, value, last);
}

static int PutRequest::sendKeyValue(void* socket,
        const char* key,
        size_t keyLength,
        const char* value,
        size_t valueLength,
        bool last = false) {
    return sendKeyValue(socket, key, keyLength, value, valueLength, last);
}