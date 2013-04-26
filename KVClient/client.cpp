#include "client.h"
#include <cstring>
#include <cassert>
#include <iostream>
#include "zutil.hpp"

using namespace std;

zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x10))
    zmsg_addstr(msg, "\x31\x01\x10");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    //Add the key to be read
    zmsg_addmem(msg, key, keyLength);
    return msg;
}

zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, size_t keyLength, const char* value, size_t valueLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x20))
    zmsg_addstr(msg, "\x31\x01\x20");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    //Add the KV pair
    zmsg_addmem(msg, key, keyLength);
    zmsg_addmem(msg, value, valueLength);
    return msg;
}

zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key) {
    return buildSingleReadRequest(tableNum, key, strlen(key));
}

zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, const char* value) {
    return buildSinglePutRequest(tableNum, key, strlen(key), value, strlen(value));
}

void addKeyValueToPutRequest(zmsg_t* msg, const char* key, size_t keyLength, const char* value, size_t valueLength) {
    zmsg_addmem(msg, key, keyLength);
    zmsg_addmem(msg, value, valueLength);
}

void addKeyValueToPutRequest(zmsg_t* msg, const char* key, const char* value) {
    zmsg_addmem(msg, key, strlen(key));
    zmsg_addmem(msg, value, strlen(value));
}

void addKeyValueToReadRequest(zmsg_t* msg, const char* key, size_t keyLength) {
    zmsg_addmem(msg, key, keyLength);
}

void addKeyValueToReadRequest(zmsg_t* msg, const char* key) {
    zmsg_addmem(msg, key, strlen(key));
}

void parseReadRequestResult(zmsg_t* readRequest, std::vector<std::string>& dataVector) {
    zframe_t* header = zmsg_first(readRequest);
    zframe_t* dataFrame = NULL;
    while ((dataFrame = zmsg_next(readRequest)) != NULL) {
        dataVector.push_back(std::string((char*) zframe_data(dataFrame), zframe_size(dataFrame)));
    }
}

zmsg_t* createCountRequest(uint32_t tableNum) {
}

ReadRequest::ReadRequest(const char* key, size_t keySize, uint32_t tablenum) {
    zmsg_addmem(msg, "\x31\x01\x11", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    zmsg_addmem(msg, key, keySize);
}

ReadRequest::ReadRequest(const char* key, uint32_t tablenum) : this(key, strlen(key), tablenum) {
}

ReadRequest::ReadRequest(const std::string& value, uint32_t tableNum) : this(value.c_str(), value.size(), tableNum) {
}

ReadRequest::executeSingle(void* socket, std::string& value) {
    //Send the read request
    if (zmsg_send(&msg, socket)) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    zframe_t* headerFrame = zmsg_first(msg);
    assert(headerFrame);
    //In contrast to executeSingle, we only read one value.
    zframe_t* valueFrame = zmsg_next(msg);
    assert(valueFrame);
    //Set the reference to the value that has been read
    value = std::string(zframe_data(valueFrame), zmsg_data(valueFrame));
    //Cleanup
    zmsg_destroy(&msg);
}





void ReadRequest::executeMultiple(void* socket, std::vector<std::string> values) {
    
}

CountRequest::CountRequest(uint32_t tableNum) : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x11", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
}

CountRequest::~CountRequest() {
    if (msg) {
        zmsg_destroy(&msg);
    }
}

uint64_t CountRequest::execute(void* socket) {
    assert(msg);
    if (zmsg_send(&msg, socket)) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply
    msg = zmsg_recv(socket);
    assert(msg);
    zframe_t* headerFrame = zmsg_first(msg);
    assert(headerFrame);
    zframe_t* countFrame = zmsg_next(msg);
    assert(countFrame);
    assert(zframe_size(countFrame) == sizeof (uint64_t));
    uint64_t count = ((uint64_t*) zframe_data(countFrame))[0];
    //Cleanup the message
    zmsg_destroy(&msg);
    return count;
}