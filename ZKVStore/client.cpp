#include "client.h"
#include <cstring>

zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x10))
    zmsg_addstr(msg, "\x31\x01\x10");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof(uint32_t));
    //Add the key to be read
    zmsg_addmem(msg, key, keyLength);
    return msg;
}


zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, size_t keyLength, const char* value, size_t valueLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x20))
    zmsg_addstr(msg, "\x31\x01\x20");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof(uint32_t));
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


void parseReadRequestResult(zmsg_t* readRequest, std::vector<std::string>& dataVector) {
    zframe_t* header = zmsg_first(readRequest);
    zframe_t* dataFrame = NULL;
    while((dataFrame = zmsg_next(readRequest)) != NULL) {
        dataVector.push_back(std::string((char*)zframe_data(dataFrame), zframe_size(dataFrame)));
    }
}