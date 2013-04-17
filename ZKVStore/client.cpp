#include "client.h"

zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x10))
    zmsg_addstr(msg, "\x31\x01\x10");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof(uint32_t));
    //Add the key to be read
    zmsg_addmem(key, keyLength);
    return msg;
}


zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, size_t keyLength, const char* value, size_t valueLength) {
    zmsg_t* msg = zmsg_new();
    //Add the header (magic, protocol version, request type(0x20))
    zmsg_addstr(msg, "\x31\x01\x20");
    //Add the table number
    zmsg_addmem(msg, &tableNum, sizeof(uint32_t));
    //Add the KV pair
    zmsg_addmem(key, keyLength);
    zmsg_addmem(value, valueLength);
    return msg;
}
