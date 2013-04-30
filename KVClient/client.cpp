#include "client.h"
#include <cstring>
#include <cassert>
#include <iostream>
#include "zutil.hpp"
#include "macros.hpp"

using namespace std;

/**
 * An instance of this class shall be returned by methods
 * that check for errors (e.g. in communication messages).
 * 
 * This allows fast checking if the result indicates an error.
 */
static struct CheckResult {
    bool error;
    Status status;
    /**
     * Construct an error check result
     */
    CheckResult(Status status) : status(status), error(true) {
        
    }
    /**
     * Construct a non-error check result
     */
    CheckResult() : error(false) {
        
    }
};

static CheckResult checkHeaderFrame() {
    //Check for errors
    if (unlikely(!headerFrame)) {
        return {true, Status("Protocol error: Header frame missing")};
    } else if (unlikely(zframe_size(headerFrame) != 4)) {
        return Status("Protocol error: Header frame size mismatch: " + headerFrame, 2);
    } else if (unlikely(zframe_data(headerFrame)[3] != 0x00)) {
        return Status("Server error: Header frame indicates error: " + zframe_data(zmsg_next(msg)), 3);
    }
}

/**
 * Executes checkHeaderFrame() and returns from the current function
 * if an error was found.
 */
#define CHECK_HEADER()

Status::Status() {

}

Status::Status(const std::string& msg, int errorCode) : errorCode(errorCode) {
    errorMessage = new std::string(msg);
}

Status::Status(Status&& other) : errorMessage(other.errorMessage), errorCode(other.errorCode) {
    other.errorMessage = nullptr;
}

bool Status::ok() {
    return unlikely(errorMessage == nullptr);
}

Status::~Status() {
    if (unlikely(errorMessage != nullptr)) {
        delete errorMessage;
    }
}

std::string Status::getErrorMessage() {
    if (likely(errorMessage) == nullptr) {
        return "";
    } else {
        return *errorMessage;
    }
}

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
    while ((dataFrame = zmsg_next(readRequest)) != nullptr) {
        dataVector.push_back(std::string((char*) zframe_data(dataFrame), zframe_size(dataFrame)));
    }
}

zmsg_t* createCountRequest(uint32_t tableNum) {
}

ReadRequest::ReadRequest(const char* key, size_t keySize, uint32_t tablenum) : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x10", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    zmsg_addmem(msg, key, keySize);
}

ReadRequest::ReadRequest(const char* key, uint32_t tablenum) : this(key, strlen(key), tablenum) {
}

ReadRequest::ReadRequest(const std::string& value, uint32_t tableNum) : this(value.c_str(), value.size(), tableNum) {
}

Status ReadRequest::executeSingle(void* socket, std::string& value) {
    //Send the read request
    if (unlikely(zmsg_send(&msg, socket))) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    R
    zframe_t* headerFrame = zmsg_first(msg);
    assert(headerFrame);
    //Check for errors
    if (unlikely(!headerFrame)) {
        return Status("Protocol error: Header frame missing");
    }
    if (unlikely(zframe_size(headerFrame) != 4)) {
        return Status("Protocol error: Header frame size mismatch: " + headerFrame, 2);
    }
    if (unlikely(zframe_data(headerFrame)[3] != 0x00)) {
        return Status("Server error: Header frame indicates error: " + zframe_data(zmsg_next(msg)), 3);
    }
    //In contrast to executeMultiple, we only read one value.
    zframe_t* valueFrame = zmsg_next(msg);
    assert(valueFrame);
    //Set the reference to the value that has been read
    value = std::string(zframe_data(valueFrame), zmsg_data(valueFrame));
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

Status ReadRequest::executeMultiple(void* socket, std::vector<std::string> values) {
    //Send the read request
    if (zmsg_send(&msg, socket)) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    zframe_t* headerFrame = zmsg_first(msg);
    //Iterate over the value frames
    zframe_t * valueFrame = nullptr;
    while ((valueFrame == zmsg_next(msg)) != nullptr) {
        values.push_back(std::string(zframe_data(valueFrame), zmsg_data(valueFrame)));
    }
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

void ReadRequest::addKey(const std::string& key) {
    addKey(key.c_str(), key.size());
}

void ReadRequest::addKey(const char* key, size_t keySize) {
    zmsg_addmem(msg, key, keySize);

}

void ReadRequest::addKey(const char* key) {
    addKey(key, strlen(key));
}

DeleteRequest::DeleteRequest(const char* key, size_t keySize, uint32_t tablenum) : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x21", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    zmsg_addmem(msg, key, keySize);
}

DeleteRequest::DeleteRequest(const char* key, uint32_t tablenum) : this(key, strlen(key), tablenum) {
}

DeleteRequest::DeleteRequest(const std::string& value, uint32_t tableNum) : this(value.c_str(), value.size(), tableNum) {
}

int DeleteRequest::execute(void* socket) {
    //Send the read request
    if (unlikely(zmsg_send(&msg, socket))) {
        debugZMQError("Send delete request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    zframe_t* headerFrame = zmsg_first(msg);
    assert(headerFrame);
    //TODO parse response code
    //Cleanup
    zmsg_destroy(&msg);
}

void DeleteRequest::addKey(const std::string& key) {
    addKey(key.c_str(), key.size());
}

void DeleteRequest::addKey(const char* key, size_t keySize) {
    zmsg_addmem(msg, key, keySize);
}

void DeleteRequest::addKey(const char* key) {
    addKey(key, strlen(key));
}

PutRequest::PutRequest(const std::string& key, const std::string& value) noexcept : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x20", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    addKeyValue(key, value);
}

PutRequest::PutRequest(const char* key, size_t keyLength, const char* value, size_t valueLength) noexcept {

}

/**
 * Add a new key to this put request.
 * The key is added to the end of the request.
 */
void PutRequest::addKeyValue(const std::string& key, const std::string& value) {
    addKeyValue(key.c_str(), key.size(), value.c_str(), value.size());
}

void PutRequest::addKeyValue(const char* key, size_t keySize, const char* value, size_t valueSize) {
    zmsg_addmem(msg, key, keySize);
    zmsg_addmem(msg, value, valueSize);
}

void PutRequest::addKeyValue(const char* key, const char* value) {
    addKeyValue(key, strlen(key), value, strlen(value));
}

void PutRequest::execute(void* socket) {

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