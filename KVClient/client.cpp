#include "client.h"
#include <cstring>
#include <cassert>
#include <iostream>
#include "zutil.hpp"
#include "macros.hpp"

using namespace std;

/**
 * Macro to check a header frame (protcol errors & return code)
 * This is used only to avoid duplicate code.
 * 
 * Not creating an inline function here avoids unncessary copying of Status objects,
 * because it would be difficult to check if an error occurred without creating an additional
 * Status copy.
 * 
 * This macro also declared the local headerFrame variable
 */
#define CHECK_HEADERFRAME(headerFrameExpr, msg) zframe_t* headerFrame = headerFrameExpr;/*Evaluate the expression exactly once, else zmsg_next() will cause headache*/\
    if(unlikely(!msg)) {\
        return Status("Communication error: Failed to receive reply", -1);\
    } else if (unlikely(!headerFrame)) {\
        zmsg_destroy(&msg);\
        return Status("Protocol error: Header frame missing", 1);\
    } else if (unlikely(zframe_size(headerFrame) != 4)) {\
        Status status(std::string("Protocol error: Header frame size mismatch (expected 4): ") + std::to_string(zframe_size(headerFrame)), 2);\
        zmsg_destroy(&msg);\
        return status;\
    } else if (unlikely(zframe_data(headerFrame)[3] != 0x00)) {\
        Status status(std::string("Server error: Header frame indicates error: ") + frameToString(zmsg_next(msg)), 3);\
        zmsg_destroy(&msg);\
        return status;\
    }

bool printErr(const Status& status, const char* action) {
    if (unlikely(!status.ok())) {
        if (strlen(action) == 0) { //No whatYouDid message 
            fprintf(stderr, "[Error] %s\n", status.getErrorMessage().c_str());
        } else {
            fprintf(stderr, "[Error] occured during %s: %s\n", action, status.getErrorMessage().c_str());
        }
        return false;
    }
    return true;
}

Status::Status() : errorMessage(nullptr) {

}

Status::Status(const std::string& msg, int errorCode) : errorCode(errorCode) {
    errorMessage = new std::string(msg);
}

Status::Status(Status&& other) : errorMessage(other.errorMessage), errorCode(other.errorCode) {
    other.errorMessage = nullptr;
}

Status::~Status() {
    if (unlikely(errorMessage != nullptr)) {
        delete errorMessage;
    }
}

bool Status::ok() const {
    return unlikely(errorMessage == nullptr);
}

std::string Status::getErrorMessage() const {
    if (likely(errorMessage == nullptr)) {
        return "";
    } else {
        return *errorMessage;
    }
}

void ReadRequest::init(const char* key, size_t keySize, uint32_t tableNum)noexcept {
    zmsg_addmem(msg, "\x31\x01\x10", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    zmsg_addmem(msg, key, keySize);
}

ReadRequest::ReadRequest(uint32_t tableNum) noexcept : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x10", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
}

ReadRequest::ReadRequest(const char* key, size_t keySize, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(key, keySize, tableNum);
}

ReadRequest::ReadRequest(const char* key, uint32_t tablenum) noexcept : msg(zmsg_new()) {
    init(key, strlen(key), tablenum);
}

ReadRequest::ReadRequest(const std::string& value, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(value.c_str(), value.size(), tableNum);
}

Status ReadRequest::executeSingle(void* socket, std::string& value) noexcept {
    assert(msg);
    //Send the read request
    if (unlikely(zmsg_send(&msg, socket))) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg)
    //In contrast to executeMultiple, we only read one value.
    zframe_t* valueFrame = zmsg_next(msg);
    assert(valueFrame);
    //Set the reference to the value that has been read
    value = std::string((char*) zframe_data(valueFrame), zframe_size(valueFrame));
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

Status ReadRequest::executeMultiple(void* socket, std::vector<std::string> values) noexcept {
    //Send the read request
    if (zmsg_send(&msg, socket)) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg)
    //Iterate over the value frames
    zframe_t * valueFrame = nullptr;
    while ((valueFrame = zmsg_next(msg)) != nullptr) {
        values.push_back(std::string((char*) zframe_data(valueFrame), zframe_size(valueFrame)));
    }
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

void ReadRequest::addKey(const std::string& key) noexcept {
    addKey(key.c_str(), key.size());
}

void ReadRequest::addKey(const char* key, size_t keySize) noexcept {
    zmsg_addmem(msg, key, keySize);
}

void ReadRequest::addKey(const char* key) noexcept {
    addKey(key, strlen(key));
}

void DeleteRequest::init(const char* key, size_t keySize, uint32_t tableNum) noexcept {
    zmsg_addmem(msg, "\x31\x01\x21", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    zmsg_addmem(msg, key, keySize);
}

DeleteRequest::DeleteRequest(const char* key, size_t keySize, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(key, keySize, tableNum);
}

DeleteRequest::DeleteRequest(const char* key, uint32_t tablenum) noexcept : msg(zmsg_new()) {
    init(key, strlen(key), tablenum);
}

DeleteRequest::DeleteRequest(const std::string& value, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(value.c_str(), value.size(), tableNum);
}

Status DeleteRequest::execute(void* socket) noexcept {
    //Send the read request
    if (unlikely(zmsg_send(&msg, socket))) {
        debugZMQError("Send delete request", errno);
    }
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg)
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

void DeleteRequest::addKey(const std::string& key) noexcept {
    addKey(key.c_str(), key.size());
}

void DeleteRequest::addKey(const char* key, size_t keySize) noexcept {
    zmsg_addmem(msg, key, keySize);
}

void DeleteRequest::addKey(const char* key) noexcept {
    addKey(key, strlen(key));
}

PutRequest::PutRequest(const std::string& key, const std::string& value, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x20", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    addKeyValue(key, value);
}

PutRequest::PutRequest(const char* key, size_t keyLength, const char* value, size_t valueLength, uint32_t tableNum) noexcept {
    zmsg_addmem(msg, "\x31\x01\x20", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    addKeyValue(key, keyLength, value, valueLength);
}

/**
 * Add a new key to this put request.
 * The key is added to the end of the request.
 */
void PutRequest::addKeyValue(const std::string& key, const std::string& value) noexcept {
    addKeyValue(key.c_str(), key.size(), value.c_str(), value.size());
}

void PutRequest::addKeyValue(const char* key, size_t keySize, const char* value, size_t valueSize) noexcept {
    zmsg_addmem(msg, key, keySize);
    zmsg_addmem(msg, value, valueSize);
}

void PutRequest::addKeyValue(const char* key, const char* value) noexcept {
    addKeyValue(key, strlen(key), value, strlen(value));
}

Status PutRequest::execute(void* socket) noexcept {
    assert(msg);
    assert(socket);
    if (unlikely(zmsg_send(&msg, socket))) {
        return Status();
    }
    //Receive the reply
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg);
    //TODO parse it (if neccessary)
    //Cleanup the message
    zmsg_destroy(&msg);
    return Status();
}

CountRequest::CountRequest(uint32_t tableNum) noexcept : msg(zmsg_new()) {
    zmsg_addmem(msg, "\x31\x01\x11", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
}

CountRequest::~CountRequest() noexcept {
    if (msg) {
        zmsg_destroy(&msg);
    }
}

Status CountRequest::execute(void* socket, uint64_t& count) noexcept {
    assert(msg);
    if (unlikely(zmsg_send(&msg, socket))) {
        debugZMQError("Send count request", errno);
    }
    //Receive the reply
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg);
    //Parse the count
    zframe_t* countFrame = zmsg_next(msg);
    assert(countFrame);
    assert(zframe_size(countFrame) == sizeof(uint64_t));
    count = ((uint64_t*) zframe_data(countFrame))[0];
    //Cleanup the message
    zmsg_destroy(&msg);
    return Status();
}