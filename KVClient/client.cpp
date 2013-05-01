#include "client.hpp"
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
        return Status(std::string("Communication error: Failed to receive reply: ") + std::string(zmq_strerror(zmq_errno())), -1);\
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

/**
 * Code-deduplication macro for functions that return a Status.
 * This macro returns an error-describing status if the message could not be sent.
 * 
 * This macro does not require a semicolon at the end!
 */
#define CHECKED_SEND(msg, socket) \
    if (unlikely(zmsg_send(&(msg), socket))) {\
        return Status(std::string("Communication error: Failed to send message: ") + std::string(zmq_strerror(zmq_errno())), -1);\
    }

/**
 * Code-deduplication macro for functions that return a Status.
 * 
 * Sends a single frame with user-specified flags.
 * Available flags (bitwise-OR them): ZFRAME_MORE, ZFRAME_REUSE, ZFRAME_DONTWAIT
 */
#define CHECKED_SEND_FRAME(frame, socket, flags) \
    if (unlikely(zframe_send(&(frame), (socket), (flags)))) {\
        return Status(std::string("Communication error: Failed to send frame: ") + std::string(zmq_strerror(zmq_errno())), -1);\
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

void ReadRequest::init(uint32_t tableNum)noexcept {
    zmsg_addmem(msg, "\x31\x01\x10", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
}

ReadRequest::ReadRequest(uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
}

ReadRequest::ReadRequest(const char* key, size_t keySize, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
    addKey(key, keySize);
}

ReadRequest::ReadRequest(const char* key, uint32_t tablenum) noexcept : msg(zmsg_new()) {
    init(tablenum);
    addKey(key, strlen(key));
}

ReadRequest::ReadRequest(const std::string& value, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
    addKey(value.c_str(), value.size());
}

ReadRequest::ReadRequest(const std::vector<std::string>& keys, uint32_t tableNum) noexcept {
    init(tableNum);
    for (const std::string& key : keys) {
        addKey(key.c_str(), key.size());
    }
}

Status ReadRequest::executeSingle(void* socket, std::string& value) noexcept {
    assert(msg);
    //Send the read request
    CHECKED_SEND(msg, socket)
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

Status ReadRequest::executeMultiple(void* socket, std::vector<std::string>& values) noexcept {
    //Send the read request
    CHECKED_SEND(msg, socket)
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
    CHECKED_SEND(msg, socket)
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
    CHECKED_SEND(msg, socket)
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

CountRequest::CountRequest(uint32_t tableNum) noexcept : haveStartKey(false), haveEndKey(false), startKey(), endKey(), tableNum(tableNum) {

}

CountRequest::~CountRequest() noexcept {
}

Status CountRequest::execute(void* socket, uint64_t& count) noexcept {
    zmsg_t* msg = zmsg_new();
    zmsg_addmem(msg, "\x31\x01\x11", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
    //Add the start/end key (we always add a frame, even if the could be left out)
    if (haveStartKey) {
        zmsg_addmem(msg, startKey.c_str(), startKey.length());
    } else {
        zmsg_addmem(msg, "", 0);
    }
    if (haveEndKey) {
        zmsg_addmem(msg, endKey.c_str(), endKey.length());
    } else {
        zmsg_addmem(msg, "", 0);
    }
    assert(zmsg_size(msg) == 4);
    //Send the message
    CHECKED_SEND(msg, socket)
    //Receive the reply
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg);
    //Parse the count
    zframe_t* countFrame = zmsg_next(msg);
    assert(countFrame);
    assert(zframe_size(countFrame) == sizeof (uint64_t));
    count = ((uint64_t*) zframe_data(countFrame))[0];
    //Cleanup the message
    zmsg_destroy(&msg);
    return Status();
}

void CountRequest::setStartKey(const std::string& startKeyArg) {
    this->haveStartKey = true;
    this->startKey = startKey;
}

void CountRequest::setEndKey(const std::string& endKey) {
    this->haveEndKey = true;
    this->endKey = startKey;
}

void ExistsRequest::init(uint32_t tableNum)noexcept {
    zmsg_addmem(msg, "\x31\x01\x12", 3);
    zmsg_addmem(msg, &tableNum, sizeof (uint32_t));
}

ExistsRequest::ExistsRequest(uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
}

ExistsRequest::ExistsRequest(const char* key, size_t keySize, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
    addKey(key, keySize);
}

ExistsRequest::ExistsRequest(const char* key, uint32_t tablenum) noexcept : msg(zmsg_new()) {
    init(tablenum);
    addKey(key, strlen(key));
}

ExistsRequest::ExistsRequest(const std::string& value, uint32_t tableNum) noexcept : msg(zmsg_new()) {
    init(tableNum);
    addKey(value.c_str(), value.size());
}

ExistsRequest::ExistsRequest(const std::vector<std::string>& keys, uint32_t tablenum) noexcept {
    init(tablenum);
    for (const std::string& key : keys) {
        addKey(key.c_str(), key.size());
    }
}

Status ExistsRequest::executeSingle(void* socket, bool& value) noexcept {
    assert(msg);
    //Send the read request
    CHECKED_SEND(msg, socket)
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg)
    //In contrast to executeMultiple, we only read one value.
    zframe_t* valueFrame = zmsg_next(msg);
    assert(valueFrame);
    assert(zframe_size(valueFrame) == 1);
    value = (zframe_data(valueFrame)[0] > 0);
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

Status ExistsRequest::executeMultiple(void* socket, std::vector<bool>& values) noexcept {
    //Send the read request
    CHECKED_SEND(msg, socket)
    //Receive the reply (blocks until finished)
    msg = zmsg_recv(socket);
    assert(msg);
    //Check for protocol correctness and non-error return code
    CHECK_HEADERFRAME(zmsg_first(msg), msg)
    //Iterate over the value frames
    zframe_t * valueFrame = nullptr;
    while ((valueFrame = zmsg_next(msg)) != nullptr) {
        assert(zframe_size(valueFrame) == 1);
        //== 0 --> does not exist
        values.push_back(zframe_data(valueFrame)[0] > 0);
    }
    //Cleanup
    zmsg_destroy(&msg);
    return Status();
}

void ExistsRequest::addKey(const std::string& key) noexcept {
    addKey(key.c_str(), key.size());
}

void ExistsRequest::addKey(const char* key, size_t keySize) noexcept {
    zmsg_addmem(msg, key, keySize);
}

void ExistsRequest::addKey(const char* key) noexcept {
    addKey(key, strlen(key));
}
