#include <zmq.h>
#include <string>
#include <cstdint>
#include "ReadRequests.hpp"
#include "zeromq_utils.hpp"

int ReadRequest::sendHeader(void* socket, uint32_t table) {
    if(sendConstFrame(socket, "\x31\x01\x10", 3, ZMQ_SNDMORE) == -1) {
        return -1;
    }
    return sendUint32Frame(socket, table, ZMQ_SNDMORE);
}
int ReadRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {
    return sendStringFrame(socket, key, (last ? 0 : ZMQ_SNDMORE));
}

int ReadRequest::sendKey(void* socket,
        const char* key,
        bool last) {
    return sendCStringFrame(socket, key, (last ? 0 : ZMQ_SNDMORE));
}

int ReadRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {
    return sendBinaryFrame(socket, key, keyLength, (last ? 0 : ZMQ_SNDMORE));

}

int ReadRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {
    
}

int ReadRequest::receiveResponseValue(void* socket, std::string& target) {

}

int CountRequest::sendHeader(void* socket, uint32_t table) {

}

int CountRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {

}

int CountRequest::sendKey(void* socket,
        const char* key,
        bool last) {

}

int CountRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {

}

int CountRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {

}

int CountRequest::receiveResponseValue(void* socket, std::string& target) {

}

int ExistsRequest::sendHeader(void* socket, uint32_t table) {

}

int ExistsRequest::sendKey(void* socket,
        const std::string& key,
        bool last) {

}

int ExistsRequest::sendKey(void* socket,
        const char* key,
        bool last) {

}

int ExistsRequest::sendKey(void* socket,
        const char* key,
        size_t keyLength,
        bool last) {

}

int ExistsRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {

}

int ExistsRequest::receiveResponseValue(void* socket, std::string& target) {

}

int ScanRequest::sendRequest(void* socket, uint32_t tableNum,
        const std::string& startKey,
        const std::string& endKey) {

}

int ScanRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {

}

/**
 * Receive the next response value.
 * @param keyTarget A string reference to write the key to
 * @param valueTarget A string reference to write the value to
 * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
 */
int ScanRequest::receiveResponseValue(void* socket, std::string& keyTarget, std::string& valueTarget) {

}

int LimitedScanRequest::sendRequest(void* socket, uint32_t tableNum,
        const std::string& startKey,
        uint64_t numKeys) {
    
}

int LimitedScanRequest::receiveResponseHeader(void* socket, std::string& errorMessage) {

}

/**
 * Receive the next response key&value.
 * @param keyTarget A string reference to write the key to
 * @param valueTarget A string reference to write the value to
 * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
 */
int LimitedScanRequest::receiveResponseValue(void* socket, std::string& keyTarget, std::string& valueTarget) {

}