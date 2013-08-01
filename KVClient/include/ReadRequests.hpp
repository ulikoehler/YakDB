/* 
 * File:   ReadRequests.hpp
 * Author: uli
 *
 * Created on 1. August 2013, 19:19
 */

#ifndef READREQUESTS_HPP
#define	READREQUESTS_HPP

/**
 * A request to read one or multiple keys.
 * In order to correctly write the request, write the header first,
 * then send an arbitrary amount of keys. Ensure that the last key
 * is sent with the last argument set to true.
 * 
 * Then, receive the header and, if no error occured, receive the values
 * (in the same order as the keys), until the return code of
 * receiveResponseValue() is 1.
 */
class ReadRequest {
    static int sendHeader(void* socket, uint32_t table);
    static int sendKey(void* socket,
            const std::string& key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            size_t keyLength,
            bool last = false);
    static int receiveResponseHeader(void* socket, std::string& errorMessage);
    /**
     * Receive the next response value.
     * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
     */
    static int receiveResponseValue(void* socket, std::string& target);
};

/**
 * A request to count a range of keys
 */
class CountRequest {
    static int sendHeader(void* socket, uint32_t table);
    static int sendKey(void* socket,
            const std::string& key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            size_t keyLength,
            bool last = false);
    static int receiveResponseHeader(void* socket, std::string& errorMessage);
    /**
     * Receive the next response value.
     * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
     */
    static int receiveResponseValue(void* socket, std::string& target);
};

/**
 * A request to check if one or multiple keys exist
 */
class ExistsRequest {
    static int sendHeader(void* socket, uint32_t table);
    static int sendKey(void* socket,
            const std::string& key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            bool last = false);
    static int sendKey(void* socket,
            const char* key,
            size_t keyLength,
            bool last = false);
    static int receiveResponseHeader(void* socket, std::string& errorMessage);
    /**
     * Receive the next response value.
     * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
     */
    static int receiveResponseValue(void* socket, std::string& target);
};

/**
 * A request to scan a range of keys and return all key-value-pairs
 * in the given request at once.
 */
class ScanRequest {
    static int sendRequest(void* socket, uint32_t tableNum,
            const std::string& startKey,
            const std::string& endKey);
    static int receiveResponseHeader(void* socket, std::string& errorMessage);
    /**
     * Receive the next response key&value.
     * @param keyTarget A string reference to write the key to
     * @param valueTarget A string reference to write the value to
     * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
     */
    static int receiveResponseValue(void* socket, std::string& keyTarget, std::string& valueTarget);
};

/**
 * A request to scan up to a limited amount of key/value pairs from a start key
 */
class LimitedScanRequest {
    static int sendRequest(void* socket, uint32_t tableNum,
            const std::string& startKey,
            uint64_t numKeys);
    static int receiveResponseHeader(void* socket, std::string& errorMessage);
    /**
     * Receive the next response key&value.
     * @param keyTarget A string reference to write the key to
     * @param valueTarget A string reference to write the value to
     * @return -1 on error, 0 == (success, there are more keys to retrieve), 1 == (success, no more keys to retrieve)
     */
    static int receiveResponseValue(void* socket, std::string& keyTarget, std::string& valueTarget);
};

#endif	/* READREQUESTS_HPP */

