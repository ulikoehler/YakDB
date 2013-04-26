/* 
 * File:   client.h
 * Author: uli
 * 
 * Provides DKV client functionality
 *
 * Created on 17. April 2013, 19:08
 */

#ifndef CLIENT_H
#define	CLIENT_H
#include <czmq.h>
#include <vector>
#include <string>

class ReadRequest {
public:
    /**
     * Create a new single-key read request from a std::string
     * @param key The key to be read
     * @param tablenum The table number to be read
     */
    ReadRequest(const std::string& key, uint32_t tablenum = 0);
    /**
     * Create a new single-key read request from a cstring
     * @param key The key to be read (NUL-terminated cstring)
     * @param tablenum The table number to be read
     */
    ReadRequest(const char* key, size_t keySize, uint32_t tablenum = 0);
    /**
     * Create a new single-key read request from an arbitrary byte string
     * @param key The key to be read
     * @param size The size of the key to be read
     * @param tablenum The table number to be read
     */
    ReadRequest(const char* key, uint32_t tablenum = 0);
    ReadRequest(const std::vector<std::string> key, uint32_t tablenum = 0);
    /**
     * Execute a read request that only reads a single values.
     * Note that for read requests reading more than one value, everything but
     * the first value is discarded.
     * @param socket The socket to send the request over
     * @param value A reference to the string to store the referenced value in.
     */
    void executeSingle(void* socket, std::string& value);
    void executeMultiple(void* socket, std::vector<std::string> values);
private:
    zmsg_t* msg;
};

class PutRequest {
public:
    PutRequest(const std::string& key, const std::string& value) noexcept;
    PutRequest(const char* key, size_t keyLength, const char* value, size_t valueLength) noexcept;
private:
    zmsg_t* msg;
};

//Functions for arbitrary data
zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key, size_t keyLength);
zmsg_t* buildSinglePutRequest(uint32_t tableNum, );
//Functions that work on cstrings (just wrappers using strlen)
zmsg_t* buildSingleReadRequest(uint32_t tableNum, const char* key);
zmsg_t* buildSinglePutRequest(uint32_t tableNum, const char* key, const char* value);
//Incremental functions
void addKeyValueToPutRequest(zmsg_t* msg, const char* key, size_t keyLength, const char* value, size_t valueLength);
void addKeyValueToPutRequest(zmsg_t* msg, const char* key, const char* value);
void addKeyValueToReadRequest(zmsg_t* msg, const char* key, size_t keyLength);
void addKeyValueToReadRequest(zmsg_t* msg, const char* key);
//
//Other/unsorted
//

class CountRequest {
public:
    CountRequest(uint32_t tableNum = 0);
    uint64_t execute(void* socket);
    ~CountRequest();
private:
    zmsg_t* msg;
};

/**
 * Extract read results into a vector
 * @param readRequest
 * @param 
 */
void parseReadRequestResult(zmsg_t* readRequest, std::vector<std::string>& dataRef);

#endif	/* CLIENT_H */

