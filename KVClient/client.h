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

/**
 * A status representation that stores information about if
 * an operation has been executed successfully, and, if not,
 * an error message
 */
class Status {
public:
    /**
     * Construct a status that indicates success
     */
    Status();
    /**
     * Construct a status that indicates an error, defined by the given error string
     * @return 
     */
    Status(const std::string& string, int errorCode = 1);
    /**
     * Construct a status from another status object, C++11 Zero-Copy version
     * @param other
     */
    Status(Status&& other);
    /**
     * @return true if and only if this status indicates success,
     */
    bool ok() const;
    ~Status();
    /**
     * @return The error message. Empty if it doesn't exist
     */
    std::string getErrorMessage() const;
private:
    std::string* errorMessage; //Set to an error message, or NULL if no error occured
    int errorCode;
};

/**
 * If the supplied status argument indicates an error, print it to stderr.
 * 
 * This function is intended to be able to write convenient one-liners that execute a request and check for errors
 * 
 * Log message: [Error] occurred during {action}: {errorMsg}
 * Log message (for empty action string): [Error] {errorMsg}
 * 
 * @param action (optional) Describe the action that yielded the status. The string is included in the log message if non-empty.
 * @return true if and only if the given status object indicated success
 */
bool printErr(const Status& status, const char* action = "");

class ReadRequest {
public:
    ReadRequest(uint32_t tablenum) noexcept;
    /**
     * Create a new single-key read request from a std::string
     * @param key The key to be read
     * @param tablenum The table number to be read
     */
    ReadRequest(const std::string& key, uint32_t tablenum) noexcept;
    /**
     * Create a new single-key read request from an arbitrary byte string
     * @param key The key to be read (NUL-terminated cstring)
     * @param tablenum The table number to be read
     */
    ReadRequest(const char* key, size_t keySize, uint32_t tablenum) noexcept;
    /**
     * Create a new single-key read request from an arbitrary byte string
     * @param key The key to be read
     * @param size The size of the key to be read
     * @param tablenum The table number to be read
     */
    ReadRequest(const char* key, uint32_t tablenum) noexcept;
    ReadRequest(const std::vector<std::string> key, uint32_t tablenum) noexcept;
    /**
     * Execute a read request that only reads a single values.
     * Note that for read requests reading more than one value, everything but
     * the first value is discarded.
     * @param socket The socket to send the request over
     * @param value A reference to the string to store the referenced value in.
     */
    Status executeSingle(void* socket, std::string& value) noexcept;
    /**
     * Execute a read request that yields multiple values.
     * The values are placed in the 'values' vector in the same order as the read
     * requests
     * @param socket
     * @param values
     */
    Status executeMultiple(void* socket, std::vector<std::string> values) noexcept;
    /**
     * Add a new key to this read request.
     * The key is added to the end of the request.
     */
    void addKey(const std::string& key) noexcept;
    void addKey(const char* key, size_t keySize) noexcept;
    void addKey(const char* key) noexcept;
private:
    void init(const char* key, size_t size, uint32_t tableNum)noexcept;
    zmsg_t* msg;
};

class DeleteRequest {
public:
    /**
     * Create a new single-key delete request from a std::string
     * @param key The key to be read
     * @param tablenum The table number to be read
     */
    DeleteRequest(const std::string& key, uint32_t tablenum) noexcept;
    /**
     * Create a new single-key delete request from a cstring
     * @param key The key to be read (NUL-terminated cstring)
     * @param tablenum The table number to be read
     */
    DeleteRequest(const char* key, size_t keySize, uint32_t tablenum) noexcept;
    /**
     * Create a new single-key delete request from an arbitrary byte string
     * @param key The key to be read
     * @param size The size of the key to be read
     * @param tablenum The table number to be read
     */
    DeleteRequest(const char* key, uint32_t tablenum) noexcept;
    DeleteRequest(const std::vector<std::string> key, uint32_t tablenum) noexcept;
    /**
     * Execute a delete request.
     * @param socket The socket to send the request over
     */
    Status execute(void* socket) noexcept;
    /**
     * Add a new key to this read request.
     * The key is added to the end of the request.
     */
    void addKey(const std::string& key) noexcept;
    void addKey(const char* key, size_t keySize) noexcept;
    void addKey(const char* key) noexcept;
private:
    void init(const char* key, size_t size, uint32_t tableNum) noexcept;
    zmsg_t* msg;
};

class PutRequest {
public:
    PutRequest(const std::string& key, const std::string& value, uint32_t tableNum) noexcept;
    PutRequest(const char* key, size_t keyLength, const char* value, size_t valueLength, uint32_t tableNum) noexcept;

    /**
     * Add a new key to this put request.
     * The key is added to the end of the request.
     */
    void addKeyValue(const std::string& key, const std::string& value) noexcept;
    void addKeyValue(const char* key, size_t keySize, const char* value, size_t valueSize) noexcept;
    void addKeyValue(const char* key, const char* value) noexcept;
    /**
     * Execute a Put request
     * @param socket
     * @return A status indicating success or error
     */
    Status execute(void* socket) noexcept;
private:
    zmsg_t* msg;
};

//
//Other/unsorted
//

class CountRequest {
public:
    CountRequest(uint32_t tableNum) noexcept;
    ~CountRequest() noexcept;
    /**
     * Execute a count request.
     * 
     * @param socket
     * @param count A pointer to a memory location where the resulting count shall be saved if no error occured
     * @return A status object that indicates if an error occured
     */
    Status execute(void* socket, uint64_t& count) noexcept;
private:
    zmsg_t* msg;
};

#endif	/* CLIENT_H */

