/* 
 * File:   UpdateRequests.hpp
 * Author: uli
 *
 * Created on 1. August 2013, 19:19
 */

#ifndef UPDATEREQUESTS_HPP
#define	UPDATEREQUESTS_HPP

/**
 * An update request that writes key-value pairs to the database.
 */
class PutRequest {
public:
    static const uint8_t PARTSYNC = 0x01;
    static const uint8_t FULLSYNC = 0x02;
    /**
     * Send the header for the current request type
     * @param socket
     * @param table The table number to write to
     * @return 0 on success, errno else
     */
    static int sendHeader(void* socket, uint32_t table, uint8_t flags = 0x00);
    /**
     * Write a single key-value pair.
     * If this is not the last key-value pair you want to send,
     * you must set the 'last' parameter to false!
     * @param socket
     * @param key The key to writes
     * @param value The value to write
     * @param last Whether this is the last key to send. Determines ZMQ_SNDMORE flag
     * @return 0 on success, errno else
     */
    static int sendKeyValue(void* socket,
            const std::string& key,
            const std::string& value,
            bool last = false);
    static int sendKeyValue(void* socket,
            const char* key,
            const char* value,
            bool last = false);
    static int sendKeyValue(void* socket,
            const char* key,
            size_t keyLength,
            const char* value,
            size_t valueLength,
            bool last = false);
    static int receiveResponse(void* socket, std::string& errorString);
};

/**
 * A delete request that deletes one or more keys.
 */
class DeleteRequest {
public:
    static const uint8_t PARTSYNC = 0x01;
    static const uint8_t FULLSYNC = 0x02;
    /**
     * Send the header for the current request type
     * @param socket
     * @param table The table number to write to
     * @return 0 on success, errno else
     */
    static int sendHeader(void* socket, uint32_t table, uint8_t flags = 0x00);
    /**
     * Write a single key to delete
     * For the last key to send, you need to set the 'last' parameter to true.
     * @param socket
     * @param key The key to writes
     * @param value The value to write
     * @param last Whether this is the last key to be sent.
     * @return 0 on success, errno else
     */
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
    /**
     * 
     * @param socket
     * @return 
     */
    static int receiveResponse(void* socket, std::string& errorMessage);
};

/**
 * A delete request that deletes a range of keys from a start key (inclusive)
 * to an end key (exclusive)
 */
class DeleteRangeRequest {
    static int sendRequest(void* socket, uint32_t tableNum,
        const std::string& startKey,
        const std::string& endKey);
    static int receiveResponse(void* socket, std::string& errorString);
};

#endif	/* UPDATEREQUESTS_HPP */

