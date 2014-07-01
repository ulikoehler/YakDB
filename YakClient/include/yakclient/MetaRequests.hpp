/*
 * File:   MetaRequests.hpp
 * Author: uli
 *
 * Created on 1. August 2013, 19:19
 */

#ifndef METAREQUESTS_HPP
#define	METAREQUESTS_HPP
#include <cstdint>
#include <limits>
#include <string>
#include <map>

/**
 * An update request that writes
 */
class ServerInfoRequest {
public:
    /**
     * Send a server info request
     */
    static int sendRequest(void* socket);
    /**
     * Receive the first server info response frame from the
     * @param socket
     * @param flags
     * @return
     */
    static int receiveFeatureFlags(void* socket, uint64_t& flags);
    static int receiveVersion(void* socket, std::string& serverVersion);
};

/**
 * Table open request.
 * Tables are opened on-the-fly, but if you intend to pass special parameters,
 * you need to use this request
 */
class TableOpenRequest {
public:
    /**
     *
     */
    static int sendRequest(void* socket, uint32_t tableNo,
            uint64_t lruCacheSize = std::numeric_limits<uint64_t>::max(),
            uint64_t tableBlockSize = std::numeric_limits<uint64_t>::max(),
            uint64_t writeBufferSize = std::numeric_limits<uint64_t>::max(),
            uint64_t bloomFilterSize = std::numeric_limits<uint64_t>::max(),
            bool enableCompression = true);
    static int receiveResponse(void* socket, std::string& errorString);
};

/**
 * Table close request.
 * Usually tables should not be closed,
 * but this allows you to save memory and/or re-open the table with
 * different flags.
 */
class TableCloseRequest {
public:
    static int sendRequest(void* socket, uint32_t tableNum);
    static int receiveResponse(void* socket, std::string& errorString);
};

/**
 * A compact request that compacts a range in a table.
 * This request is extremely expensive, especially for large tables.
 */
class CompactRequest {
    /**
     * Send a compact request to compact a specific range of keys
     * @param startKey The start key, or the empty string to start at the beginning of the table
     * @param endKey The end key, or the empty string to stop at the end of the table
     */
    static int sendRequest(void* socket, uint32_t tableNum,
            const std::string& startKey,
            const std::string& endKey);
    static int receiveResponse(void* socket, std::string& errorString);
};

/**
 * Truncate request that deletes any
 *
 * This request may
 */
class TruncateRequest {
    /**
     * Truncate a table
     * @param tableNum The ID of the table to truncate
     */
    static int sendRequest(void* socket, uint32_t tableNum);
    static int receiveResponse(void* socket, std::string& errorString);
};


/**
 * Table info request
 */
class TableInfoRequest {
public:
    /**
     * Send a table info request for the given table number
     */
    static int sendRequest(void* socket, uint32_t tableNo);
    /**
     * Receive a table info response
     * @param params In this map the received values will be placed
     */
    static int receiveResponse(void* socket, std::string& errorString,
                               std::map<std::string, std::string>& params);
};

#endif	/* METAREQUESTS_HPP */
