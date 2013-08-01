/* 
 * File:   MetaRequests.hpp
 * Author: uli
 *
 * Created on 1. August 2013, 19:19
 */

#ifndef METAREQUESTS_HPP
#define	METAREQUESTS_HPP

/**
 * An update request that writes 
 */
class ServerInfoRequest {
public:
    /**
     * Send a server info request
     */
    static void sendRequest(void* socket);
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
    static void sendRequest(void* socket, uint32_t tableNo,
            uint64_t lruCacheSize = UINT64_MAX,
            uint64_t tableBlockSize = UINT64_MAX,
            uint64_t writeBufferSize = UINT64_MAX,
            uint64_t bloomFilterSize = UINT64_MAX,
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

#endif	/* METAREQUESTS_HPP */

