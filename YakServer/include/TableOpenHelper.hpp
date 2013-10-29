/* 
 * File:   TableOpenHelper.hpp
 * Author: uli
 *
 * Created on 6. April 2013, 18:15
 */

#ifndef TABLEOPENHELPER_HPP
#define	TABLEOPENHELPER_HPP
#include <cstdint>
#include <limits>
#include <map>
#include <czmq.h>
#include <thread>
#include <leveldb/db.h>
#include <vector>
#include "Logger.hpp"
#include "ConfigParser.hpp"

#ifndef UINT64_MAX
#define UINT64_MAX (std::numeric_limits<uint64_t>::max())
#endif

/**
 * This class starts a single thread in the background that shall receive an
 * inproc message.
 * 
 * This class shall be instantiated exactly once or the behaviour will be undefined.
 */
class TableOpenServer {
public:
    TableOpenServer(void* ctx,
                    ConfigParser& configParser, 
                    std::vector<leveldb::DB*>& databases);
    ~TableOpenServer();
    /**
     * Terminate the table open server.
     * Includes a full cleanup.
     */
    void terminate();
    void tableOpenWorkerThread();
private:
    void* context;
    std::thread* workerThread;
    Logger logger;
    ConfigParser& configParser;
    void* repSocket;
    std::vector<leveldb::DB*>& databases;
    /**
     * We need to save a list of LRU caches to delete.
     */
    std::map<leveldb::DB*, leveldb::Cache*> cacheMap;
};

/**
 * This class provides lock-free concurrent table opener by using
 * a ZMQ inproc transport
 * 
 * This class provides the client. The server must be started before the client.
 * 
 * Msg format: A single sizeof(IndexType)-sized frame containing the index
 * 
 * @param context
 */
class TableOpenHelper {
public:
    typedef uint32_t IndexType;
    //Constructor
    TableOpenHelper(void* context);
    ~TableOpenHelper();
    /**
     * Any parameter that is set to the numeric_limits<T>::max() limit,
     * is assumed default.
     * @param tableIdFrame: Frame of size 4, must not be empty
     * @param lruCacheSizeFrame LevelDB Block LRU cache size in bytes
     * @param tableBlockSizeFrame LevelDB bloc size in bytes
     * @param writeBufferSizeFrame LevelDB write buffer in bytes
     * @param bloomFilterBitsPerKeyFrame LevelDB bloom filter bits per key
     */
    void openTable(IndexType tableId,
            uint64_t lruCacheSize = UINT64_MAX,
            uint64_t tableBlockSizeFrame = UINT64_MAX,
            uint64_t writeBufferSize = UINT64_MAX,
            uint64_t bloomFilterBitsPerKey = UINT64_MAX,
            bool compressionEnabled = true);
    void closeTable(IndexType index);
    void truncateTable(IndexType index);
    void* reqSocket; //This ZMQ socket is used to send requests
private:
    void* context;
    Logger logger;
};

#endif	/* TABLEOPENHELPER_HPP */

