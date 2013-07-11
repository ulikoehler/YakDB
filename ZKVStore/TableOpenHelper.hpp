/* 
 * File:   TableOpenHelper.hpp
 * Author: uli
 *
 * Created on 6. April 2013, 18:15
 */

#ifndef TABLEOPENHELPER_HPP
#define	TABLEOPENHELPER_HPP
#include <czmq.h>
#include <thread>
#include <leveldb/db.h>
#include <vector>
#include <climits>
#include "Logger.hpp"

/**
 * This class starts a single thread in the background that shall receive an
 * inproc message.
 * 
 * This class shall be instantiated exactly once or the behaviour will be undefined.
 */
class TableOpenServer {
public:
    TableOpenServer(zctx_t* ctx, std::vector<leveldb::DB*>& databases, bool dbCompressionEnabled = true);
    ~TableOpenServer();
private:
    zctx_t* context;
    std::thread* workerThread;
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
    TableOpenHelper(zctx_t* context);
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
    zctx_t* context;
    Logger logger;
};

#endif	/* TABLEOPENHELPER_HPP */

