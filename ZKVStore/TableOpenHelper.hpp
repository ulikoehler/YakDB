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
     * Simple table open without additional parameters
     * @param index
     */
    void openTable(IndexType index);
    /**
     * Advanced table open with all optional parameters.
     * Table ID is 32-bit little-endian, other parameters 
     * must be 64-bit little-endian
     * @param tableIdFrame: Frame of size 4, must not be empty
     * @param lruCacheSizeFrame optional parameter, empty if default shall be assumed
     * @param tableBlockSizeFrame optional parameter, empty if default shall be assumed
     * @param writeBufferSizeFrame optional parameter, empty if default shall be assumed
     * @param bloomFilterBitsPerKeyFrame optional parameter, empty if default shall be assumed
     */
    void openTable(zframe_t* tableIdFrame, zframe_t* lruCacheSizeFrame, zframe_t* tableBlockSizeFrame, zframe_t* writeBufferSizeFrame, zframe_t* bloomFilterBitsPerKeyFrame);
    void closeTable(IndexType index);
    void truncateTable(IndexType index);
private:
    void* reqSocket; //This ZMQ socket is used to send requests
    zctx_t* context;
    Logger logger;
};

#endif	/* TABLEOPENHELPER_HPP */

