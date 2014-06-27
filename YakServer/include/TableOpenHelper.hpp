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
#include <zmq.h>
#include <thread>
#include <rocksdb/db.h>
#include <vector>
#include "Logger.hpp"
#include "ConfigParser.hpp"
#include "AbstractFrameProcessor.hpp"

#ifndef UINT64_MAX
#define UINT64_MAX (std::numeric_limits<uint64_t>::max())
#endif

/**
 * This class starts a single thread in the background that shall receive an
 * inproc message.
 * 
 * This class shall be instantiated exactly once or the behaviour will be undefined.
 */
class TableOpenServer : private AbstractFrameProcessor {
public:
    TableOpenServer(void* ctx,
                    ConfigParser& configParser, 
                    std::vector<rocksdb::DB*>& databases);
    ~TableOpenServer();
    /**
     * Terminate the table open server.
     * Includes a full cleanup.
     */
    void terminate();
    void tableOpenWorkerThread();
private:
    std::thread* workerThread;
    ConfigParser& configParser;
    std::vector<rocksdb::DB*>& databases;
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
    TableOpenHelper(void* context, ConfigParser& cfg);
    ~TableOpenHelper();
    /**
     * Open a table using a socket from which parameters are read
     */
    void openTable(IndexType tableId, void* srcSock=nullptr);
    void closeTable(IndexType index);
    void truncateTable(IndexType index);
    void* reqSocket; //This ZMQ socket is used to send requests
private:
    void* context;
    ConfigParser& cfg;
    Logger logger;
};

#endif	/* TABLEOPENHELPER_HPP */

