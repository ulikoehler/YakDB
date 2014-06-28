/*
 * File:   TableOpenHelper.hpp
 * Author: uli
 *
 * Created on 6. April 2013, 18:15
 */

#ifndef TABLEOPENHELPER_HPP
#define	TABLEOPENHELPER_HPP
#include <cstdint>
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

struct TableOpenParameters  {
    uint64_t lruCacheSize;
    uint64_t tableBlockSize;
    uint64_t writeBufferSize;
    uint64_t bloomFilterBitsPerKey;
    rocksdb::CompressionType compression;
    std::string mergeOperatorCode;

    /**
     * Construct a new instance with all parameters set from config
     */
    TableOpenParameters(const ConfigParser& cfg);

    /**
     * Evaluate the given parameter map and replace any value in the current instance
     * with the appropiate entry in the map (if any)
     */
    void parseFromParameterMap(std::map<std::string, std::string>& parameters);

    /**
     * Convert this instance to a parameter map so that when using
     * parseFromParameterMap() on said map, an equivalent instance is yielded
     */
    void toParameterMap(std::map<std::string, std::string>& parameters);

    /**
     * Convert the current instance to a rocksdb optionset
     * @param options In this reference the values are stored.
     */
    void getOptions(rocksdb::Options& options);

    /**
     * Read a table config file (request is ignored if file does not exist).
     * Such a config file can be written by writeToFile()
     */
    void readTableConfigFile(const ConfigParser& cfg, uint32_t tableIndex);

    /**
     * Write the currently set values to a config file.
     * The file written by this can be read using readTableConfigFile()
     */
    void writeToFile(const ConfigParser& cfg, uint32_t tableIndex);
};


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
