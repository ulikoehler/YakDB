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

enum class TableOperationRequestType : uint8_t {
    StopServer = 0,
    OpenTable = 1,
    CloseTable = 2,
    TruncateTable = 3
};


/**
 * Send a table operation request
 */
int sendTableOperationRequest(void* socket, TableOperationRequestType requestType, int flags = 0);

class Tablespace;

struct TableOpenParameters  {
    uint64_t lruCacheSize;
    uint64_t tableBlockSize;
    uint64_t writeBufferSize;
    uint64_t bloomFilterBitsPerKey;
    rocksdb::CompressionType compression;
    std::string mergeOperatorCode;

    /**
     * Return code of the getOptions() function
     */
    enum class GetOptionsResult : uint8_t {
        Success = 0,
        MergeOperatorCodeIllegal = 1
    };

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
     * @return One of the status codes defined in GetOptionsResult
     */
    GetOptionsResult getOptions(rocksdb::Options& options);

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
     * @return String with 1st byte: return code, remaining bytes: error message
     */
    std::string openTable(IndexType tableId, void* srcSock=nullptr);
    void closeTable(IndexType index);
    void truncateTable(IndexType index);
    void* reqSocket; //This ZMQ socket is used to send requests
private:
    void* context;
    ConfigParser& cfg;
    Logger logger;
};

#endif	/* TABLEOPENHELPER_HPP */
