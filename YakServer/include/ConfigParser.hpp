
#ifndef CONFIGPARSER_HPP
#define CONFIGPARSER_HPP
#include <string>
#include <vector>

class ConfigParser {
public:
    ConfigParser(int argc, char** argv);

    /*
     * Config options accessible as fields.
     * To simplify the code and to avoid possible performance issues,
     * we avoid using getters for any single property.
     *
     * However this means one needs to take special care not to modify
     * those collections unless desired.
     */
    //Log options
    std::string logFile;
    //Statistics options
    uint64_t statisticsExpungeTimeout;
    //ZMQ options
    std::vector<std::string> repEndpoints;
    std::vector<std::string> pullEndpoints;
    std::vector<std::string> subEndpoints;
    bool zmqIPv4Only;
    int externalRCVHWM;
    int externalSNDHWM;
    int internalRCVHWM;
    int internalSNDHWM;
    //HTTP options
    std::string httpEndpoint;
    std::string staticFilePath;
    bool httpIPv4Only;
    //Table options
    uint64_t defaultLRUCacheSize;
    uint64_t defaultTableBlockSize;
    uint64_t defaultWriteBufferSize;
    uint64_t defaultBloomFilterBitsPerKey;
    rocksdb::CompressionType defaultCompression;
    std::string defaultMergeOperator;
    //Save folder, normalized to have a slash at the end.
    std::string tableSaveFolder;
};

/**
 * Parse a compression code (see default_config.cfg for details) into a RocksDB code.
 * @param val The code to parse, e.g. "SNAPPY"
 */
rocksdb::CompressionType compressionModeFromString(const std::string& val);
std::string compressionModeToString (rocksdb::CompressionType compression);


#endif //CONFIGPARSER_HPP
