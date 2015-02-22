
#ifndef CONFIGPARSER_HPP
#define CONFIGPARSER_HPP
#include <string>
#include <vector>

enum class CompactionStyle {
    LevelStyleCompaction,
    UniversalStyleCompaction
};

class ConfigParser {
public:
    ConfigParser(int argc, char** argv);

    /**
     * For the given table index, get the directory where the table resides
     */
    std::string getTableDirectory(uint32_t tableIndex) const;

    /**
     * For the given table index, get the table config file
     */
    std::string getTableConfigFile(uint32_t tableIndex) const;

    /**
     * Safer stoull version that logs issues if a value could not be converted.
     */
    unsigned long long safeStoull(std::map<std::string, std::string>& cfg, const std::string& cfgKey);

    /**
     * Safer stoull version that logs issues if a value could not be converted.
     */
    int safeStoi(std::map<std::string, std::string>& cfg, const std::string& cfgKey);
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
    //RocksDB table options
    bool useMMapReads;
    bool useMMapWrites;
    uint64_t defaultLRUCacheSize;
    uint64_t defaultTableBlockSize;
    uint64_t defaultWriteBufferSize;
    uint64_t defaultBloomFilterBitsPerKey;
    rocksdb::CompressionType defaultCompression;
    std::string defaultMergeOperator;
    //Other RocksDB options
    int rocksdbConcurrency;
    uint32_t putBatchSize;
    uint64_t compactionMemoryBudget;
    CompactionStyle compactionStyle;
    //Save folder, normalized to have a terminal slash.
    std::string tableSaveFolder;
};

/**
 * Parse a compression code (see default_config.cfg for details) into a RocksDB code.
 * @param val The code to parse, e.g. "SNAPPY"
 */
rocksdb::CompressionType compressionModeFromString(const std::string& val);
std::string compressionModeToString (rocksdb::CompressionType compression);


#endif //CONFIGPARSER_HPP
