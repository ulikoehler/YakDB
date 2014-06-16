
#ifndef CONFIGPARSER_HPP
#define CONFIGPARSER_HPP
#include <string>
#include <vector>

class ConfigParser {
public:
    ConfigParser(int argc, char** argv);
    const std::vector<std::string>& getREPEndpoints();
    const std::vector<std::string>& getPULLEndpoints();
    const std::vector<std::string>& getSUBEndpoints();
    const std::string& getLogFile();
    const std::string& getTableSaveFolderPath();
    const std::string& getHTTPEndpoint();
    const bool isIPv4Only();
    /**
     * Persists the current configuration to yak.cfg.
     * This is called automatically after configuration is
     * finished.
     * 
     * This allows the user to only specify the options once
     * and then only execute the server from the same directory.
     */
    void saveConfigFile();
    std::string getStaticFilePath();
    uint64_t getStatisticsExpungeTimeout();
    uint64_t getDefaultLRUCacheSize();
    uint64_t getDefaultTableBlockSize();
    uint64_t getDefaultWriteBufferSize();
    uint64_t getDefaultBloomFilterBitsPerKey();
    bool isCompressionEnabledPerDefault();
    int getInternalHWM();
    int getExternalHWM();

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
    bool compressionEnabledPerDefault;
    std::string tableSaveFolder;
};

#endif //CONFIGPARSER_HPP