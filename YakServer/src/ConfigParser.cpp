#include <fstream>
#include <string>
#include <regex>
#include <thread>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <map>
#include <rocksdb/options.h>
#include <iostream>
#include <boost/algorithm/string.hpp>
#include "macros.hpp"
#include "ConfigParser.hpp"
#include "FileUtils.hpp"

using namespace boost;
using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

/**
 * A buffer-overflow-safe readlink() wrapper for C++.
 * @return A string containing the readlink()ed filename, or
 *         an empty string with errno being set to the appropriate error.
 *         See the readlink() man(2) for errno details.
 *
 * See http://techoverflow.net/blog/2013/12/31/buffer-overflow-safe-readlink-in-c-/
 */
static std::string safeReadlink(const std::string& filename) {
    size_t bufferSize = 255;

    //Increase buffer size until the buffer is large enough
    while (1) {
        char* buffer = new char[bufferSize];
        ssize_t rc = readlink (filename.c_str(), buffer, bufferSize);
        if (rc == -1) {
            delete[] buffer;
            if(errno == EINVAL) {
                //We know that bufsize is positive, so
                // the file is not a symlink.
                errno = 0;
                return filename;
            } else if(errno == ENAMETOOLONG) {
                bufferSize += 255;
            } else {
                //errno still contains the error value
                return "";
            }
        } else {
            //Success! rc == number of valid chars in buffer
            errno = 0;
            return string(buffer, rc);
        }
    }
}

static std::map<std::string, std::string> readConfigFile(const char* filename) {
    std::map<std::string, std::string> config;
    std::ifstream fin(filename);
    std::string line;
    std::string currentSection = "";
    while(std::getline(fin, line)) {
        trim(line);
        //Ignore empty lines (and lines with only whitespace)
        if(line.empty()) {
            continue;
        }
        //Ignore comment-only lines
        if(line[0] == '#') {
            continue;
        }
        //Parse section header
        if(line[0] == '[') {
            //There must not be a line like "[foobar"
            if(line[line.size() - 1] != ']') {

            }
            //OK, it's a correct section header
            currentSection = line.substr(1, line.size() - 2); //[foo] -> foo
            continue;
        }
        //It must be a normal config line.
        //Check if it is of the form "key=value"
        //Note that "key=" is explicitly allowed and treated as empty value.
        size_t equalsSignPosition = line.find_first_of('=');
        if(equalsSignPosition == string::npos //No equals sign at all, e.g. "foobar"
            || equalsSignPosition == 0 //No key, e.g. "=foo" or "="
            ) {
            cerr << "\x1B[33m[Warn] Illegal config file line '"
                 << line
                 << "' -- ignoring this line!" << endl;
            continue;
        }
        //If this is reached, we seem to have a sane config line.
        //Parse key and value and add them to the config map.
        //If the KV pair is not inside a section, we'll just use the key.
        std::string key = currentSection.empty()
                            ? line.substr(0, equalsSignPosition)
                            : currentSection + "." + line.substr(0, equalsSignPosition);
        config[key] = line.substr(equalsSignPosition + 1);
    }
    return config;
}

static void printUsageAndExit(char** argv) {
    cerr << "Usage: " << argv[0]
         << " <config file>\nUse default yakdb.cfg if in doubt.\n";
    exit(1);
}

rocksdb::CompressionType compressionModeFromString(const std::string& val) {
    if(val == "NONE") {
        return rocksdb::kNoCompression;
    } else if (val == "SNAPPY") {
        return rocksdb::kSnappyCompression;
    } else if (val == "ZLIB") {
        return rocksdb::kZlibCompression;
    } else if (val == "BZIP2") {
        return rocksdb::kBZip2Compression;
    } else if (val == "LZ4") {
        return rocksdb::kLZ4Compression;
    } else if (val == "LZ4HC") {
        return rocksdb::kLZ4HCCompression;
    }
    //Else: Unknown --> Log and use default
    cerr << "\x1B[33m[Warn] Unknown compression '"
         << val
         << "' -- using default (SNAPPY)!" << endl;
    return rocksdb::kSnappyCompression;
}

std::string compressionModeToString(rocksdb::CompressionType compression) {
    switch(compression) {
        case rocksdb::kNoCompression:
            return "NONE";
        case rocksdb::kSnappyCompression:
            return "SNAPPY";
        case rocksdb::kZlibCompression:
            return "ZLIB";
        case rocksdb::kBZip2Compression:
            return "BZIP2";
        case rocksdb::kLZ4Compression:
            return "LZ4";
        case rocksdb::kLZ4HCCompression:
            return "LZ4HC";
        default: return "UNKNOWN";
    }
}

/*
 * Parse a bool in a case-insensitive manner, also recognizing
 * 1/0 and yes/no for compatibility.
 *
 * Any unclear value is recognized as false and logged.
 */
static bool parseBool(const std::string& value) {
    std::string ciValue = to_lower_copy(value);
    bool isClearlyTrue = (
            ciValue == "true"
             || ciValue == "1"
             || ciValue == "yes"
        );
    bool isClearlyFalse = (
            ciValue == "false"
             || ciValue == "0"
             || ciValue == "no"
        );
    if(!(isClearlyFalse || isClearlyTrue)) {
        cerr << "\x1B[33m[Warn] Can't recognize boolean value '"
             << value
             << "' -- treating as false (please use true/false!)\x1B[0;30m\n" << endl;
        return true;
    }
    return isClearlyTrue;
}

std::string ConfigParser::getTableDirectory(uint32_t tableIndex) const {
    return tableSaveFolder + std::to_string(tableIndex);
}

std::string ConfigParser::getTableConfigFile(uint32_t tableIndex) const {
    return getTableDirectory(tableIndex) + ".cfg";
}

unsigned long long ConfigParser::safeStoull(std::map<std::string, std::string>& cfg, const std::string& cfgKey) {
    const std::string& value = cfg[cfgKey];
    try {
        return std::stoull(value);
    } catch (...) {
        cerr << "\x1B[33m[Warn] Can't parse unsigned integral value '"
             << value << "' for config key '"
             << cfgKey << "'\x1B[0;30m\n" << endl;
        exit(1);
    }
}

int ConfigParser::safeStoi(std::map<std::string, std::string>& cfg, const std::string& cfgKey) {
    const std::string& value = cfg[cfgKey];
    try {
        return std::stoi(value);
    } catch (...) {
        cerr << "\x1B[33m[Warn] Can't parse integral value '"
             << value << "' for config key '" << cfgKey
             << "'\x1B[0;30m\n" << endl;
        exit(1);
    }
}

COLD ConfigParser::ConfigParser(int argc, char** argv) {
    //Handle --help or -h
    if(argc >= 2 &&
        (strcmp(argv[1], "--help") == 0
            || strcmp(argv[1], "-h") == 0)) {
        printUsageAndExit(argv);
    }
    const char* configFile = nullptr;
    //Handle not enough arguments
    if(argc < 2) {
        //try to use global config
        if(fileExists("/etc/yakdb/yakdb.cfg")) {
            configFile = "/etc/yakdb/yakdb.cfg";
            cout << "Using config " << configFile << endl;
        } else if(fileExists("./yakdb.cfg")) {
            configFile = "./yakdb.cfg";
            cout << "Using config " << configFile << endl;
        } else if(fileExists("../YakServer/yakdb.cfg")) {
            //This option is used when running directly from the build directory
            configFile = "../YakServer/yakdb.cfg";
            cout << "Using config " << configFile << endl;
        } else {
            printUsageAndExit(argv);
        }
    } else {
        configFile = argv[1];
    }
    //Parse the config file
    assert(configFile); //If this fails, argv is not parsed correctly
    std::map<std::string, std::string> cfg = readConfigFile(configFile);
    //Log options
    logFile = cfg["Logging.log-file"];
    //Statistics options
    statisticsExpungeTimeout = safeStoull(cfg, "Statistics.expunge-timeout");
    //ZMQ options
    //FIXME Using space with token_compress=on seems a bit hackish. Could it cause errors?
    split(repEndpoints, cfg["ZMQ.rep-endpoints"], is_any_of(", "), token_compress_on);
    split(pullEndpoints, cfg["ZMQ.pull-endpoints"], is_any_of(", "), token_compress_on);
    split(subEndpoints, cfg["ZMQ.sub-endpoints"], is_any_of(", "), token_compress_on);
    zmqIPv4Only = parseBool(cfg["ZMQ.ipv4-only"]);
    externalRCVHWM = safeStoi(cfg, "ZMQ.external-rcv-hwm");
    externalSNDHWM = safeStoi(cfg, "ZMQ.external-snd-hwm");
    internalRCVHWM = safeStoi(cfg, "ZMQ.internal-rcv-hwm");
    internalSNDHWM = safeStoi(cfg, "ZMQ.internal-snd-hwm");
    //Table options
    useMMapReads = parseBool(cfg["RocksDB.use-mmap-reads"]);
    useMMapWrites = parseBool(cfg["RocksDB.use-mmap-writes"]);
    defaultTableBlockSize = safeStoull(cfg, "RocksDB.table-block-size");
    defaultWriteBufferSize = safeStoull(cfg, "RocksDB.write-buffer-size");
    defaultBloomFilterBitsPerKey = safeStoull(cfg, "RocksDB.bloom-filter-bits-per-key");
    defaultCompression = compressionModeFromString(cfg["RocksDB.compression"]);
    defaultMergeOperator = cfg["RocksDB.merge-operator"];
    tableSaveFolder = cfg["RocksDB.table-dir"];
    //RocksDB options
    compactionMemoryBudget = safeStoull(cfg, "RocksDB.compaction-memory-budget");
    putBatchSize = safeStoull(cfg, "RocksDB.put-batch-size");
    if(cfg["RocksDB.concurrency"] == "auto") {
        rocksdbConcurrency = std::thread::hardware_concurrency();
    } else {
        rocksdbConcurrency = safeStoi(cfg, "RocksDB.concurrency");
    }
    if(cfg["RocksDB.compaction-style"] == "level") {
        compactionStyle = CompactionStyle::LevelStyleCompaction;
    } else if(cfg["RocksDB.compaction-style"] == "universal") {
        compactionStyle = CompactionStyle::UniversalStyleCompaction;
        rocksdbConcurrency = safeStoi(cfg, "RocksDB.concurrency");
    } else {
        cerr << "\x1B[33m[Warn] Can't parse compaction style configuration '"
             << cfg["RocksDB.compaction-style"] << "'\x1B[0;30m\n" << endl;
        exit(1);
    }
    //Normalize table save folder to be slash-terminated
    if(tableSaveFolder[tableSaveFolder.size() - 1] != '/') { //if last char is not slash
        tableSaveFolder += "/";
    }
}
