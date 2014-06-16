#include <fstream>
#include <string>
#include <regex>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <map>
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
        size_t rc = readlink (filename.c_str(), buffer, bufferSize);
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
    std::string currentSection = "General";
    while(std::getline(is, str)) {
        trim(line);
        //Ignore empty lines (and lines with only whitespace)
        if(line.size() == 0) {
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
        std::string key = currentSection + "." + line.substr(0, equalsSignPosition);
        config[key] = line.substr(equalsSignPosition + 1);
    }
    return config;
}

static void printUsageAndExit() {
    cerr << "Usage: " << argv[0]
         << " <config file>\nUse default_config.cfg if in doubt.\n"
    exit(1);
}

/*
 * Parse a bool in a case-insensitive manner, also recognizing
 * 1/0 and yes/no for compatibility.
 *
 * Any unclear value is recognized as false and logged.
 */
bool parseBool(const std::string& value) {
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
        cerr << "\x1B[33m[Warn] Can't recognize boolean in config line '"
             << line
             << "' -- treating as false (please use true/false!)" << endl;
    }
}

COLD ConfigParser::ConfigParser(int argc, char** argv) {
    //Handle not enough arguments
    if(argc < 2) {
        printUsageAndExit();
    }
    //Handle --help or -h
    if(strcmp(argv[1], "--help") == 0
        || strcmp(argv[1], "-h") == 0) {
        printUsageAndExit();
    }
    //Parse the config file
    std::map<std::string, std::string> cfg = readConfigFile(argv[1]);
    /*
     * 
     */
    //Log options
    logFile = cfg["Logging.log-file"];
    //Statistics options
    statisticsExpungeTimeout = cfg["Statistics.statistics-expunge-timeout"];
    //ZMQ options
    //TODO Using space with token_compress=on seems a bit hackish. Could it cause errors?
    split(repEndpoints, cfg["ZMQ.rep-endpoints"], is_any_of(", "), token_compress_on);
    split(pullEndpoints, cfg["ZMQ.pull-endpoints"], is_any_of(", "), token_compress_on);
    split(subEndpoints, cfg["ZMQ.sub-endpoints"], is_any_of(", "), token_compress_on);
    bool zmqIPv4Only = parseBool(cfg["ZMQ.ipv4-only"]);
    externalRCVHWM = stoi(cfg["ZMQ.external-rcv-hwm"]);
    externalSNDHWM = stoi(cfg["ZMQ.external-snd-hwm"]);;
    internalRCVHWM = stoi(cfg["ZMQ.internal-rcv-hwm"]);;
    internalSNDHWM = stoi(cfg["ZMQ.internal-snd-hwm"]);;
    //HTTP options
    httpEndpoint = cfg["HTTP.endpoint"];
    staticFilePath = cfg["HTTP.static-file-path"];
    httpIPv4Only = parseBool(cfg["HTTP.ipv4-only"]);
    //Table options
    uint64_t defaultLRUCacheSize;
    uint64_t defaultTableBlockSize;
    uint64_t defaultWriteBufferSize;
    uint64_t defaultBloomFilterBitsPerKey;
    bool compressionEnabledPerDefault;
    std::string tableSaveFolder;

    //Check directories & ensure symlinks are resolved properly
    // and they exist and are readable
    string originalStaticFilePath = staticFilePath;
    staticFilePath = safeReadlink(staticFilePath);
    if(unlikely(staticFilePath.size() == 0)) {
        if(errno == ENOENT) {
            cerr << "\x1B[33m[Warn] Static file directory '"
                 << originalStaticFilePath
                 << "' does not exist! The HTTP server definitely won't work.\x1B[0m" << endl;
        } else if (errno == EACCES) {
            cerr << "\x1B[33m[Warn] Static file directory '"
                 << originalStaticFilePath
                 << "' can't be used because the current user does not have the permission to read / list the directory! The HTTP server probably won't work.\x1B[0m" << endl;
        } else {
            cerr << "\x1B[33m[Warn] Unknown error '"
                 << strerror(errno)
                 << "' while trying to check the HTTP static file directory. The HTTP server probably won't work.\x1B[0m" << endl;
        }
    } else if(staticFilePath.back() != '/') {
        staticFilePath += "/";
    }
    //Write the config data to the config file unless there are no arguments
    if(argc > 1 || (processedConfigFile && configFileName != "yak.cfg")) {
        saveConfigFile();
    }
}

const std::string& ConfigParser::getLogFile() {
    return logFile;
}

const std::string& ConfigParser::getTableSaveFolderPath() {
    return tableSaveFolder;
}

const std::vector<std::string>& ConfigParser::getREPEndpoints() {
    return repEndpoints;
}

const std::vector<std::string>& ConfigParser::getPULLEndpoints() {
    return pullEndpoints;
}

const std::vector<std::string>& ConfigParser::getSUBEndpoints() {
    return subEndpoints;
}

const bool ConfigParser::isIPv4Only() {
    return ipv4Only;
}


const std::string& ConfigParser::getHTTPEndpoint() {
    return httpEndpoint;
}

uint64_t ConfigParser::getDefaultLRUCacheSize() {
    return defaultLRUCacheSize;
}

uint64_t ConfigParser::getDefaultTableBlockSize() {
    return defaultTableBlockSize;
}

uint64_t ConfigParser::getDefaultWriteBufferSize() {
    return defaultWriteBufferSize;
}

uint64_t ConfigParser::getDefaultBloomFilterBitsPerKey() {
    return defaultBloomFilterBitsPerKey;
}

bool ConfigParser::isCompressionEnabledPerDefault() {
    return compressionEnabledPerDefault;
}

int ConfigParser::getInternalHWM() {
    return internalHWM;
}

int ConfigParser::getExternalHWM() {
    return externalHWM;
}

std::string ConfigParser::getStaticFilePath() {
    return staticFilePath;
}