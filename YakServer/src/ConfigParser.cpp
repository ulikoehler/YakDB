#include <fstream>
#include <string>
#include <regex>
#include <unistd.h>
#include <vector>
#include <sstream>
#include <map>
#include <iostream>
#include "macros.hpp"
#include "ConfigParser.hpp"
#include "FileUtils.hpp"

namespace po = boost::program_options;
using std::string;
using std::vector;
using std::cout;
using std::cerr;
using std::endl;

/**
 * TODO refactor out our own commandline parser
 */

struct FlagInfo {
    /**
     * Initialize a CLI flag with an argument.
     */
    FlagInfo(const char* shortopt, const char* longopt, const std::string& defaultValue, const char* help)
        : shortopt(shortopt), longopt(longopt), value(defaultValue), defaultValue(defaultValue), help(help), isDefault(false), hasArgument(true) {
        }
        
    /**
     * Initialize a CLI flag without argument
     */
    FlagInfo(const char* shortopt, const char* longopt, const std::string& defaultValue, const char* help)
        : shortopt(shortopt), longopt(longopt), value(defaultValue), defaultValue(defaultValue), help(help), isDefault(false), hasArgument(false) {
     
    /**
     * Print help for the current flag to stderr.
     * TODO: Currently this does not handle terminal width and linebreaks correctly
     */
    void printToStderr() {
        cerr << "\t" << shortopt << " [ " << longopt << "] ";
        if(hasArgument) {
            cerr << "(= " << defaultValue << ") ";
        }
        cerr << help << '\n';
    }

    std::string value;
    std::string defaultValue;
    const char* help;
    /**
     * The short commandline option, without prefixed minus sign.
     * Example: "r" (--> "-r")
     */
    const char* shortopt;
    /**
     * The short commandline option, without prefixed double-minus sign.
     * Example: "record" (--> "--record")
     */
    const char* longopt;
    /**
     * True if the current value is equal to the default value.
     * False if the 
     */
    bool defaultOverridden;
    /**
     * True if this is a flag with a value. False otherwise.
     */
    bool hasArgument;
}

/**
 * Our own little commandline parser
 */
class CommandLineParser {
public:
    CommandLineParser() {
        
    }
    
    void parseFlags(int argc, char** argv) {
        //Save the program name for later usgae
        assert(argc >= 1);
        programName = argv[0];
        int pos = 1;
        while(pos < argv) {
            //Iterate over all flags
        }
    }
    
    
private:
    const char* programName;
    std::multimap<std::string, FlagInfo> optionGroupToOptions;
}

/**
 * Split a comma-separated string into its components.
 * Does not handle quotes at all. Any comma is treated as a delimiter
 */
static void split(const std::string& source, std::vector<std::string>& elems, char delimiter=',') {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        elems.push_back(item);
    }
    return elems;
}

/**
 * Check if a given ZMQ endpoint is a valid TCP or IPC endpoint
 */
static bool COLD checkTCPIPCEndpoint(const string& endpoint, bool allowIPC = true) {
    //Check if the parameter is valid
    bool isTCP = endpoint.find("tcp://") == 0;
    bool isIPC = endpoint.find("ipc://") == 0 && allowIPC;
    if(!isTCP && !isIPC) {
        cout << "Endpoint " << endpoint << " is not a valid TCP" << (allowIPC ? " or IPC": "") << " endpoint!" << endl;
        return false;
    }
    return true;
}

void COLD ConfigParser::saveConfigFile() {
    std::ofstream fout("yak.cfg");
    fout << "statistics-expunge-timeout=" << statisticsExpungeTimeout << '\n';
    fout << "static-file-path=" << staticFilePath << '\n';
    if(!logFile.empty()) {
        fout << "logfile=" << logFile << '\n';
    }
    for(string endpoint : repEndpoints) {
        fout << "rep-endpoint=" << endpoint << '\n';
    }
    for(string endpoint : pullEndpoints) {
        fout << "pull-endpoint=" << endpoint << '\n';
    }
    for(string endpoint : subEndpoints) {
        fout << "sub-endpoint=" << endpoint << '\n';
    }
    fout << "http-endpoint=" << httpEndpoint << '\n';
    if(ipv4Only) {
        fout << "ipv4-only" << '\n';
    }
    fout << "lru-cache-size=" << defaultLRUCacheSize << '\n';
    fout << "table-block-size=" << defaultTableBlockSize << '\n';
    fout << "write-buffer-size=" << defaultWriteBufferSize << '\n';
    fout << "bloom-filter-bits-per-key=" << defaultBloomFilterBitsPerKey << '\n';
    fout << "internal-hwm=" << internalHWM << '\n';
    fout << "external-hwm=" << externalHWM << '\n';
    if(!compressionEnabledPerDefault) {
        fout << "disable-compression=true" << '\n';
    }
    fout << "table-dir=" << tableSaveFolder << '\n';
    fout.close();
}

uint64_t ConfigParser::getStatisticsExpungeTimeout() {
    return statisticsExpungeTimeout;
}

/**
 * A buffer-overflow-safe readlink() wrapper for C++.
 * @return A string containing the readlink()ed filename, or
 *         an empty string with errno being set to the appropriate error.
 *         See the readlink() man(2) for errno details.
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


COLD ConfigParser::ConfigParser(int argc, char** argv) {
    /**
     * Remember when adding options, saveConfigFile() must also be updated!
     */
    // Declare the supported options.
    string configFileName;
    repEndpoints = {"tcp://*:7100","ipc:///tmp/yakserver-rep"};
    pullEndpoints = {"tcp://*:7101","ipc:///tmp/yakserver-pull"};
    subEndpoints = {"tcp://*:7102","ipc:///tmp/yakserver-sub"};
    
    
    httpEndpoint = "tcp://*:7109";
    po::options_description generalOptions("General options");
    generalOptions.add_options()
        ("help,h", "Print help message")
        ("logfile,l", po::value<string>(&logFile)->default_value(""), "")
        
        DEFINE_string(logfile, "", "If specified, the log will be written to stdout and this file.");
        DEFINE_string(configfile, "", "Read configuration paramters from the given file. CLI flags always take precedence.");
        DEFINE_string(config, "", "Read configuration paramters from the given file. CLI flags always take precedence.");
        
        ("config,c",
            po::value<string>(&configFileName)->default_value("yak.cfg"),
            "The configuration file to use")
        ("static-file-path",
            po::value<string>(&staticFilePath)->default_value("./static"),
            "The static file directory for HTML files")
        ("statistics-expunge-timeout",
            po::value<uint64_t>(&statisticsExpungeTimeout)->default_value(3600*1000)/*1 hour default*/, 
            "The time in milliseconds statistics will be saved for retrieval after a job has finished.");
    po::options_description socketOptions("Socket options");
    socketOptions.add_options()
        ("req-endpoints,r", 
            po::value<vector<string> >(&repEndpoints),
            "The endpoints the REP backend will bind to.\nDefaults to tcp://*:7100, ipc:///tmp/yakserver-rep")
        ("pull-endpoint,p", po::value<vector<string> >(&pullEndpoints),
            "The endpoints the PULL backend will bind to.\nDefaults to tcp://*:7101, ipc:///tmp/yakserver-pull")
        ("sub-endpoint,s", po::value<vector<string> >(&subEndpoints),
            "The endpoints the SUB backend will bind to.\nDefaults to tcp://*:7102, ipc:///tmp/yakserver-sub")
        ("http-endpoint,e", po::value<string>(&httpEndpoint),
            "The endpoint the internal HTTP server will listen on. Defaults to tcp://*:7109")
        ("ipv4-only,4","By default the application uses IPv6 sockets to bind to both IPv6 and IPv4. This option tells the application not to use IPv6 capable sockets.")
        ("external-hwm",
            po::value<int>(&externalHWM)->default_value(250),
            "Set the ZMQ High-watermark for external sockets")
        ("internal-hwm",
            po::value<int>(&internalHWM)->default_value(250),
            "Set the ZMQ High-watermark for internal sockets")
    ;
    po::options_description tableOptions("Table options");
    tableOptions.add_options()
        ("lru-cache-size",
            po::value<uint64_t>(&defaultLRUCacheSize)->default_value(1024 * 1024 * 16),
            "Set the default LRU cache size in bytes. Overriden by table-specific options.")
        ("table-block-size",
            po::value<uint64_t>(&defaultTableBlockSize)->default_value(256*1024),
            "Set the default table block size in bytes. Overriden by table-specific options.")
        ("write-buffer-size",
            po::value<uint64_t>(&defaultWriteBufferSize)->default_value(1024 * 1024 * 64),
            "Set the default write buffer size in bytes. Overriden by table-specific options.")
        ("bloom-filter-bits-per-key",
            po::value<uint64_t>(&defaultBloomFilterBitsPerKey)->default_value(0),
            "Set the default bits per key for the bloom filter. Set to 0 to disable bloom filter. Overriden by table-specific options.")
        ("disable-compression,d","By default table compression is enabled for all unconfigured tables. If this option is used, table compression is disabled by default. Overridden by table-specific options.")
        ("table-dir,t", po::value<string>(&tableSaveFolder)->default_value("./tables"), "The folder were the database tables should be saved to.")
    ;
    //Create the main options group
    po::options_description desc("Options");
    desc.add(generalOptions).add(socketOptions).add(tableOptions);
    
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    //Parse the config file
    bool processedConfigFile = false;
    if(fexists(configFileName)) {
        processedConfigFile = true;
        po::store(po::parse_config_file<char>(configFileName.c_str(), desc, true), vm);
    }
    po::notify(vm);
    //Check if --help is given
    if (vm.count("help")) {
        cout << desc << endl;
        exit(1);
    }
    //Check the endpoints
    for(string endpoint : repEndpoints) {
        if(!checkTCPIPCEndpoint(endpoint)) {
            cout << desc << endl;
            exit(1);
        }
    }
    for(string endpoint : pullEndpoints) {
        if(!checkTCPIPCEndpoint(endpoint)) {
            cout << desc << endl;
            exit(1);
        }
    }
    for(string endpoint : subEndpoints) {
        if(!checkTCPIPCEndpoint(endpoint)) {
            cout << desc << endl;
            exit(1);
        }
    }
    if(!checkTCPIPCEndpoint(httpEndpoint, false)) {
        cout << desc << endl;
        exit(1);
    }
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
    //Get bool-ish options
    this->ipv4Only = (vm.count("ipv4-only") > 0);
    this->compressionEnabledPerDefault = (vm.count("disable-compression") > 0);
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