#include "macros.hpp"
#include "ConfigParser.hpp"
#include "FileUtils.hpp"
#include <string>
#include <regex>
#include <vector>
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>

namespace po = boost::program_options;
using std::string;
using std::vector;
using std::cout;
using std::endl;

/**
 * Check if a given ZMQ endpoint is a valid TCP or IPC endpoint
 */
static bool checkTCPIPCEndpoint(const string& endpoint) {
    //Check if the parameter is valid
    bool isTCP = endpoint.find("tcp://") == 0;
    bool isIPC = endpoint.find("ipc://") == 0;
    if(!isTCP && !isIPC) {
        cout << "Endpoint " << endpoint << " is not a valid TCP or IPC endpoint!" << endl;
        return false;
    }
    return true;
}

COLD ConfigParser::ConfigParser(int argc, char** argv) {
    // Declare the supported options.
    string configFileName;
    repEndpoints = {"tcp://localhost:7100","ipc:///tmp/yakserver-rep"};
    pullEndpoints = {"tcp://localhost:7101","ipc:///tmp/yakserver-pull"};
    subEndpoints = {"tcp://localhost:7102","ipc:///tmp/yakserver-sub"};
    po::options_description generalOptions("General options");
    generalOptions.add_options()
        ("help", "Print help message")
        ("logfile,l", po::value<string>(&logFile)->default_value(""), "The file the log will be written to")
        ("config,c",
            po::value<string>(&configFileName)->default_value("yak.cfg"),
            "The configuration file to use");
    po::options_description socketOptions("Socket options");
    socketOptions.add_options()
        ("req-endpoint,r", 
            po::value<vector<string> >(&repEndpoints),
            "The endpoints the REP backend will bind to.\nDefaults to tcp://localhost:7100, ipc:///tmp/yakserver-rep")
        ("pull-endpoint,p", po::value<vector<string> >(&pullEndpoints),
            "The endpoints the PULL backend will bind to.\nDefaults to tcp://localhost:7101, ipc:///tmp/yakserver-pull")
        ("sub-endpoint,s", po::value<vector<string> >(&pullEndpoints),
            "The endpoints the SUB backend will bind to.\nDefaults to tcp://localhost:7102, ipc:///tmp/yakserver-sub")
        ("ipv4-only,4","By default the application uses IPv6 sockets to bind to both IPv6 and IPv4. This option tells the application not to use IPv6 capable sockets.")
    ;
    //Create the main options group
    po::options_description desc("Options");
    desc.add(generalOptions).add(socketOptions);
    
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);
    //Parse the config file
    if(fexists(configFileName)) {
        po::store(po::parse_config_file<char>(configFileName.c_str(), desc, true), vm);
    }
    po::notify(vm);
    
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
    
    this->ipv4Only = (vm.count("ipv4-only") > 0);
}

const std::string& ConfigParser::getLogFile() {
    return logFile;
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