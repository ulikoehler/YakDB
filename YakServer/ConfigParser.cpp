#include "macros.hpp"
#include "ConfigParser.hpp"
#include "FileUtils.hpp"
#include <string>
#include <vector>
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/program_options/parsers.hpp>

namespace po = boost::program_options;
using std::string;
using std::vector;
using std::cout;
using std::endl;

COLD ConfigParser::ConfigParser(int argc, char** argv) {
    // Declare the supported options.
    po::options_description desc("Options");
    string configFileName;
    repEndpoints = {"tcp://localhost:7100","ipc:///tmp/yakserver-rep"};
    pullEndpoints = {"tcp://localhost:7101","ipc:///tmp/yakserver-pull"};
    subEndpoints = {"tcp://localhost:7102","ipc:///tmp/yakserver-sub"};
    desc.add_options()
        ("help", "Print help message")
        ("l,logfile", po::value<string>(&logFile)->default_value(""), "The file the log will be written to")
        ("c,config",
            po::value<string>(&configFileName)->default_value("yak.cfg"),
            "The configuration file to use")
        ("req-endpoint", 
            po::value<vector<string> >(&repEndpoints),
            "The endpoints the REP backend will bind to.\nDefaults to tcp://localhost:7100, ipc:///tmp/yakserver-rep")
        ("pull-endpoint", po::value<vector<string> >(&pullEndpoints),
            "The endpoints the PULL backend will bind to.\nDefaults to tcp://localhost:7101, ipc:///tmp/yakserver-pull")
        ("sub-endpoint", po::value<vector<string> >(&pullEndpoints),
            "The endpoints the SUB backend will bind to.\nDefaults to tcp://localhost:7102, ipc:///tmp/yakserver-sub")
    ;
    
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
}

const std::string& ConfigParser::getLogFile() {
    return logFile;
}

const std::vector<std::string>& ConfigParser::getREPEndpoints() {
    return repEndpoints;
}

const std::vector<std::string>& ConfigParser::getPullEndpoints() {
    return pullEndpoints;
}

const std::vector<std::string>& ConfigParser::getSubEndpoints() {
    return subEndpoints;
}