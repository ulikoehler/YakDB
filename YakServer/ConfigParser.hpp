
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
private:
    std::string logFile;
    std::vector<std::string> repEndpoints;
    std::vector<std::string> pullEndpoints;
    std::vector<std::string> subEndpoints;
    std::string httpEndpoint;
    bool ipv4Only;
};

#endif //CONFIGPARSER_HPP