
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
    const bool isIPv4Only();
private:
    std::string logFile;
    std::vector<std::string> repEndpoints;
    std::vector<std::string> pullEndpoints;
    std::vector<std::string> subEndpoints;
    bool ipv4Only;
};

#endif //CONFIGPARSER_HPP