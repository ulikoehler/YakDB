
#ifndef CONFIGPARSER_HPP
#define CONFIGPARSER_HPP
#include <string>
#include <vector>

class ConfigParser {
public:
    ConfigParser(int argc, char** argv);
    const std::vector<std::string>& getREPEndpoints();
    const std::vector<std::string>& getPullEndpoints();
    const std::vector<std::string>& getSubEndpoints();
    const std::string& getLogFile();
private:
    std::string logFile;
    std::vector<std::string> repEndpoints;
    std::vector<std::string> pullEndpoints;
    std::vector<std::string> subEndpoints;
};

#endif //CONFIGPARSER_HPP