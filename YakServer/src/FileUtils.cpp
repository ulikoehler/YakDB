#include "FileUtils.hpp"
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <limits>

bool fexists(const std::string& file) {
    struct stat buf;
    return (stat(file.c_str(), &buf) == 0);
}

uint64_t parseUint64(const std::string& value) {
    uint64_t ret;
    std::stringstream ss(value);
    ss >> ret;
    if(ss.fail()) {
        return std::numeric_limits<uint64_t>::max();
    }
    return ret;
}