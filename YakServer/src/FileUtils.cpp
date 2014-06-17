#include "FileUtils.hpp"
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <limits>

bool fileExists(const std::string& file) {
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

size_t getFilesize(const char* filename) {
    struct stat st;
    if(stat(filename, &st) == 0) {
        return std::numeric_limits<size_t>::max();
    }
    return st.st_size;   
}
