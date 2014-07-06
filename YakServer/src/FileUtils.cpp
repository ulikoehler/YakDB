#include "FileUtils.hpp"
#include <sys/stat.h>
#include <dirent.h>
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
#include <iostream>
using namespace std;
size_t getDirectoryFilesize(const char* dirname) {
    /**
     * Using boost::filesystem would make this task easier,
     * however it would introduce a large, clunky dependency
     * which is not practicable for all applications YakDB is
     * designed for.
     */
    DIR* dir;
    struct dirent* ent;
    size_t filesizeSum = 0;
    cout << "FREC " << dirname << endl;
    if ((dir = opendir(dirname)) != nullptr) {
        //Iterate over entries in directory
        while ((ent = readdir(dir)) != nullptr) {
            if(ent->d_type == DT_DIR) {
                //Skip . and .. which lead to infinite recursion
                if (strcmp(".", ent->d_name) == 0 || strcmp("..", ent->d_name) == 0) {
                    continue;
                }
                //Recurse into directory
                filesizeSum += getDirectoryFilesize(ent->d_name);
            } else if(ent->d_type == DT_REG) {
                //Regular file -> use its size
                size_t filesize = getFilesize(ent->d_name);
                //getFilesize returns max() on error
                if(filesize == std::numeric_limits<size_t>::max()) {
                    filesize = 0;
                }
                filesizeSum += filesize;
            } //Else: Ignore
        }
        closedir (dir);
    }
    return filesizeSum;
}