#include "http/URLParser.hpp"
#include <cstring>

using std::string;

char hexToChar(char hex1, char hex2) {
    return (hex1 - '0') * 16 + (hex2 - '0');
}

bool decodeURLEntities(const char* in, size_t length, std::string& out) {
    out.clear();
    out.reserve(length);
    for (size_t i = 0; i < length; i++) {
        if (in[i] == '%') {
            if (i + 3 <= length) {
                int value = 0;
                out += hexToChar(in[i+1], in[i+2]);
                i += 2;
            } else {
                return false;
            }
        } else if (in[i] == '+') {
            out += ' ';
        } else {
            out += in[i];
        }
    }
    return true;
}

std::string decodeURLEntities(const char* in, size_t length) {
    std::string ret;
    if(!decodeURLEntities(in, length, ret)) {
        return ""; //On error
    }
    return ret;
}
    

bool decodeURLEntities(const std::string& in, std::string& out) {
    return decodeURLEntities(in.data(), in.size(), out);
}

void parseQueryPart(const char* query, std::map<std::string, std::string>& map) {
    //Skip '?' at the beginning, if any
    if(query[0] == '?') {
        query++;
    }

    while(true) {
        char* kvSeparator = strchr(query, '=');
        if(kvSeparator == nullptr) {
            //No separator -- no argument left
            //This branch won't execute for correct inputs
            break;
        }
        char* argSeparator = strchr(kvSeparator, '&');
        string key = decodeURLEntities(query, (kvSeparator - query));
        if(argSeparator == nullptr) {
            //last argument
            map[key] = decodeURLEntities(kvSeparator + 1, strlen(kvSeparator + 1));
            break;
        } else {
            map[key] = decodeURLEntities(kvSeparator + 1, argSeparator - kvSeparator - 1);
        }
        query = argSeparator + 1;
    }
}