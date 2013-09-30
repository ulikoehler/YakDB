#include "http/URLParser.hpp"
#include <cstring>

using std::string;

char hexToChar(char hex1, char hex2) {
    return (hex1 - '0') * 16 + (hex2 - '0');
}

bool decodeURLEntities(const std::string& in, std::string& out) {
    out.clear();
    out.reserve(in.size());
    for (size_t i = 0; i < in.size(); i++) {
        if (in[i] == '%') {
            if (i + 3 <= in.size()) {
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



std::map<std::string, std::string> parseQueryPart(const char* query) {
    std::map<std::string, std::string> ret;
    
    //Skip '?' at the beginning, if any
    if(query[0] == '?') {
        query++;
    }

    while(true) {
        char* kvSeparator = strchr(query, '=');
        if(kvSeparator == nullptr) {
            //No separator -- no argument left
            break;
        }
        char* argSeparator = strchr(kvSeparator, '&');
        string key(query, (kvSeparator - query));
        if(argSeparator == nullptr) {
            //last argument
            ret[key] = string(kvSeparator);
            break;
        } else {
            ret[key] = string(kvSeparator, argSeparator - kvSeparator - 1);
        }
        query = argSeparator + 1;
    }
    return ret;
}