#include "http/URLParser.hpp"


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



std::map<std::string, std::string> parseQueryPart(const std::string& in) {
    std::map<std::string, std::string> ret;
    vector<string> andSplit;
    split(andSplit, queryPart, is_any_of("&"));

    for(string kv : andSplit) {
        vector<string> kvPair;
        split(kvPair, kv, is_any_of("="));
        //Decode the paths
        string key, value;
        url_decode(kvPair[0], key);
        url_decode(kvPair[1], value);
        ret[kvPair[0]] = kvPair[1];
    }
    return ret;
}