#include <cstdlib>
#include <string>

using std::string;

/**
 * Convert a two-char hex character code (in URLs prefixed by '%')
 * to the corresponding character
 * @param hex1 The most significant hex char, e.g. 2 for %20
 * @param hex2 The most significant hex char, e.g. 2 for %20
 */
char hexToChar(char hex1, char hex2) {
    return (hex1 - '0') * 16 + (hex2 - '0');
}

/**
 * Decode entities like %20 in a string
 * Examples:
 *  "a%20b" --> "a b"
 *  "c+d" --> "c d"
 * @return true if the URL has been parsed successfully
 */
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