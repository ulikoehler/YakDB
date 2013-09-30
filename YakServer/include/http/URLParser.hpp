#ifndef __URLPARSER_HPP
#define __URLPARSER_HPP

#include <cstdlib>
#include <string>
#include <map>

/**
 * Convert a two-char hex character code (in URLs prefixed by '%')
 * to the corresponding character
 * @param hex1 The most significant hex char, e.g. 2 for %20
 * @param hex2 The most significant hex char, e.g. 2 for %20
 */
char hexToChar(char hex1, char hex2);

/**
 * Decode entities like %20 in a string
 * Examples:
 *  "a%20b" --> "a b"
 *  "c+d" --> "c d"
 * @return true if the URL has been parsed successfully
 */
bool decodeURLEntities(const std::string& in, std::string& out);
std::string decodeURLEntities(const char* in, size_t length);
bool decodeURLEntities(const char* in, size_t length, std::string& out);

/**
 * Parse the query part of an URL
 */
void parseQueryPart(const char* in, std::map<std::string, std::string>& map);

#endif //__URLPARSER_HPP