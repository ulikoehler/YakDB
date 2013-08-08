#ifndef FILEUTILS_HPP
#define FILEUTILS_HPP

#include <string>

/**
 * @return true if and only if the file exists
 */
bool fexists(const std::string& filename);

/**
 * Parse a uint64 from a string
 * @return the parsed int or UINT64_MAX if it does not exist
 */
uint64_t parseUint64(const std::string& value);

#endif // FILEUTILS_HPP
