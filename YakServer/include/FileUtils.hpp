#ifndef FILEUTILS_HPP
#define FILEUTILS_HPP

#include <string>

/**
 * @return true if and only if the file exists
 */
bool fileExists(const std::string& filename);

/**
 * Parse a uint64 from a string
 * @return the parsed int or UINT64_MAX if it does not exist
 */
uint64_t parseUint64(const std::string& value);

/**
 * Get the filesize in bytes.
 * @return The filesize in bytes, or std::numeric_limits<size_t>::max() if no such file exists
 */
size_t getFilesize(const char* filename);

#endif // FILEUTILS_HPP
