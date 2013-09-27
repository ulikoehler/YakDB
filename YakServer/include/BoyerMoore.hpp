#ifndef __BOYER_MOORE_HPP
#define __BOYER_MOORE_HPP

#include <cstring>
#include <cassert>
#include <cstdint>
#include <string>

#define SKIPTABLE_LENGTH 256

class BoyerMooreHorspoolSearcher {
public:
    /**
     * Initialize a new Boyer Moore Horspool searcher from a
     * cstring.
     */
    BoyerMooreHorspoolSearcher(const char* pattern);
    /**
     * Initialize a new Boyer Moore Horspool Searcher from
     * a std::string
     */
    BoyerMooreHorspoolSearcher(const std::string& pattern);
    /**
     * Find an occurrence of the pattern in a given corpus
     */
    const int find(const char* corpus);
    const int find(const std::string& corpus);
    const int find(const char* corpus, size_t corpusLength);
private:
    void initializeSkipTable();
    /**
     * The skip table
     */
    uint32_t skipTable[SKIPTABLE_LENGTH];
    const char* pattern;
    size_t patternLength;
};

#endif //__BOYER_MOORE_HPP