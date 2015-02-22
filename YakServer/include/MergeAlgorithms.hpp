#ifndef __MERGE_ALGORITHMS_HPP
#define __MERGE_ALGORITHMS_HPP


/**
 * Efficiently plit at NUL characters.
 * Algorithm: Iterate over chars until \0 char is found,
 *   then emplace a string from the current substr start
 *   to the current character in the container.
 * Expects traits:
 *   - Container has emplace method
 *   - Container member has (char* data, size_t length) constructor
 */
template <class Container>
void splitByNUL(Container& container, const char* data, size_t n) {
    if(n == 0) {
        return;
    }
    size_t currentSubstrOffset = 0; //Stores begin of the current substring
    for (size_t i = 0; i < n; ++i) {
        if(data[i] == '\0') {
            //Add current substring to set
            container.emplace(data + currentSubstrOffset, i - currentSubstrOffset);
            //Next substring begins at next character
            currentSubstrOffset = i + 1; //NOTE: Might be > n
        }
    }
    //Emplace last substring (n = 0 already handled)
    container.emplace(data + currentSubstrOffset, n - currentSubstrOffset);
}

#endif //__MERGE_ALGORITHMS_HPP