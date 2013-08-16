#include "BoyerMoore.hpp"

BoyerMooreHorspoolSearcher::BoyerMooreHorspoolSearcher(const char* pattern)
    : pattern(pattern),
      patternLength(strlen(pattern)) {
    if(patternLength != 0) {
        initializeSkipTable();
    }
}

BoyerMooreHorspoolSearcher::BoyerMooreHorspoolSearcher(const std::string& patternParam)
    : pattern(patternParam.c_str()),
      patternLength(patternParam.size()) {
    if(patternLength != 0) {
        initializeSkipTable();
    }
}

void BoyerMooreHorspoolSearcher::initializeSkipTable() {
    assert(pattern);
    //Initialize table to maximum skip length
    for (size_t i = 0;i < SKIPTABLE_LENGTH; i++) {
        skipTable[i] = patternLength;
    }
    //Calculate skip table for characters that exist in the pattern.
    for (size_t i = 0; i < patternLength - 1; i++) {
        skipTable[pattern[i]] = patternLength - i - 1;
    }
}

const int BoyerMooreHorspoolSearcher::find(const char* corpus) {
    return find(corpus, strlen(corpus));
}

const int BoyerMooreHorspoolSearcher::find(const std::string& corpus) {
    return find(corpus.c_str(), corpus.size());
}

const int BoyerMooreHorspoolSearcher::find(const char* corpus, size_t corpusLength) {
    assert(corpus);
    //Shortcut if there's an empty pattern (--> we DEFINE that as not found)
    if(patternLength == 0) {
        return -1;
    }
    //Can't find pattern if its larger than corpus
    if (patternLength > corpusLength) {
        return -1;
    }
    for(int k = patternLength - 1 ; k < corpusLength ; ) {
        int j = patternLength - 1;
        int i = k;
        while (j >= 0 && corpus[i] == pattern[j]) {
            j--;
            i--;
        }
        if (j == -1) {
            return i + 1;
        }
        k += skipTable[corpus[k]];
    }
    //Couldn't find it 
    return -1;
}