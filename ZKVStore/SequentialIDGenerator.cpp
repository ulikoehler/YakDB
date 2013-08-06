/* 
 * File:   SequentialIDGenerator.cpp
 * Author: uli
 * 
 * Created on 10. Februar 2013, 04:14
 */

#include "SequentialIDGenerator.hpp"
#include "macros.hpp"
#include <cstdio>
#include <sys/stat.h>
#include <sys/types.h>

bool COLD fexists(const std::string& file) {
    struct stat buf;
    return (stat(file.c_str(), &buf) == 0);
}

COLD SequentialIDGenerator::SequentialIDGenerator(const std::string& file) : filename(file), nextId(), noFilePersistence(false) {
    if (fexists(file)) {
        //For maximum compatibility and the least possible binsize
        // use C IO here instead of fstreams etc.
        FILE* fin = fopen(file.c_str(), "r");
        uint64_t tempVal = 1;
        if (fread(&tempVal, sizeof (uint64_t), 1, fin) < 1) {
            //We *could* log that we created the file
            fprintf(stderr, "Failed to read sequential ID initialization data from %s - initializing to zero.", file.c_str());
            tempVal = 1;
        }
        fclose(fin);
        std::atomic_store(&nextId, tempVal);
    } else {
        //Initialize the atomic variable
        std::atomic_store(&nextId, (uint64_t) 1);
        //Create the file
        persist();
    }
}

COLD SequentialIDGenerator::SequentialIDGenerator() : nextId(), noFilePersistence(true) {
    std::atomic_store(&nextId, (uint64_t) 1);
}
COLD SequentialIDGenerator::SequentialIDGenerator(uint64_t nextId) : nextId(), noFilePersistence(true) {
    std::atomic_store((std::atomic<uint64_t>*)&nextId, nextId);
}

void COLD SequentialIDGenerator::setFilename(const std::string& newFilename) {
    noFilePersistence = false;
    filename = newFilename;
}

void COLD SequentialIDGenerator::disableFilePersistence() {
    noFilePersistence = true;
}

COLD SequentialIDGenerator::~SequentialIDGenerator() {
    persist();
}

uint64_t HOT SequentialIDGenerator::getNewId() {
    return std::atomic_fetch_add(&nextId, (uint64_t) 1);
}

uint64_t HOT SequentialIDGenerator::getNextId() const {
    return std::atomic_load(&nextId);
}

void HOT SequentialIDGenerator::setNextId(uint64_t newValue) {
    std::atomic_store(&nextId, newValue);

}

void SequentialIDGenerator::persist() {
    if (noFilePersistence) {
        return;
    }
    uint64_t nextIdValue = std::atomic_load(&nextId);
    FILE* fout = fopen(filename.c_str(), "w");
    if (fwrite(&nextIdValue, sizeof (uint64_t), 1, fout) < 1) {
        fprintf(stderr, "Failed to write sequential ID initialization data to %s - counter value is %lu", filename.c_str(), nextIdValue);
        nextId = 0;
    }
    fclose(fout);
}