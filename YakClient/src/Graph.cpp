/**
 * Note: This file contains implementations for different header files.
 */
#include <cstring>
#include "Graph/Serialize.hpp"

char* serializeExtAttrId(const std::string& entityId, const std::string& key, size_t* calculatedLength) {
    return serializeExtAttrId(entityId.data(), entityId.size(), key.data(), key.size(), calculatedLength);
}

char* serializeBasicAttributes(const std::string& key, const std::string& value, size_t* actualSize) {
    size_t keySize = key.size();
    size_t valueSize = value.size();
    *actualSize = keySize + valueSize + 2;
    char* res = new char[*actualSize];
    char* curPos = res;
    //Write key
    memcpy(curPos, key.c_str(), keySize);
    curPos += keySize;
    *curPos = 0x1F;
    curPos++;
    //Write value
    memcpy(curPos, value.c_str(), valueSize);
    curPos += valueSize;
    *curPos = 0x1E;
    curPos++;
    return res;
}

char* serializeExtAttrId(const char* entityId,
                         size_t entityIdLength,
                         const char* key,
                         size_t keyLength,
                         size_t* calculatedLength
                        ) {
    *calculatedLength = entityIdLength + keyLength + 1;
    char* buf = new char[*calculatedLength];
    memcpy(buf, entityId, entityIdLength);
    buf[entityIdLength] = '\x1D';
    memcpy(buf + entityIdLength + 1, key, keyLength);
    return buf;
}

void serializeEdgeId(const std::string& sourceNodeId,
                     const std::string& targetNodeId,
                     const std::string& edgeType,
                     size_t* calculatedLength,
                     char** forwardTarget,
                     char** backwardTarget) {
    serializeEdgeId(
        sourceNodeId.data(),
        sourceNodeId.size(),
        targetNodeId.data(),
        targetNodeId.size(),
        edgeType.data(),
        edgeType.size(),
        calculatedLength,
        forwardTarget,
        backwardTarget
    );
}

void serializeEdgeId(const char* sourceNodeId,
                   size_t sourceNodeIdLength,
                   const char* targetNodeId,
                   size_t targetNodeIdLength,
                   const char* type,
                   size_t typeLength,
                   size_t* calculatedLength,
                   char** forwardTarget,
                   char** backwardTarget
                  ) {
    *calculatedLength = sourceNodeIdLength + targetNodeIdLength + typeLength + 2;
    char* forward = new char[*calculatedLength];
    char* backward = new char[*calculatedLength];
    *forwardTarget = forward;
    *backwardTarget = backward;
    //--Serialize--
    //Type
    memcpy(forward, type, typeLength);
    memcpy(backward, type, typeLength);
    //Type separator
    forward[typeLength] = '\x1F';
    backward[typeLength] = '\x1F';
    //Primary node
    memcpy(forward + typeLength + 1, sourceNodeId, sourceNodeIdLength);
    memcpy(backward + typeLength + 1, targetNodeId, targetNodeIdLength);
    //Node separator
    size_t forwardPos = typeLength + 1 + sourceNodeIdLength;
    size_t backwardPos = typeLength + 1 + targetNodeIdLength;
    forward[forwardPos] = '\x0E';
    backward[backwardPos] = '\x0F';
    //Secondary node
    memcpy(forward + forwardPos + 1, targetNodeId, targetNodeIdLength);
    memcpy(backward + backwardPos + 1, sourceNodeId, sourceNodeIdLength);
}