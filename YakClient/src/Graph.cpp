/**
 * Note: This file contains implementations for different header files.
 */
#include <cstring>
#include "Graph/Serialize.hpp"

char* serializeExtAttrId(const std::string& entityId, const std::string& key, size_t& targetSize) {
    return serializeExtAttrId(entityId.data(), entityId.size(), key.data(), key.size(), targetSize);
}
char* serializeExtAttrId(const char* entityId,
                         size_t entityIdLength,
                         const char* key,
                         size_t keyLength,
                         size_t& targetSize
                        ) {
    targetSize = entityIdLength + keyLength + 1;
    char* buf = new char[targetSize];
    memcpy(buf, entityId, entityIdLength);
    buf[entityIdLength] = '\x1D';
    memcpy(buf + entityIdLength + 1, key, keyLength);
    return buf;
}