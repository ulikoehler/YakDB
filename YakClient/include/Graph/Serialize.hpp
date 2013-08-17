#ifndef __GRAPH_SERIALIZE_HPP
#define __GRAPH_SERIALIZE_HPP
#include <string>
#include <cstdint>

/**
 * Serialize a basic attribute set
 * @param size The result size will be placed here.
 * @return A new[] allocated serialized dataset.
 *         The caller must ensure it frees the return value
 */
template<typename MapType>
char* serializeBasicAttributes(const MapType& map, size_t& size) {
    //Get size
    size = 0;
    for(auto pair : map) {
        size += pair.first.size() + pair.second.size() + 2;
    }
    //Serialize
    char* data = new char[size];
    char* curPos = data;
    for(auto pair : map) {
        //Write key
        size_t keySize = pair.first.size();
        memcpy(curPos, pair.first.c_str(), keySize);
        curPos += keySize;
        *curPos = 0x1F;
        curPos++;
        //Write value
        size_t valSize = pair.second.size();
        memcpy(curPos, pair.second.c_str(), valSize);
        curPos += valSize;
        *curPos = 0x1E;
        curPos++;
    }
    return data;
}

/**
 * Serialize the key (=ID) of an extended attribute.
 * @return A new[]-allocated char array containing the entity ID.
 *      The targetSize parameter is set to the number of bytes in that array.
 * @param size The size of the returned array
 */
char* serializeExtAttrId(const std::string& entityId, const std::string& key, size_t& targetSize);
char* serializeExtAttrId(const char* entityId,
                         size_t entityIdLength,
                         const char* key,
                         size_t keyLength,
                         size_t& targetSize
                        );

#endif //__GRAPH_SERIALIZE_HPP
