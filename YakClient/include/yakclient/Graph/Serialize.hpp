#ifndef __GRAPH_SERIALIZE_HPP
#define __GRAPH_SERIALIZE_HPP
#include <string>
#include <cstdint>
#include <map>

/**
 * Serialize a basic attribute set
 * @param size The result size will be placed here.
 * @return A new[] allocated serialized dataset.
 *         The caller must ensure it frees the return value
 */
template<typename MapType>
char* serializeBasicAttributes(const MapType& map, size_t* size) {
    //Get size
    *size = 0;
    for(auto pair : map) {
        *size += pair.first.size() + pair.second.size() + 2;
    }
    //Serialize
    char* data = new char[*size];
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
 * Serialize a single-key-value basic attribute set
 * @return A new[] allocated serialized dataset.
 *         The caller must ensure it frees the return value
 */
char* serializeBasicAttributes(const std::string& key, const std::string& value, size_t* actualSize);

/**
 * Serialize the key (=ID) of an extended attribute.
 * @return A new[]-allocated char array containing the entity ID.
 *      The calculatedLength parameter is set to the number of bytes in that array.
 * @param size The size of the returned array
 */
char* serializeExtAttrId(const std::string& entityId, const std::string& key, size_t* targetSize);
char* serializeExtAttrId(const char* entityId,
                         size_t entityIdLength,
                         const char* key,
                         size_t keyLength,
                         size_t* calculatedLength
                        );

/**
 * Serialize both edge IDs at once
 * @param sourceNodeId The source node identifier
 * @param targetNodeId The target node identifier
 * @param type The edge type
 * @param calculatedLength This will be set to the number of valid chars in forwardTarget and backwardTarget
 * @param forwardTarget A new[] allocated pointer to the forward edge ID will be placed here
 * @param backwardTarget A new[] allocated pointer to the backward edge ID will be placed here
 */
void serializeEdgeId(const std::string& sourceNodeId,
                     const std::string& targetNodeId,
                     const std::string& edgeType,
                     size_t* calculatedLength,
                     char** forwardTarget,
                     char** backwardTarget);
void serializeEdgeId(const char* sourceNodeId,
                   size_t sourceNodeIdLength,
                   const char* targetNodeId,
                   size_t targetNodeIdLength,
                   const char* type,
                   size_t typeLength,
                   size_t* calculatedLength,
                   char** forwardTarget,
                   char** backwardTarget
                  );

#endif //__GRAPH_SERIALIZE_HPP
