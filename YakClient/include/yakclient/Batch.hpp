#ifndef BATCH_HPP
#define BATCH_HPP
#include <zmq.h>
#include <string>
#include <cstdint>
#include "YakClient.hpp"

/**
 * A put batch that automatically batches
 * write requests.
 * 
 * The user must ensure that only one write batch is used at a time,
 * and no other requests and threads are active on the same connection.
 * 
 * Additionally, the user must ensure the connection will be valid
 * when the destructor is called.
 */
class AutoPutBatch {
public:
    AutoPutBatch(YakClient& conn, uint32_t tableNo=1, size_t batchSize=2500, uint8_t flags = 0);
    /**
     * Destructor -- Auto-flushes.
     */
    ~AutoPutBatch();
    /**
     * Uses move-semantics to efficiently batch-write
     * the current key-value
     * 
     * @param key The key to write. Value after functions returns is undefined.
     * @param key The value to write. Value after functions returns is undefined.
     */
    void put(std::string& key, std::string& value);
    /**
     * Simple cstring put
     */
    void put(const char* key, const char* value);
    /**
     * Call this to manually flush the current instance.
     */
    void flush();
private:
    void* socket;
    size_t batchSize;
    size_t currentBatchSize;
    uint32_t tableNo;
    bool wroteHeader;
    bool haveLastKV;
    std::string lastKey;
    std::string lastValue;
    uint8_t flags;
};

#endif //BATCH_HPP