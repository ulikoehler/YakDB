#include "Batch.hpp"
#include "YakClient.hpp"
#include "WriteRequests.hpp"

AutoPutBatch::AutoPutBatch(YakClient& conn, uint32_t tableNo, size_t batchSize, uint8_t flags) :
socket(conn.getSocket()),
wroteHeader(false),
haveLastKV(false),
batchSize(batchSize),
currentBatchSize(0),
tableNo(tableNo),
flags(flags) {
    
}

AutoPutBatch::~AutoPutBatch() {
    flush();
}

void AutoPutBatch::flush() {
    if(haveLastKV) {
        PutRequest::sendKeyValue(socket, lastKey, lastValue, true);
        haveLastKV = false;
    }
    currentBatchSize = 0;
}

void AutoPutBatch::put(std::string& key, std::string& value) {
    if(!wroteHeader) {
        PutRequest::sendHeader(socket, tableNo, flags);
        wroteHeader = true;
    }
    if(haveLastKV) {
        PutRequest::sendKeyValue(socket, lastKey, lastValue, false);
    }
    //Swap the new KV values
    lastKey.swap(key);
    lastValue.swap(value);
    haveLastKV = true;
    currentBatchSize++;
    if(currentBatchSize >= batchSize) {
        flush();
    }
}

void AutoPutBatch::put(const char* key, const char* value) {
    std::string keyString(key);
    std::string valueString(value);
    put(keyString, valueString);
}