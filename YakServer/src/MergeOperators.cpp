#include "MergeOperators.hpp"
#include "macros.hpp"

#include <iostream>
#include <rocksdb/env.h>

bool HOT Int64AddOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    // assuming 0 if no existing value
    int64_t existing = 0;
    if (existing_value != nullptr) {
        if (unlikely(existing_value->size() != sizeof(int64_t))) {
            // if existing_value is corrupted, treat it as 0
            Log(logger, "existing value corruption");
            existing = 0;
        } else {
            memcpy(&existing, existing_value->data(), sizeof(int64_t));
        }
    }

    int64_t operand;
    if (unlikely(value.size() != sizeof(int64_t))) {
        // if existing_value is corrupted, treat it as 0
        Log(logger, "operand value corruption");
        operand = 0;
    } else {
        memcpy(&operand, value.data(), sizeof(int64_t));
    }

    int64_t result = existing + operand;
    *new_value = std::move(std::string((char*)&result, sizeof(int64_t)));
    //Errors are treated as 0.
    return true;
}

const char* Int64AddOperator::Name() const {
    return "Int64 add";
}

bool HOT DMulOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    //Assuming 0 if no existing value
    double existing = 0;
    if (existing_value) {
        if (unlikely(existing_value->size() != sizeof(double))) {
            // if existing_value is corrupted, treat it as 0
            Log(logger, "existing value corruption");
            existing = 0;
        } else {
            memcpy(&existing, existing_value->data(), sizeof(double));
        }
    }

    double operand;
    if (unlikely(value.size() != sizeof(double))) {
        // if existing_value is corrupted, treat it as 0
        Log(logger, "operand value corruption");
        operand = 0;
    } else {
        memcpy(&operand, value.data(), sizeof(double));
    }

    double result = existing * operand;
    *new_value = std::move(std::string((char*)&result, sizeof(double)));
    //Errors are treated as 0.
    return true;
}

const char* DMulOperator::Name() const {
    return "Double multiplication";
}

bool HOT DAddOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    //Assuming 0 if no existing value
    double existing = 0;
    if (existing_value) {
        if (unlikely(existing_value->size() != sizeof(double))) {
            // if existing_value is corrupted, treat it as 0
            Log(logger, "existing value corruption");
            existing = 0;
        } else {
            memcpy(&existing, existing_value->data(), sizeof(double));
        }
    }

    double operand;
    if (unlikely(value.size() != sizeof(double))) {
        // if existing_value is corrupted, treat it as 0
        Log(logger, "operand value corruption");
        operand = 0;
    } else {
        memcpy(&operand, value.data(), sizeof(double));
    }

    double result = existing + operand;
    *new_value = std::move(std::string((char*)&result, sizeof(double)));
    //Errors are treated as 0.
    return true;
}

const char* DAddOperator::Name() const {
    return "Double add";
}

bool HOT AppendOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    // assuming empty if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    }

    *new_value = std::move(existing + value.ToString());
    //Errors are treated as 0.
    return true;
}

const char* AppendOperator::Name() const {
    return "AppendOperator";
}


bool ReplaceOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    *new_value = value.ToString();
    return true;
}

const char* ReplaceOperator::Name() const {
    return "Replace";
}

const char* ListAppendOperator::Name() const {
    return "List append";
}

bool HOT ListAppendOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    // assuming empty if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    }
    //Note that it is not inherently safe to assume the new value size is < 
    uint32_t newValLength = value.size();
    //In between the old and the new value, we need to add the 4 bytes size
    *new_value = std::move(existing + std::string((const char*)&newValLength, sizeof(uint32_t)) + value.ToString());
    return true;
}

const char* NULAppendOperator::Name() const {
    return "NUL-separated append";
}

bool HOT NULAppendOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    // assuming empty if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    }
    if(existing.size() == 0) {
        *new_value = value.ToString();
    } else { //Add NUL separator in between
        *new_value = existing + "\x00" + value.ToString();
    }
    return true;
}

bool HOT ANDOperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    //Assuming 0 if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    } else {
        //Skip complex boolean logic, just copy value
        *new_value = value.ToString();
        return true;
    }

    //Extract raw binary info
    const char* existingData = existing.data();
    size_t existingSize = existing.size();
    const char* valueData = value.data();
    size_t valueSize = value.size();

    /*
     * Allocate the result memory (will be copied to the result string later)
     * Writing to a string's data() could produce undefined behaviour
     *  with some C++ STL implementations
     */
    size_t dstSize = std::max(existingSize, valueSize);
    char* dst = new char[dstSize];

    /*
     * Perform bytewise boolean operation, ignoring extra bytes
     * TODO: Optimize to use 128+-bit SSE / 64 bit / 32 bit operator,
     *       or check if the compiler is intelligent enough to do that
     */
    size_t numCommonBytes = std::min(existingSize, valueSize);
    for(size_t i = 0; i < numCommonBytes; i++) {
        dst[i] = existingData[i] & valueData[i];
    }
    //Copy remaining bytes, if any
    if(existingSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, existingData + numCommonBytes, existingSize - numCommonBytes);
    } else if(valueSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, valueData + numCommonBytes, valueSize - numCommonBytes);
    } //Else: Both have the same size, nothing to be done

    //Copy dst buffer to new_value string
    *new_value = std::move(std::string(dst, dstSize));
    //Free allocated temporary resources
    delete[] dst;
    //This function does not have any logical error condition
    return true;
}

const char* ANDOperator::Name() const {
    return "Binary AND";
}


bool HOT OROperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    //Assuming 0 if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    } else {
        //Skip complex boolean logic, just copy value
        *new_value = value.ToString();
        return true;
    }

    //Extract raw binary info
    const char* existingData = existing.data();
    size_t existingSize = existing.size();
    const char* valueData = value.data();
    size_t valueSize = value.size();

    /*
     * Allocate the result memory (will be copied to the result string later)
     * Writing to a string's data() could produce undefined behaviour
     *  with some C++ STL implementations
     */
    size_t dstSize = std::max(existingSize, valueSize);
    char* dst = new char[dstSize];

    /*
     * Perform bytewise boolean operation, ignoring extra bytes
     * TODO: Optimize to use 128+-bit SSE / 64 bit / 32 bit operator,
     *       or check if the compiler is intelligent enough to do that
     */
    size_t numCommonBytes = std::min(existingSize, valueSize);
    for(size_t i = 0; i < numCommonBytes; i++) {
        dst[i] = existingData[i] | valueData[i];
    }
    //Copy remaining bytes, if any
    if(existingSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, existingData + numCommonBytes, existingSize - numCommonBytes);
    } else if(valueSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, valueData + numCommonBytes, valueSize - numCommonBytes);
    } //Else: Both have the same size, nothing to be done

    //Copy dst buffer to new_value string
    *new_value = std::move(std::string(dst, dstSize));
    //Free allocated temporary resources
    delete[] dst;
    //This function does not have any logical error condition
    return true;
}

const char* OROperator::Name() const {
    return "Binary OR";
}

bool HOT XOROperator::Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const {
    //Assuming 0 if no existing value
    std::string existing;
    if (existing_value) {
        existing = existing_value->ToString();
    } else {
        //Skip complex boolean logic, just copy value
        *new_value = value.ToString();
        return true;
    }

    //Extract raw binary info
    const char* existingData = existing.data();
    size_t existingSize = existing.size();
    const char* valueData = value.data();
    size_t valueSize = value.size();

    /*
     * Allocate the result memory (will be copied to the result string later)
     * Writing to a string's data() could produce undefined behaviour
     *  with some C++ STL implementations
     */
    size_t dstSize = std::max(existingSize, valueSize);
    char* dst = new char[dstSize];

    /*
     * Perform bytewise boolean operation, ignoring extra bytes
     * TODO: Optimize to use 128+-bit SSE / 64 bit / 32 bit operator,
     *       or check if the compiler is intelligent enough to do that
     */
    size_t numCommonBytes = std::min(existingSize, valueSize);
    for(size_t i = 0; i < numCommonBytes; i++) {
        dst[i] = existingData[i] ^ valueData[i];
    }
    //Copy remaining bytes, if any
    if(existingSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, existingData + numCommonBytes, existingSize - numCommonBytes);
    } else if(valueSize > numCommonBytes) {
        memcpy(dst + numCommonBytes, valueData + numCommonBytes, valueSize - numCommonBytes);
    } //Else: Both have the same size, nothing to be done

    //Copy dst buffer to new_value string
    *new_value = std::move(std::string(dst, dstSize));
    //Free allocated temporary resources
    delete[] dst;
    //This function does not have any logical error condition
    return true;
}

const char* XOROperator::Name() const {
    return "Binary XOR";
}

std::shared_ptr<rocksdb::MergeOperator> createMergeOperator(
    const std::string& mergeOperatorCode) {
    if(mergeOperatorCode.empty()) {
        //empty --> default
        return std::make_shared<ReplaceOperator>();
    } else if(mergeOperatorCode == "INT64ADD") {
        return std::make_shared<Int64AddOperator>();
    } else if(mergeOperatorCode == "DMUL") {
        return std::make_shared<DMulOperator>();
    } else if(mergeOperatorCode == "DADD") {
        return std::make_shared<DAddOperator>();
    } else if(mergeOperatorCode == "APPEND") {
        return std::make_shared<AppendOperator>();
    } else if(mergeOperatorCode == "REPLACE") { //Also handles REPLACE
        return std::make_shared<ReplaceOperator>();
    } else if(mergeOperatorCode == "AND") {
        return std::make_shared<ANDOperator>();
    } else if(mergeOperatorCode == "OR") {
        return std::make_shared<OROperator>();
    } else if(mergeOperatorCode == "XOR") {
        return std::make_shared<XOROperator>();
    } else if(mergeOperatorCode == "LISTAPPEND") {
        return std::make_shared<ListAppendOperator>();
    } else if(mergeOperatorCode == "NULAPPEND") {
        return std::make_shared<NULAppendOperator>();
    } else {
        std::cerr << "Warning: Invalid merge operator code: " << mergeOperatorCode << std::endl;
        return std::make_shared<ReplaceOperator>();
    }
}

bool isReplaceMergeOperator(const std::string& mergeOperatorCode) {
    return mergeOperatorCode == "REPLACE";
}