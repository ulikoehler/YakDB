#include "MergeOperators.hpp"
#include "macros.hpp"

#include <iostream>
#include <rocksdb/env.h>

bool Int64AddOperator::Merge(
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
    return "Int64AddOperator";
}

bool DMulOperator::Merge(
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
    return "DMulOperator";
}

bool AppendOperator::Merge(
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
    return "ReplaceOperator";
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
    } else if(mergeOperatorCode == "APPEND") {
        return std::make_shared<AppendOperator>();
    } else if(mergeOperatorCode == "REPLACE") { //Also handles REPLACE
        return std::make_shared<ReplaceOperator>();
    } else {
        std::cerr << "Warning: Invalid merge operator code: " << mergeOperatorCode << std::endl;
        return std::make_shared<ReplaceOperator>();
    }
}