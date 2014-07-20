#ifndef __MERGE_OPERATORS_HPP
#define __MERGE_OPERATORS_HPP

#include <rocksdb/merge_operator.h>
#include <rocksdb/db.h>

/**
 * Signed 64-bit add operator
 */
class Int64AddOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

/**
 * Signed 64-bit double multiply operator
 */
class DMulOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

/**
 * Signed 64-bit double add operator
 */
class DAddOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

/**
 * Binary append operator
 */
class AppendOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};


/**
 * Replace merge operator. Acts as if no merge but a normal Put would be done.
 */
class ReplaceOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

/**
 * List append merge operator. Acts as if no merge but a normal Put would be done.
 */
class ListAppendOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

/**
 * Create a merge operator instance by merge operator code
 */
std::shared_ptr<rocksdb::MergeOperator> createMergeOperator(
    const std::string& mergeOperatorCode);

#endif //__MERGE_OPERATORS_HPP