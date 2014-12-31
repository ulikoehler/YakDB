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

class NULAppendOperator : public rocksdb::AssociativeMergeOperator {
 public:
  virtual bool Merge(
    const rocksdb::Slice& key,
    const rocksdb::Slice* existing_value,
    const rocksdb::Slice& value,
    std::string* new_value,
    rocksdb::Logger* logger) const override;

    virtual const char* Name() const override;
};

class NULAppendSetOperator : public rocksdb::AssociativeMergeOperator {
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
 * Arbitrary size binary boolean AND.
 * If existing/new value is shorter, missing bytes are assumed to be 0xFF (i.e. copied)
 */
class ANDOperator : public rocksdb::AssociativeMergeOperator {
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
 * Arbitrary size binary boolean OR.
 * If existing/new value is shorter, missing bytes are assumed to be 0x00 (i.e. copied)
 */
class OROperator : public rocksdb::AssociativeMergeOperator {
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
 * Arbitrary size binary boolean XOR.
 * If existing/new value is shorter, missing bytes are assumed to be 0x00 (i.e. copied)
 */
class XOROperator : public rocksdb::AssociativeMergeOperator {
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
 * @return The merge operator or nullptr (as shared ptr) if code is illegal
 */
std::shared_ptr<rocksdb::MergeOperator> createMergeOperator(
    const std::string& mergeOperatorCode);

/**
 * @return true if the given merge operator code represents
 *   a trivial replace operator
 */
bool isReplaceMergeOperator(const char* mergeOperatorCode);

#endif //__MERGE_OPERATORS_HPP