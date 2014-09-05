#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include <vector>

#include "grnxx/types.hpp"

#include "grnxx/array/primary.hpp"

namespace grnxx {

template <>
class ArrayCRef<Record> {
 public:
  using Value = Record;

  ArrayCRef() = default;
  ArrayCRef(const ArrayCRef &) = default;

  ArrayCRef &operator=(const ArrayCRef &) = default;

  bool operator==(ArrayCRef<Record> rhs) const {
    return (records_ == rhs.records_) && (size_ == rhs.size_);
  }
  bool operator!=(ArrayCRef<Record> rhs) const {
    return (records_ != rhs.records_) || (size_ != rhs.size_);
  }

  ArrayCRef ref(Int offset = 0) const {
    return ArrayCRef(records_ + offset, size_ - offset);
  }
  ArrayCRef ref(Int offset, Int size) const {
    return ArrayCRef(records_ + offset, size);
  }

  Record get(Int i) const {
    return records_[i];
  }
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  Float get_score(Int i) const {
    return records_[i].score;
  }

  Record operator[](Int i) const {
    return records_[i];
  }

  Int size() const {
    return size_;
  }

 private:
  const Record *records_;
  Int size_;

  ArrayCRef(const Record *records, Int size)
      : records_(records),
        size_(size) {}

  friend class ArrayRef<Value>;
  friend class Array<Value>;
};

template <>
class ArrayRef<Record> {
 public:
  using Value = Record;

  ArrayRef() = default;
  ArrayRef(const ArrayRef &) = default;

  ArrayRef &operator=(const ArrayRef &) = default;

  bool operator==(ArrayCRef<Record> rhs) const {
    return (records_ == rhs.records_) && (size_ == rhs.size_);
  }
  bool operator==(ArrayRef<Record> rhs) const {
    return (records_ == rhs.records_) && (size_ == rhs.size_);
  }

  bool operator!=(ArrayCRef<Record> rhs) const {
    return (records_ != rhs.records_) || (size_ != rhs.size_);
  }
  bool operator!=(ArrayRef<Record> rhs) const {
    return (records_ != rhs.records_) || (size_ != rhs.size_);
  }

  operator ArrayCRef<Record>() const {
    return ref();
  }

  ArrayCRef<Record> ref(Int offset = 0) const {
    return ArrayCRef<Record>(records_ + offset, size_ - offset);
  }
  ArrayCRef<Record> ref(Int offset, Int size) const {
    return ArrayCRef<Record>(records_ + offset, size);
  }

  ArrayRef ref(Int offset = 0) {
    return ArrayRef(records_ + offset, size_ - offset);
  }
  ArrayRef ref(Int offset, Int size) {
    return ArrayRef(records_ + offset, size);
  }

  Record get(Int i) const {
    return records_[i];
  }
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  Float get_score(Int i) const {
    return records_[i].score;
  }
  void set(Int i, Record record) {
    records_[i] = record;
  }
  void set_row_id(Int i, Int row_id) {
    records_[i].row_id = row_id;
  }
  void set_score(Int i, Float score) {
    records_[i].score = score;
  }

  Record operator[](Int i) const {
    return records_[i];
  }

  Int size() const {
    return size_;
  }

  void swap(Int i, Int j) {
    std::swap(records_[i], records_[j]);
  }

 private:
  Record *records_;
  Int size_;

  ArrayRef(Record *records, Int size) : records_(records), size_(size) {}

  friend class Array<Value>;
};

template <>
class Array<Record> {
 public:
  Array() : records_() {}
  ~Array() {}

  Array(Array &&array) : records_(std::move(array.records_)) {}

  Array &operator=(Array &&array) {
    records_ = std::move(array.records_);
    return *this;
  }

  operator ArrayCRef<Record>() const {
    return ref();
  }
  operator ArrayRef<Record>() {
    return ref();
  }

  ArrayCRef<Record> ref(Int offset = 0) const {
    return ArrayCRef<Record>(records_.data() + offset, size() - offset);
  }
  ArrayCRef<Record> ref(Int offset, Int size) const {
    return ArrayCRef<Record>(records_.data() + offset, size);
  }

  ArrayRef<Record> ref(Int offset = 0) {
    return ArrayRef<Record>(records_.data() + offset, size() - offset);
  }
  ArrayRef<Record> ref(Int offset, Int size) {
    return ArrayRef<Record>(records_.data() + offset, size);
  }

  Record get(Int i) const {
    return records_[i];
  }
  Int get_row_id(Int i) const {
    return records_[i].row_id;
  }
  Float get_score(Int i) const {
    return records_[i].score;
  }
  void set(Int i, Record record) {
    records_[i] = record;
  }
  void set_row_id(Int i, Int row_id) {
    records_[i].row_id = row_id;
  }
  void set_score(Int i, Float score) {
    records_[i].score = score;
  }

  Record operator[](Int i) const {
    return records_[static_cast<size_t>(i)];
  }

  Record front() const {
    return records_.front();
  }

  Record back() const {
    return records_.back();
  }

  Int size() const {
    return static_cast<Int>(records_.size());
  }
  Int capacity() const {
    return static_cast<Int>(records_.capacity());
  }

  bool reserve(Error *error, Int new_size) try {
    records_.reserve(new_size);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  bool resize(Error *error, Int new_size) try {
    records_.resize(new_size);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  bool resize(Error *error, Int new_size, Record record) try {
    records_.resize(new_size, record);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  bool shrink_to_fit(Error *error) try {
    records_.shrink_to_fit();
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  void clear() {
    records_.clear();
  }

  void erase(Int i) {
    records_.erase(records_.begin() + i);
  }

  bool push_back(Error *error, Record record) try {
    records_.push_back(record);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  void pop_back() {
    records_.pop_back();
  }

  void swap(Int i, Int j) {
    std::swap(records_[i], records_[j]);
  }

 private:
  std::vector<Record> records_;
};

}  // namespace grnxx

#include "grnxx/array/bool.hpp"

#endif  // GRNXX_ARRAY_HPP
