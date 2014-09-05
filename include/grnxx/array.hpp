#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include <vector>

#include "grnxx/types.hpp"

#include "grnxx/array/primary.hpp"

namespace grnxx {

//class ArrayErrorReporter {
// public:
//  static void report_memory_error(Error *error);
//};

//template <typename T> class ArrayCRef;
//template <typename T> class ArrayRef;
//template <typename T> class Array;

//template <typename T>
//class ArrayCRef {
// public:
//  using Value = T;

//  ArrayCRef() = default;
//  ArrayCRef(const ArrayCRef &) = default;

//  ArrayCRef &operator=(const ArrayCRef &) = default;

//  bool operator==(ArrayCRef<Value> rhs) const {
//    return (values_ == rhs.values_) && (size_ == rhs.size_);
//  }
//  bool operator!=(ArrayCRef<Value> rhs) const {
//    return (values_ != rhs.values_) || (size_ != rhs.size_);
//  }

//  ArrayCRef ref(Int offset = 0) const {
//    return ArrayCRef(values_ + offset, size_ - offset);
//  }
//  ArrayCRef ref(Int offset, Int size) const {
//    return ArrayCRef(values_ + offset, size);
//  }

//  Value get(Int i) const {
//    return values_[i];
//  }

//  const Value &operator[](Int i) const {
//    return values_[i];
//  }

//  Int size() const {
//    return size_;
//  }

// private:
//  const Value *values_;
//  Int size_;

//  ArrayCRef(const Value *values, Int size) : values_(values), size_(size) {}

//  friend class ArrayRef<Value>;
//  friend class Array<Value>;
//};

//template <typename T>
//class ArrayRef {
// public:
//  using Value = T;

//  ArrayRef() = default;
//  ArrayRef(const ArrayRef &) = default;

//  ArrayRef &operator=(const ArrayRef &) = default;

//  operator ArrayCRef<Value>() const {
//    return ref();
//  }

//  bool operator==(ArrayCRef<Value> rhs) const {
//    return (values_ == rhs.values_) && (size_ == rhs.size_);
//  }
//  bool operator==(ArrayRef<Value> rhs) const {
//    return (values_ == rhs.values_) && (size_ == rhs.size_);
//  }

//  bool operator!=(ArrayCRef<Value> rhs) const {
//    return (values_ != rhs.values_) || (size_ != rhs.size_);
//  }
//  bool operator!=(ArrayRef<Value> rhs) const {
//    return (values_ != rhs.values_) || (size_ != rhs.size_);
//  }

//  ArrayCRef<Value> ref(Int offset = 0) const {
//    return ArrayCRef<Value>(values_ + offset, size_ - offset);
//  }
//  ArrayCRef<Value> ref(Int offset, Int size) const {
//    return ArrayCRef<Value>(values_ + offset, size);
//  }

//  ArrayRef ref(Int offset = 0) {
//    return ArrayRef(values_ + offset, size_ - offset);
//  }
//  ArrayRef ref(Int offset, Int size) {
//    return ArrayRef(values_ + offset, size);
//  }

//  Value get(Int i) const {
//    return values_[i];
//  }
//  void set(Int i, Value value) {
//    values_[i] = value;
//  }

//  Value &operator[](Int i) {
//    return values_[i];
//  }
//  const Value &operator[](Int i) const {
//    return values_[i];
//  }

//  Int size() const {
//    return size_;
//  }

//  void swap(Int i, Int j) {
//    std::swap(values_[i], values_[j]);
//  }

// private:
//  Value *values_;
//  Int size_;

//  ArrayRef(Value *values, Int size) : values_(values), size_(size) {}

//  friend class Array<Value>;
//};

//template <typename T>
//inline bool operator==(ArrayCRef<T> lhs, ArrayRef<T> rhs) {
//  return rhs == lhs;
//}
//template <typename T>
//inline bool operator!=(ArrayCRef<T> lhs, ArrayRef<T> rhs) {
//  return rhs != lhs;
//}

//template <typename T>
//class Array {
// public:
//  using Value = T;

//  Array() : values_() {}
//  ~Array() {}

//  Array(Array &&array) : values_(std::move(array.values_)) {}

//  Array &operator=(Array &&array) {
//    values_ = std::move(array.values_);
//    return *this;
//  }

//  operator ArrayCRef<Value>() const {
//    return ref();
//  }
//  operator ArrayRef<Value>() {
//    return ref();
//  }

//  ArrayCRef<Value> ref(Int offset = 0) const {
//    return ArrayCRef<Value>(values_.data() + offset, size() - offset);
//  }
//  ArrayCRef<Value> ref(Int offset, Int size) const {
//    return ArrayCRef<Value>(values_.data() + offset, size);
//  }

//  ArrayRef<Value> ref(Int offset = 0) {
//    return ArrayRef<Value>(values_.data() + offset, size() - offset);
//  }
//  ArrayRef<Value> ref(Int offset, Int size) {
//    return ArrayRef<Value>(values_.data() + offset, size);
//  }

//  Value get(Int i) const {
//    return values_[i];
//  }
//  void set(Int i, Value value) {
//    values_[i] = value;
//  }

//  Value &operator[](Int i) {
//    return values_[static_cast<size_t>(i)];
//  }
//  const Value &operator[](Int i) const {
//    return values_[static_cast<size_t>(i)];
//  }

//  Value &front() {
//    return values_.front();
//  }
//  const Value &front() const {
//    return values_.front();
//  }

//  Value &back() {
//    return values_.back();
//  }
//  const Value &back() const {
//    return values_.back();
//  }

//  Value *data() {
//    return values_.data();
//  }
//  const Value *data() const {
//    return values_.data();
//  }

//  Int size() const {
//    return static_cast<Int>(values_.size());
//  }
//  Int capacity() const {
//    return static_cast<Int>(values_.capacity());
//  }

//  bool reserve(Error *error, Int new_size) try {
//    values_.reserve(new_size);
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }

//  bool resize(Error *error, Int new_size) try {
//    values_.resize(new_size);
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }
//  bool resize(Error *error, Int new_size, const Value &value) try {
//    values_.resize(new_size, value);
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }

//  bool shrink_to_fit(Error *error) try {
//    values_.shrink_to_fit();
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }

//  void clear() {
//    values_.clear();
//  }

//  void erase(Int i) {
//    values_.erase(values_.begin() + i);
//  }

//  bool push_back(Error *error, const Value &value) try {
//    values_.push_back(value);
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }
//  bool push_back(Error *error, Value &&value) try {
//    values_.push_back(std::move(value));
//    return true;
//  } catch (...) {
//    ArrayErrorReporter::report_memory_error(error);
//    return false;
//  }
//  void pop_back() {
//    values_.pop_back();
//  }

//  void swap(Int i, Int j) {
//    std::swap(values_[i], values_[j]);
//  }

// private:
//  std::vector<Value> values_;
//};

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
