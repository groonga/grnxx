#ifndef GRNXX_ARRAY_PRIMARY_HPP
#define GRNXX_ARRAY_PRIMARY_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class ArrayErrorReporter {
 public:
  static void report_memory_error(Error *error);
};

template <typename T> class ArrayCRef;
template <typename T> class ArrayRef;
template <typename T> class Array;

template <typename T>
class ArrayCRef {
 public:
  using Value = T;

  ArrayCRef() = default;
  ArrayCRef(const ArrayCRef &) = default;

  ArrayCRef &operator=(const ArrayCRef &) = default;

  bool operator==(const ArrayCRef<Value> &rhs) const {
    return (values_ == rhs.values_) && (size_ == rhs.size_);
  }
  bool operator!=(const ArrayCRef<Value> &rhs) const {
    return (values_ != rhs.values_) || (size_ != rhs.size_);
  }

  ArrayCRef ref(Int offset = 0) const {
    return ArrayCRef(values_ + offset, size_ - offset);
  }
  ArrayCRef ref(Int offset, Int size) const {
    return ArrayCRef(values_ + offset, size);
  }

  const Value &get(Int i) const {
    return values_[i];
  }

  const Value &operator[](Int i) const {
    return values_[i];
  }

  Int size() const {
    return size_;
  }

 private:
  const Value *values_;
  Int size_;

  ArrayCRef(const Value *values, Int size) : values_(values), size_(size) {}

  friend class ArrayRef<Value>;
  friend class Array<Value>;
};

template <typename T>
class ArrayRef {
 public:
  using Value = T;

  ArrayRef() = default;
  ArrayRef(const ArrayRef &) = default;

  ArrayRef &operator=(const ArrayRef &) = default;

  operator ArrayCRef<Value>() const {
    return ref();
  }

  bool operator==(const ArrayCRef<Value> &rhs) const {
    return (values_ == rhs.values_) && (size_ == rhs.size_);
  }
  bool operator==(const ArrayRef<Value> &rhs) const {
    return (values_ == rhs.values_) && (size_ == rhs.size_);
  }

  bool operator!=(const ArrayCRef<Value> &rhs) const {
    return (values_ != rhs.values_) || (size_ != rhs.size_);
  }
  bool operator!=(const ArrayRef<Value> &rhs) const {
    return (values_ != rhs.values_) || (size_ != rhs.size_);
  }

  ArrayCRef<Value> ref(Int offset = 0) const {
    return ArrayCRef<Value>(values_ + offset, size_ - offset);
  }
  ArrayCRef<Value> ref(Int offset, Int size) const {
    return ArrayCRef<Value>(values_ + offset, size);
  }

  ArrayRef ref(Int offset = 0) {
    return ArrayRef(values_ + offset, size_ - offset);
  }
  ArrayRef ref(Int offset, Int size) {
    return ArrayRef(values_ + offset, size);
  }

  const Value &get(Int i) const {
    return values_[i];
  }
  void set(Int i, const Value &value) {
    values_[i] = value;
  }
  void set(Int i, Value &&value) {
    values_[i] = std::move(value);
  }

  Value &operator[](Int i) {
    return values_[i];
  }
  const Value &operator[](Int i) const {
    return values_[i];
  }

  Int size() const {
    return size_;
  }

  void swap(Int i, Int j) {
    Value temp = std::move(values_[i]);
    values_[i] = std::move(values_[j]);
    values_[j] = std::move(temp);
  }

 private:
  Value *values_;
  Int size_;

  ArrayRef(Value *values, Int size) : values_(values), size_(size) {}

  friend class Array<Value>;
};

template <typename T>
class Array {
 public:
  using Value = T;

  Array() : values_() {}
  ~Array() {}

  Array(Array &&array) : values_(std::move(array.values_)) {}

  Array &operator=(Array &&array) {
    values_ = std::move(array.values_);
    return *this;
  }

  operator ArrayCRef<Value>() const {
    return ref();
  }

  ArrayCRef<Value> ref(Int offset = 0) const {
    return ArrayCRef<Value>(values_.data() + offset, size() - offset);
  }
  ArrayCRef<Value> ref(Int offset, Int size) const {
    return ArrayCRef<Value>(values_.data() + offset, size);
  }

  ArrayRef<Value> ref(Int offset = 0) {
    return ArrayRef<Value>(values_.data() + offset, size() - offset);
  }
  ArrayRef<Value> ref(Int offset, Int size) {
    return ArrayRef<Value>(values_.data() + offset, size);
  }

  const Value &get(Int i) const {
    return values_[i];
  }
  void set(Int i, const Value &value) {
    values_[i] = value;
  }
  void set(Int i, Value &&value) {
    values_[i] = value;
  }

  Value &operator[](Int i) {
    return values_[static_cast<size_t>(i)];
  }
  const Value &operator[](Int i) const {
    return values_[static_cast<size_t>(i)];
  }

  Value &front() {
    return values_.front();
  }
  const Value &front() const {
    return values_.front();
  }

  Value &back() {
    return values_.back();
  }
  const Value &back() const {
    return values_.back();
  }

  Value *data() {
    return values_.data();
  }
  const Value *data() const {
    return values_.data();
  }

  Int size() const {
    return static_cast<Int>(values_.size());
  }
  Int capacity() const {
    return static_cast<Int>(values_.capacity());
  }

  bool reserve(Error *error, Int new_size) try {
    values_.reserve(new_size);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  bool resize(Error *error, Int new_size) try {
    values_.resize(new_size);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  bool resize(Error *error, Int new_size, const Value &value) try {
    values_.resize(new_size, value);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  bool shrink_to_fit(Error *error) try {
    values_.shrink_to_fit();
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }

  void clear() {
    values_.clear();
  }

  void erase(Int i) {
    values_.erase(values_.begin() + i);
  }

  bool push_back(Error *error, const Value &value) try {
    values_.push_back(value);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  bool push_back(Error *error, Value &&value) try {
    values_.push_back(std::move(value));
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  void pop_back() {
    values_.pop_back();
  }

  void swap(Int i, Int j) {
    std::swap(values_[i], values_[j]);
  }

 private:
  std::vector<Value> values_;
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_PRIMARY_HPP
