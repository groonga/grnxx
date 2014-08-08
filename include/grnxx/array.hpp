#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

class ArrayErrorReporter {
 public:
  static void report_memory_error(Error *error);
};

template <typename T>
class ArrayRef {
 public:
  ArrayRef() = default;
  ArrayRef(T *values, Int size) : values_(values), size_(size) {}
  ArrayRef(const ArrayRef &) = default;

  ArrayRef &operator=(const ArrayRef &) = default;

  ArrayRef ref(Int offset = 0) const {
    return ArrayRef(values_ + offset, size_ - offset);
  }
  ArrayRef ref(Int offset, Int size) const {
    return ArrayRef(values_ + offset, size);
  }

  T get(Int i) const {
    return values_[i];
  }
  void set(Int i, T value) {
    values_[i] = value;
  }

  T &operator[](Int i) {
    return values_[i];
  }
  const T &operator[](Int i) const {
    return values_[i];
  }

  Int size() const {
    return size_;
  }

 private:
  T *values_;
  Int size_;
};

template <typename T>
class Array {
 public:
  Array() : values_() {}
  ~Array() {}

  Array(Array &&array) : values_(std::move(array.values_)) {}

  Array &operator=(Array &&array) {
    values_ = std::move(array.values_);
    return *this;
  }

  ArrayRef<T> ref(Int offset = 0) const {
    return ArrayRef<T>(const_cast<T *>(data()) + offset, size() - offset);
  }
  ArrayRef<T> ref(Int offset, Int size) const {
    return ArrayRef<T>(const_cast<T *>(data()) + offset, size);
  }

  T get(Int i) const {
    return values_[i];
  }
  void set(Int i, T value) {
    values_[i] = value;
  }

  T &operator[](Int i) {
    return values_[static_cast<size_t>(i)];
  }
  const T &operator[](Int i) const {
    return values_[static_cast<size_t>(i)];
  }

  T &front() {
    return values_.front();
  }
  const T &front() const {
    return values_.front();
  }

  T &back() {
    return values_.back();
  }
  const T &back() const {
    return values_.back();
  }

  T *data() {
    return values_.data();
  }
  const T *data() const {
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
  bool resize(Error *error, Int new_size, const T &value) try {
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

  bool push_back(Error *error, const T &value) try {
    values_.push_back(value);
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  bool push_back(Error *error, T &&value) try {
    values_.push_back(std::move(value));
    return true;
  } catch (...) {
    ArrayErrorReporter::report_memory_error(error);
    return false;
  }
  void pop_back() {
    values_.pop_back();
  }

 private:
  std::vector<T> values_;
};

class BoolReference {
 public:
  BoolReference(uint64_t *block, uint64_t mask)
      : block_(block),
        mask_(mask) {}

  operator Bool() const {
    return (*block_ & mask_) != 0;
  }

  BoolReference operator=(Bool rhs) {
    if (rhs) {
      *block_ |= mask_;
    } else {
      *block_ &= ~mask_;
    }
    return *this;
  }
  BoolReference operator=(BoolReference rhs) {
    if (rhs) {
      *block_ |= mask_;
    } else {
      *block_ &= ~mask_;
    }
    return *this;
  }

 private:
  uint64_t *block_;
  uint64_t mask_;
};

inline bool operator==(const BoolReference &lhs, Bool rhs) {
  return static_cast<Bool>(lhs) == rhs;
}

inline bool operator!=(const BoolReference &lhs, Bool rhs) {
  return static_cast<Bool>(lhs) != rhs;
}

// ArrayRef<Bool> is specialized because a bit does not have its own unique
// address and thus a pointer type for Bool is not available.
template <>
class ArrayRef<Bool> {
 public:
  ArrayRef() = default;
  ArrayRef(uint64_t *blocks, Int offset, Int size)
      : blocks_(blocks + (offset / 64)),
        offset_(static_cast<uint64_t>(offset % 64)),
        size_(static_cast<uint64_t>(size)) {}
  ArrayRef(const ArrayRef &) = default;

  ArrayRef &operator=(const ArrayRef &) = default;

  ArrayRef ref(Int offset = 0) const {
    offset += static_cast<Int>(offset_);
    return ArrayRef(blocks_, offset, size() - offset);
  }
  ArrayRef ref(Int offset, Int size) const {
    offset += static_cast<Int>(offset_);
    return ArrayRef(blocks_, offset, size);
  }

  Bool get(Int i) const {
    i += static_cast<Int>(offset_);
    return (blocks_[i / 64] & (uint64_t(1) << (i % 64))) != 0;
  }
  void set(Int i, Bool value) {
    i += static_cast<Int>(offset_);
    if (value) {
      blocks_[i / 64] |= uint64_t(1) << (i % 64);
    } else {
      blocks_[i / 64] &= ~(uint64_t(1) << (i % 64));
    }
  }

  BoolReference operator[](Int i) {
    i += static_cast<Int>(offset_);
    return BoolReference(&blocks_[i / 64], uint64_t(1) << (i % 64));
  }
  Bool operator[](Int i) const {
    i += static_cast<Int>(offset_);
    return (blocks_[i / 64] & (uint64_t(1) << (i % 64))) != 0;
  }

  // TODO: For optimization.
//  uint64_t *blocks() const {
//    return blocks_;
//  }
//  Int offset() const {
//    return static_cast<Int>(offset_);
//  }

  Int size() const {
    return static_cast<Int>(size_);
  }

 private:
  uint64_t *blocks_;
  struct {
    uint64_t offset_:16;
    uint64_t size_:48;
  };
};

// Array<Bool> is specialized because a bit does not have its own unique
// address and thus a pointer type for Bool is not available.
template <>
class Array<Bool> {
 public:
  Array() : blocks_(), size_(0) {}
  ~Array() {}

  Array(Array &&array) : blocks_(std::move(array.blocks_)) {}

  Array &operator=(Array &&array) {
    blocks_ = std::move(array.blocks_);
    return *this;
  }

  ArrayRef<Bool> ref(Int offset = 0) const {
    return ArrayRef<Bool>(const_cast<uint64_t *>(blocks()),
                          offset, size() - offset);
  }
  ArrayRef<Bool> ref(Int offset, Int size) const {
    return ArrayRef<Bool>(const_cast<uint64_t *>(blocks()), offset, size);
  }

  Bool get(Int i) const {
    return (blocks_[i / 64] & (uint64_t(1) << (i % 64))) != 0;
  }
  void set(Int i, Bool value) {
    if (value) {
      blocks_[i / 64] |= uint64_t(1) << (i % 64);
    } else {
      blocks_[i / 64] &= ~(uint64_t(1) << (i % 64));
    }
  }

  BoolReference operator[](Int i) {
    return BoolReference(&blocks_[i / 64], uint64_t(1) << (i % 64));
  }
  Bool operator[](Int i) const {
    return (blocks_[i / 64] & (uint64_t(1) << (i % 64))) != 0;
  }

  BoolReference front() {
    return BoolReference(&blocks_[0], 1);
  }
  Bool front() const {
    return (blocks_[0] & 1) != 0;
  }

  BoolReference back() {
    return operator[](size_ - 1);
  }
  Bool back() const {
    return operator[](size_ - 1);
  }

  // Instead of data().
  uint64_t *blocks() {
    return blocks_.data();
  }
  const uint64_t *blocks() const {
    return blocks_.data();
  }

  Int size() const {
    return size_;
  }
  Int capacity() const {
    return blocks_.capacity() * 64;
  }

  bool reserve(Error *error, Int new_size) {
    return blocks_.reserve(error, (new_size + 63) / 64);
  }

  bool resize(Error *error, Int new_size) {
    if (!blocks_.resize(error, (new_size + 63) / 64)) {
      return false;
    }
    size_ = new_size;
    return true;
  }
  bool resize(Error *error, Int new_size, Bool value) {
    if (!blocks_.resize(error, (new_size + 63) / 64,
                        value ? ~uint64_t(0) : uint64_t(0))) {
      return false;
    }
    size_ = new_size;
    return true;
  }

  bool shrink_to_fit(Error *error) {
    return blocks_.shrink_to_fit(error);
  }

  void clear() {
    blocks_.clear();
    size_ = 0;
  }

//  void erase(Int i) {
//    values_.erase(values_.begin() + i);
//  }

  bool push_back(Error *error, Bool value) {
    if ((size_ % 64) == 0) {
      if (!blocks_.push_back(error, 0)) {
        return false;
      }
    }
    if (value) {
      blocks_[size_ / 64] |= uint64_t(1) << (size_ % 64);
    } else {
      blocks_[size_ / 64] &= ~(uint64_t(1) << (size_ % 64));
    }
    ++size_;
    return true;
  }
  void pop_back() {
    if ((size_ % 64) == 1) {
      blocks_.pop_back();
    }
    --size_;
  }

 private:
  Array<uint64_t> blocks_;
  Int size_;
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_HPP
