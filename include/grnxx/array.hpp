#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

class ArrayHelper {
 public:
  static void report_memory_error(Error *error);
  static void report_empty_error(Error *error);
};

template <typename T>
class Array {
 public:
  Array() : values_() {}
  ~Array() {}

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
    ArrayHelper::report_memory_error(error);
    return false;
  }

  bool resize(Error *error, Int new_size) try {
    values_.resize(new_size);
    return true;
  } catch (...) {
    ArrayHelper::report_memory_error(error);
    return false;
  }
  bool resize(Error *error, Int new_size, const T &value) try {
    values_.resize(new_size, value);
    return true;
  } catch (...) {
    ArrayHelper::report_memory_error(error);
    return false;
  }

  bool shrink_to_fit(Error *error) try {
    values_.shrink_to_fit();
    return true;
  } catch (...) {
    ArrayHelper::report_memory_error(error);
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
    ArrayHelper::report_memory_error(error);
    return false;
  }
  bool push_back(Error *error, T &&value) try {
    values_.push_back(std::move(value));
    return true;
  } catch (...) {
    ArrayHelper::report_memory_error(error);
    return false;
  }
  bool pop_back(Error *error) {
    if (values_.size() == 0) {
      ArrayHelper::report_empty_error(error);
      return false;
    }
    values_.pop_back();
    return true;
  }

 private:
  std::vector<T> values_;
};

template class Array<Int>;

}  // namespace grnxx

#endif  // GRNXX_ARRAY_HPP
