#ifndef GRNXX_TYPES_ARRAY_PRIMARY_HPP
#define GRNXX_TYPES_ARRAY_PRIMARY_HPP

#include "grnxx/types/base_types.hpp"
// TODO: Error will be provided in grnxx/types/error.hpp
#include "grnxx/types/forward.hpp"

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

  Array() : buf_(), size_(0), capacity_(0) {}
  ~Array() {}

  Array(Array &&array)
      : buf_(std::move(array.buf_)),
        size_(array.size_),
        capacity_(array.capacity_) {
    array.size_ = 0;
    array.capacity_ = 0;
  }

  Array &operator=(Array &&array) {
    buf_ = std::move(array.buf_);
    size_ = array.size_;
    capacity_ = array.capacity_;
    array.size_ = 0;
    array.capacity_ = 0;
    return *this;
  }

  operator ArrayCRef<Value>() const {
    return ref();
  }

  ArrayCRef<Value> ref(Int offset = 0) const {
    return ArrayCRef<Value>(data() + offset, size_ - offset);
  }
  ArrayCRef<Value> ref(Int offset, Int size) const {
    return ArrayCRef<Value>(data() + offset, size);
  }

  ArrayRef<Value> ref(Int offset = 0) {
    return ArrayRef<Value>(data() + offset, size_ - offset);
  }
  ArrayRef<Value> ref(Int offset, Int size) {
    return ArrayRef<Value>(data() + offset, size);
  }

  const Value &get(Int i) const {
    return data()[i];
  }
  void set(Int i, const Value &value) {
    data()[i] = value;
  }
  void set(Int i, Value &&value) {
    data()[i] = std::move(value);
  }

  Value &operator[](Int i) {
    return data()[i];
  }
  const Value &operator[](Int i) const {
    return data()[i];
  }

  Value &front() {
    return *data();
  }
  const Value &front() const {
    return *data();
  }

  Value &back() {
    return data()[size_ - 1];
  }
  const Value &back() const {
    return data()[size_ - 1];
  }

  Value *data() {
    return reinterpret_cast<Value *>(buf_.get());
  }
  const Value *data() const {
    return reinterpret_cast<const Value *>(buf_.get());
  }

  Int size() const {
    return size_;
  }
  Int capacity() const {
    return capacity_;
  }

  bool reserve(Error *error, Int new_size) {
    if (new_size <= capacity_) {
      return true;
    }
    return resize_buf(error, new_size);
  }

  bool resize(Error *error, Int new_size) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    for (Int i = new_size; i < size_; ++i) {
      data()[i].~Value();
    }
    for (Int i = size_; i < new_size; ++i) {
      new (&data()[i]) Value;
    }
    size_ = new_size;
    return true;
  }
  bool resize(Error *error, Int new_size, const Value &value) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    for (Int i = new_size; i < size_; ++i) {
      data()[i].~Value();
    }
    for (Int i = size_; i < new_size; ++i) {
      new (&data()[i]) Value(value);
    }
    size_ = new_size;
    return true;
  }

  void clear() {
    for (Int i = 0; i < size_; ++i) {
      data()[i].~Value();
    }
    size_ = 0;
  }

  void erase(Int i) {
    for (Int j = i + 1; j < size_; ++j) {
      data()[j - 1] = std::move(data()[j]);
    }
    data()[size_ - 1].~Value();
    --size_;
  }

  bool push_back(Error *error, const Value &value) {
    if (size_ == capacity_) {
      if (!resize_buf(error, size_ + 1)) {
        return false;
      }
    }
    new (&data()[size_]) Value(value);
    ++size_;
    return true;
  }
  bool push_back(Error *error, Value &&value) {
    if (size_ == capacity_) {
      if (!resize_buf(error, size_ + 1)) {
        return false;
      }
    }
    new (&data()[size_]) Value(std::move(value));
    ++size_;
    return true;
  }
  void pop_back() {
    data()[size_ - 1].~Value();
    --size_;
  }

  void swap(Int i, Int j) {
    Value temp = std::move(data()[i]);
    data()[i] = std::move(data()[j]);
    data()[j] = std::move(temp);
  }

 private:
  unique_ptr<char[]> buf_;
  Int size_;
  Int capacity_;

  // Assume new_size > capacity_.
  bool resize_buf(Error *error, Int new_size) {
    Int new_capacity = capacity_ * 2;
    if (new_size > new_capacity) {
      new_capacity = new_size;
    }
    Int new_buf_size = sizeof(Value) * new_capacity;
    unique_ptr<char[]> new_buf(new (nothrow) char[new_buf_size]);
    if (!new_buf) {
      ArrayErrorReporter::report_memory_error(error);
      return false;
    }
    Value *new_values = reinterpret_cast<Value *>(new_buf.get());
    for (Int i = 0; i < size_; ++i) {
      new (&new_values[i]) Value(std::move(data()[i]));
    }
    buf_ = std::move(new_buf);
    capacity_ = new_capacity;
    return true;
  }
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_ARRAY_PRIMARY_HPP
