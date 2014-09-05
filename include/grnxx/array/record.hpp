#ifndef GRNXX_ARRAY_RECORD_HPP
#define GRNXX_ARRAY_RECORD_HPP

#include "grnxx/types.hpp"

namespace grnxx {

template <typename T> class ArrayCRef;
template <typename T> class ArrayRef;
template <typename T> class Array;

template <>
class ArrayCRef<Record> {
 public:
  using Value = Record;

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
  Int get_row_id(Int i) const {
    return values_[i].row_id;
  }
  Float get_score(Int i) const {
    return values_[i].score;
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

template <>
class ArrayRef<Record> {
 public:
  using Value = Record;

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
  Int get_row_id(Int i) const {
    return values_[i].row_id;
  }
  Float get_score(Int i) const {
    return values_[i].score;
  }
  void set(Int i, const Value &value) {
    values_[i] = value;
  }
  void set_row_id(Int i, Int row_id) {
    values_[i].row_id = row_id;
  }
  void set_score(Int i, Float score) {
    values_[i].score = score;
  }

  const Value &operator[](Int i) const {
    return values_[i];
  }

  Int size() const {
    return size_;
  }

  void swap(Int i, Int j) {
    Value temp = values_[i];
    values_[i] = values_[j];
    values_[j] = temp;
  }

 private:
  Value *values_;
  Int size_;

  ArrayRef(Value *values, Int size) : values_(values), size_(size) {}

  friend class Array<Value>;
};

template <>
class Array<Record> {
 public:
  using Value = Record;

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
  Int get_row_id(Int i) const {
    return data()[i].row_id;
  }
  Float get_score(Int i) const {
    return data()[i].score;
  }
  void set(Int i, const Value &value) {
    data()[i] = value;
  }
  void set_row_id(Int i, Int row_id) {
    data()[i].row_id = row_id;
  }
  void set_score(Int i, Float score) {
    data()[i].score = score;
  }

  const Value &operator[](Int i) const {
    return data()[i];
  }

  const Value &front() const {
    return *data();
  }

  const Value &back() const {
    return data()[size_ - 1];
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
  void pop_back() {
    data()[size_ - 1].~Value();
    --size_;
  }

  void swap(Int i, Int j) {
    Value temp = data()[i];
    data()[i] = data()[j];
    data()[j] = temp;
  }

 private:
  unique_ptr<char[]> buf_;
  Int size_;
  Int capacity_;

  Value *data() {
    return reinterpret_cast<Value *>(buf_.get());
  }
  const Value *data() const {
    return reinterpret_cast<const Value *>(buf_.get());
  }

  // Assume new_size > capacity_.
  bool resize_buf(Error *error, Int new_size);
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_RECORD_HPP
