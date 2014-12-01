#ifndef GRNXX_ARRAY_HPP
#define GRNXX_ARRAY_HPP

#include <cstdlib>
#include <cstring>
#include <new>
#include <type_traits>
#include <utility>

namespace grnxx {

template <typename T>
class ArrayCRef {
 public:
  using Value = T;

  ArrayCRef() = default;
  ~ArrayCRef() = default;

  ArrayCRef(const ArrayCRef &) = default;
  ArrayCRef &operator=(const ArrayCRef &) & = default;

  // Create a reference to an array.
  ArrayCRef(const Value *values, size_t size) : values_(values), size_(size) {}

  // Create a reference to a part of "this".
  ArrayCRef cref(size_t offset = 0) const {
    return ArrayCRef(values_ + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayCRef cref(size_t offset, size_t size) const {
    return ArrayCRef(values_ + offset, size);
  }

  // Return a reference to the "i"-th value.
  const Value &get(size_t i) const {
    return values_[i];
  }

  // Return a reference to the "i"-th value.
  const Value &operator[](size_t i) const {
    return values_[i];
  }

  // Return a pointer to the contents.
  const Value *data() const {
    return values_;
  }

  // Return whether the array is empty or not.
  bool is_empty() const {
    return size_ == 0;
  }
  // Return the number of values.
  size_t size() const {
    return size_;
  }

 private:
  const Value *values_;
  size_t size_;
};

template <typename T>
class ArrayRef {
 public:
  using Value = T;

  ArrayRef() = default;
  ~ArrayRef() = default;

  ArrayRef(const ArrayRef &) = default;
  ArrayRef &operator=(const ArrayRef &) & = default;

  // Create a reference to an array.
  ArrayRef(Value *values, size_t size) : values_(values), size_(size) {}

  // Create a reference to "this".
  operator ArrayCRef<Value>() const {
    return cref();
  }

  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset = 0) const {
    return ArrayCRef<Value>(values_ + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset, size_t size) const {
    return ArrayCRef<Value>(values_ + offset, size);
  }

  // Create a reference to a part of "this".
  ArrayRef ref(size_t offset = 0) {
    return ArrayRef(values_ + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayRef ref(size_t offset, size_t size) {
    return ArrayRef(values_ + offset, size);
  }

  // Return the "i"-th value.
  const Value &get(size_t i) const {
    return values_[i];
  }
  // Set the "i"-th value.
  void set(size_t i, const Value &value) {
    values_[i] = value;
  }
  // Set the "i"-th value.
  void set(size_t i, Value &&value) {
    values_[i] = std::move(value);
  }

  // Return a reference to the "i"-th value.
  Value &operator[](size_t i) {
    return values_[i];
  }
  // Return a reference to the "i"-th value.
  const Value &operator[](size_t i) const {
    return values_[i];
  }

  // Return a pointer to the contents.
  const Value *data() const {
    return values_;
  }

  // Return whether the array is empty or not.
  bool is_empty() const {
    return size_ == 0;
  }
  // Return the number of values.
  size_t size() const {
    return size_;
  }

 private:
  Value *values_;
  size_t size_;
};

template <typename T, bool IS_TRIVIAL = std::is_trivial<T>::value>
class Array;

// The Array implementation for non-trivial types.
template <typename T>
class Array<T, false> {
 public:
  using Value = T;

  Array() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~Array() {
    std::free(buffer_);
  }

  Array(const Array &) = delete;
  Array &operator=(const Array &) & = delete;

  // Move the ownership of an array.
  Array(Array &&array)
      : buffer_(array.buffer_),
        size_(array.size_),
        capacity_(array.capacity_) {
    array.buffer_ = nullptr;
    array.size_ = 0;
    array.capacity_ = 0;
  }
  // Move the ownership of an array.
  Array &operator=(Array &&array) & {
    std::free(buffer_);
    buffer_ = array.buffer_;
    size_ = array.size_;
    capacity_ = array.capacity_;
    array.buffer_ = nullptr;
    array.size_ = 0;
    array.capacity_ = 0;
    return *this;
  }

  // Create a reference to "this".
  operator ArrayCRef<Value>() const {
    return cref();
  }

  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset = 0) const {
    return ArrayCRef<Value>(data() + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset, size_t size) const {
    return ArrayCRef<Value>(data() + offset, size);
  }

  // Create a reference to a part of "this".
  ArrayRef<Value> ref(size_t offset = 0) {
    return ArrayRef<Value>(buffer() + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayRef<Value> ref(size_t offset, size_t size) {
    return ArrayRef<Value>(buffer() + offset, size);
  }

  // Return a reference to the "i"-th value.
  const Value &get(size_t i) const {
    return data()[i];
  }
  // Set the "i"-th value.
  void set(size_t i, const Value &value) {
    buffer()[i] = value;
  }
  // Set the "i"-th value.
  void set(size_t i, Value &&value) {
    buffer()[i] = std::move(value);
  }

  // Return a reference to the "i"-th value.
  Value &operator[](size_t i) {
    return buffer()[i];
  }
  // Return a reference to the "i"-th value.
  const Value &operator[](size_t i) const {
    return data()[i];
  }

  // Return a reference to the first value.
  Value &front() {
    return *buffer();
  }
  // Return a reference to the first value.
  const Value &front() const {
    return *data();
  }

  // Return a reference to the last value.
  Value &back() {
    return buffer()[size_ - 1];
  }
  // Return a reference to the last value.
  const Value &back() const {
    return data()[size_ - 1];
  }

  // Return a pointer to the buffer.
  Value *buffer() {
    return static_cast<Value *>(buffer_);
  }
  // Return a pointer to the contents.
  const Value *data() const {
    return static_cast<const Value *>(buffer_);
  }

  // Return whether the array is empty or not.
  bool is_empty() const {
    return size_ == 0;
  }
  // Return the number of values.
  size_t size() const {
    return size_;
  }
  // Return the number of values can be stored in the buffer.
  size_t capacity() const {
    return capacity_;
  }

  // Reserve memory for at least "new_size" values.
  //
  // On failure, throws an exception.
  void reserve(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
  }

  // Resize "this".
  //
  // On failure, throws an exception.
  void resize(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    for (size_t i = new_size; i < size_; ++i) {
      buffer()[i].~Value();
    }
    for (size_t i = size_; i < new_size; ++i) {
      new (&buffer()[i]) Value;
    }
    size_ = new_size;
  }
  // Resize "this" and fill the new values with "value".
  //
  // On failure, throws an exception.
  void resize(size_t new_size, const Value &value) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    for (size_t i = new_size; i < size_; ++i) {
      buffer()[i].~Value();
    }
    for (size_t i = size_; i < new_size; ++i) {
      new (&buffer()[i]) Value(value);
    }
    size_ = new_size;
  }

  // Clear the contents.
  void clear() {
    for (size_t i = 0; i < size_; ++i) {
      buffer()[i].~Value();
    }
    size_ = 0;
  }

  // Remove the "i"-th value.
  void erase(size_t i) {
    for (size_t j = i + 1; j < size_; ++j) {
      buffer()[j - 1] = std::move(buffer()[j]);
    }
    buffer()[size_ - 1].~Value();
    --size_;
  }

  // Append "value" to the end.
  //
  // On failure, throws an exception.
  void push_back(const Value &value) {
    if (size_ == capacity_) {
      resize_buffer(size_ + 1);
    }
    new (&buffer()[size_]) Value(value);
    ++size_;
  }
  // Append "value" to the end.
  //
  // On failure, throws an exception.
  void push_back(Value &&value) {
    if (size_ == capacity_) {
      resize_buffer(size_ + 1);
    }
    new (&buffer()[size_]) Value(std::move(value));
    ++size_;
  }
  // Remove the last value.
  void pop_back() {
    buffer()[size_ - 1].~Value();
    --size_;
  }

 private:
  void *buffer_;
  size_t size_;
  size_t capacity_;

  // Resize the buffer for at least "new_size" values.
  //
  // Assumes that "new_size" is greater than "capacity_".
  void resize_buffer(size_t new_size) {
    size_t new_capacity = capacity_ * 2;
    if (new_size > new_capacity) {
      new_capacity = new_size;
    }
    size_t new_buffer_size = sizeof(Value) * new_capacity;
    Value *new_buffer = static_cast<Value *>(std::malloc(new_buffer_size));
    if (!new_buffer) {
      throw "Memory allocation failed";  // TODO
    }
    for (size_t i = 0; i < size_; ++i) {
      new (&new_buffer[i]) Value(std::move(buffer()[i]));
    }
    std::free(buffer_);
    buffer_ = new_buffer;
    capacity_ = new_capacity;
  }
};

// The Array implementation for trivial types.
template <typename T>
class Array<T, true> {
 public:
  using Value = T;

  Array() : buffer_(nullptr), size_(0), capacity_(0) {}
  ~Array() {
    std::free(buffer_);
  }

  Array(const Array &) = delete;
  Array &operator=(const Array &) & = delete;

  // Move the ownership of an array.
  Array(Array &&array)
      : buffer_(array.buffer_),
        size_(array.size_),
        capacity_(array.capacity_) {
    array.buffer_ = nullptr;
    array.size_ = 0;
    array.capacity_ = 0;
  }
  // Move the ownership of an array.
  Array &operator=(Array &&array) & {
    std::free(buffer_);
    buffer_ = array.buffer_;
    size_ = array.size_;
    capacity_ = array.capacity_;
    array.buffer_ = nullptr;
    array.size_ = 0;
    array.capacity_ = 0;
    return *this;
  }

  // Create a reference to "this".
  operator ArrayCRef<Value>() const {
    return cref();
  }

  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset = 0) const {
    return ArrayCRef<Value>(data() + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayCRef<Value> cref(size_t offset, size_t size) const {
    return ArrayCRef<Value>(data() + offset, size);
  }

  // Create a reference to a part of "this".
  ArrayRef<Value> ref(size_t offset = 0) {
    return ArrayRef<Value>(buffer() + offset, size_ - offset);
  }
  // Create a reference to a part of "this".
  ArrayRef<Value> ref(size_t offset, size_t size) {
    return ArrayRef<Value>(buffer() + offset, size);
  }

  // Return a reference to the "i"-th value.
  const Value &get(size_t i) const {
    return data()[i];
  }
  // Set the "i"-th value.
  void set(size_t i, const Value &value) {
    buffer()[i] = value;
  }

  // Return a reference to the "i"-th value.
  Value &operator[](size_t i) {
    return buffer()[i];
  }
  // Return a reference to the "i"-th value.
  const Value &operator[](size_t i) const {
    return data()[i];
  }

  // Return a reference to the first value.
  Value &front() {
    return *buffer();
  }
  // Return a reference to the first value.
  const Value &front() const {
    return *data();
  }

  // Return a reference to the last value.
  Value &back() {
    return buffer()[size_ - 1];
  }
  // Return a reference to the last value.
  const Value &back() const {
    return data()[size_ - 1];
  }

  // Return a pointer to the buffer.
  Value *buffer() {
    return static_cast<Value *>(buffer_);
  }
  // Return a pointer to the contents.
  const Value *data() const {
    return static_cast<const Value *>(buffer_);
  }

  // Return whether the array is empty or not.
  bool is_empty() const {
    return size_ == 0;
  }
  // Return the number of values.
  size_t size() const {
    return size_;
  }
  // Return the number of values can be stored in the buffer.
  size_t capacity() const {
    return capacity_;
  }

  // Reserve memory for at least "new_size" values.
  //
  // On failure, throws an exception.
  void reserve(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
  }

  // Resize "this".
  //
  // On failure, throws an exception.
  void resize(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    size_ = new_size;
  }
  // Resize "this" and fill the new values with "value".
  //
  // On failure, throws an exception.
  void resize(size_t new_size, const Value &value) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    for (size_t i = size_; i < new_size; ++i) {
      buffer()[i] = value;
    }
    size_ = new_size;
  }

  // Clear the contents.
  void clear() {
    size_ = 0;
  }

  // Remove the "i"-th value.
  void erase(size_t i) {
    std::memmove(&buffer()[i], &buffer()[i + 1],
                 sizeof(Value) * (size_ - i - 1));
    --size_;
  }

  // Append "value" to the end.
  //
  // On failure, throws an exception.
  void push_back(const Value &value) {
    if (size_ == capacity_) {
      resize_buffer(size_ + 1);
    }
    buffer()[size_] = value;
    ++size_;
  }
  // Remove the last value.
  void pop_back() {
    --size_;
  }

 private:
  void *buffer_;
  size_t size_;
  size_t capacity_;

  // Resize the buffer for at least "new_size" values.
  //
  // Assumes that "new_size" is greater than "capacity_".
  void resize_buffer(size_t new_size) {
    if (capacity_ != 0) {
      size_t new_capacity = capacity_ * 2;
      if (new_size > new_capacity) {
        new_capacity = new_size;
      }
      size_t new_buffer_size = sizeof(Value) * new_capacity;
      Value *new_buffer =
          static_cast<Value *>(std::realloc(buffer_, new_buffer_size));
      if (!new_buffer) {
        throw "Memory allocation failed";  // TODO
      }
      buffer_ = new_buffer;
      capacity_ = new_capacity;
    } else {
      size_t new_buffer_size = sizeof(Value) * new_size;
      Value *new_buffer = static_cast<Value *>(std::malloc(new_buffer_size));
      if (!new_buffer) {
        throw "Memory allocation failed";  // TODO
      }
      buffer_ = new_buffer;
      capacity_ = new_size;
    }
  }
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_HPP
