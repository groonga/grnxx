#ifndef GRNXX_TYPES_STRING_HPP
#define GRNXX_TYPES_STRING_HPP

#include <cstring>

#include "grnxx/types/base_types.hpp"

namespace grnxx {

// Reference to a byte string.
class StringCRef {
 public:
  // The default constructor does nothing.
  StringCRef() = default;
  // Refer to a zero-terminated string.
  StringCRef(const char *arg) : data_(arg), size_(std::strlen(arg)) {}
  // Refer to an arbitrary byte string.
  StringCRef(const char *data, Int size) : data_(data), size_(size) {}

  // Return the "i"-th byte.
  const char &operator[](Int i) const {
    return data_[i];
  }

  // Return the address.
  const char *data() const {
    return data_;
  }
  // Return the number of bytes.
  Int size() const {
    return size_;
  }

  // Compare a strings.
  bool operator==(const StringCRef &arg) const {
    return (size_ == arg.size_) && (std::memcmp(data_, arg.data_, size_) == 0);
  }
  bool operator!=(const StringCRef &arg) const {
    return (size_ != arg.size_) || (std::memcmp(data_, arg.data_, size_) != 0);
  }
  bool operator<(const StringCRef &arg) const {
    Int min_size = size_ < arg.size_ ? size_ : arg.size_;
    int result = std::memcmp(data_, arg.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ < arg.size_));
  }
  bool operator>(const StringCRef &arg) const {
    Int min_size = size_ < arg.size_ ? size_ : arg.size_;
    int result = std::memcmp(data_, arg.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ > arg.size_));
  }
  bool operator<=(const StringCRef &arg) const {
    Int min_size = size_ < arg.size_ ? size_ : arg.size_;
    int result = std::memcmp(data_, arg.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ <= arg.size_));
  }
  bool operator>=(const StringCRef &arg) const {
    Int min_size = size_ < arg.size_ ? size_ : arg.size_;
    int result = std::memcmp(data_, arg.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ >= arg.size_));
  }

  // Compare a string with a zero-terminated string.
  bool operator==(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if ((arg[i] == '\0') || (data_[i] != arg[i])) {
        return false;
      }
    }
    return arg[size_] == '\0';
  }
  bool operator!=(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if ((arg[i] == '\0') || (data_[i] != arg[i])) {
        return true;
      }
    }
    return arg[size_] != '\0';
  }
  bool operator<(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if (arg[i] == '\0') {
        return false;
      }
      if (data_[i] != arg[i]) {
        return static_cast<unsigned char>(data_[i]) <
               static_cast<unsigned char>(arg[i]);
      }
    }
    return arg[size_] != '\0';
  }
  bool operator>(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if (arg[i] == '\0') {
        return true;
      }
      if (data_[i] != arg[i]) {
        return static_cast<unsigned char>(data_[i]) >
               static_cast<unsigned char>(arg[i]);
      }
    }
    return false;
  }
  bool operator<=(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if (arg[i] == '\0') {
        return false;
      }
      if (data_[i] != arg[i]) {
        return static_cast<unsigned char>(data_[i]) <
               static_cast<unsigned char>(arg[i]);
      }
    }
    return true;
  }
  bool operator>=(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if (arg[i] == '\0') {
        return true;
      }
      if (data_[i] != arg[i]) {
        return static_cast<unsigned char>(data_[i]) >
               static_cast<unsigned char>(arg[i]);
      }
    }
    return arg[size_] == '\0';
  }

  // Return true if "*this" starts with "arg".
  bool starts_with(const StringCRef &arg) const {
    if (size_ < arg.size_) {
      return false;
    }
    return std::memcmp(data_, arg.data_, arg.size_) == 0;
  }
  bool starts_with(const char *arg) const {
    for (Int i = 0; i < size_; ++i) {
      if (arg[i] == '\0') {
        return true;
      } else if (data_[i] != arg[i]) {
        return false;
      }
    }
    return arg[size_] == '\0';
  }

  // Return true if "*this" ends with "arg".
  bool ends_with(const StringCRef &arg) const {
    if (size_ < arg.size_) {
      return false;
    }
    return std::memcmp(data_ + size_ - arg.size_, arg.data_, arg.size_) == 0;
  }
  bool ends_with(const char *arg) const {
    return ends_with(StringCRef(arg));
  }

 private:
  const char *data_;
  Int size_;
};

// Compare a null-terminated string with a string.
inline bool operator==(const char *lhs, const StringCRef &rhs) {
  return rhs == lhs;
}
inline bool operator!=(const char *lhs, const StringCRef &rhs) {
  return rhs != lhs;
}
inline bool operator<(const char *lhs, const StringCRef &rhs) {
  return rhs > lhs;
}
inline bool operator>(const char *lhs, const StringCRef &rhs) {
  return rhs < lhs;
}
inline bool operator<=(const char *lhs, const StringCRef &rhs) {
  return rhs >= lhs;
}
inline bool operator>=(const char *lhs, const StringCRef &rhs) {
  return rhs <= lhs;
}

using Text = StringCRef;

class String {
 public:
  String() : buf_(), size_(0), capacity_(0) {}
  ~String() {}

  String(String &&arg)
      : buf_(std::move(arg.buf_)),
        size_(arg.size_),
        capacity_(arg.capacity_) {
    arg.size_ = 0;
    arg.capacity_ = 0;
  }

  String &operator=(String &&arg) {
    buf_ = std::move(arg.buf_);
    size_ = arg.size_;
    capacity_ = arg.capacity_;
    arg.size_ = 0;
    arg.capacity_ = 0;
    return *this;
  }

  operator StringCRef() const {
    return ref();
  }

  StringCRef ref(Int offset = 0) const {
    return StringCRef(buf_.get() + offset, size_ - offset);
  }
  StringCRef ref(Int offset, Int size) const {
    return StringCRef(buf_.get() + offset, size);
  }

  char &operator[](Int i) {
    return buf_[i];
  }
  const char &operator[](Int i) const {
    return buf_[i];
  }

  char &front() {
    return buf_[0];
  }
  const char &front() const {
    return buf_[0];
  }

  char &back() {
    return buf_[size_ - 1];
  }
  const char &back() const {
    return buf_[size_ - 1];
  }

  char *data() {
    return buf_.get();
  }
  const char *data() const {
    return buf_.get();
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

  bool assign(Error *error, const StringCRef &arg) {
    if (arg.size() > capacity_) {
      if (!resize_buf(error, arg.size())) {
        return false;
      }
    }
    std::memcpy(buf_.get(), arg.data(), arg.size());
    size_ = arg.size();
    return true;
  }
  bool assign(Error *error, const char *data, Int size) {
    return assign(error, StringCRef(data, size));
  }

  bool resize(Error *error, Int new_size) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    size_ = new_size;
    return true;
  }
  bool resize(Error *error, Int new_size, char value) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    if (new_size > size_) {
      std::memset(buf_.get() + size_, value, new_size - size_);
    }
    size_ = new_size;
    return true;
  }

  void clear() {
    size_ = 0;
  }

  bool push_back(Error *error, char value) {
    if (size_ == capacity_) {
      if (!resize_buf(error, size_ + 1)) {
        return false;
      }
    }
    buf_[size_] = value;
    ++size_;
    return true;
  }
  void pop_back() {
    --size_;
  }

  bool append(Error *error, const StringCRef &arg) {
    if ((size_ + arg.size()) > capacity_) {
      if ((arg.data() >= buf_.get()) && (arg.data() < (buf_.get() + size_))) {
        // Note that "arg" will be deleted in resize_buf() if "arg" is a part
        // of "*this".
        return append_overlap(error, arg);
      } else if (!resize_buf(error, (size_ + arg.size()))) {
        return false;
      }
    }
    std::memcpy(buf_.get() + size_, arg.data(), arg.size());
    size_ += arg.size();
    return true;
  }
  bool append(Error *error, const char *data, Int size) {
    return append(error, StringCRef(data, size));
  }

  void swap(Int i, Int j) {
    char temp = buf_[i];
    buf_[i] = buf_[j];
    buf_[j] = temp;
  }

  // Compare a strings.
  bool operator==(const StringCRef &arg) const {
    return ref() == arg;
  }
  bool operator!=(const StringCRef &arg) const {
    return ref() != arg;
  }
  bool operator<(const StringCRef &arg) const {
    return ref() < arg;
  }
  bool operator>(const StringCRef &arg) const {
    return ref() > arg;
  }
  bool operator<=(const StringCRef &arg) const {
    return ref() <= arg;
  }
  bool operator>=(const StringCRef &arg) const {
    return ref() >= arg;
  }

  // Compare a string with a zero-terminated string.
  bool operator==(const char *arg) const {
    return ref() == arg;
  }
  bool operator!=(const char *arg) const {
    return ref() != arg;
  }
  bool operator<(const char *arg) const {
    return ref() < arg;
  }
  bool operator>(const char *arg) const {
    return ref() > arg;
  }
  bool operator<=(const char *arg) const {
    return ref() <= arg;
  }
  bool operator>=(const char *arg) const {
    return ref() >= arg;
  }

  // Return true if "*this" starts with "arg".
  bool starts_with(const StringCRef &arg) const {
    return ref().starts_with(arg);
  }
  bool starts_with(const char *arg) const {
    return ref().starts_with(arg);
  }

  // Return true if "*this" ends with "arg".
  bool ends_with(const StringCRef &arg) const {
    return ref().ends_with(arg);
  }
  bool ends_with(const char *arg) const {
    return ref().ends_with(arg);
  }

 private:
  unique_ptr<char[]> buf_;
  Int size_;
  Int capacity_;

  // Assume new_size > capacity_.
  bool resize_buf(Error *error, Int new_size);
  // Resize the internal buffer and append a part of "*this".
  bool append_overlap(Error *error, const StringCRef &arg);
};

// Compare a null-terminated string with a string.
inline bool operator==(const char *lhs, const String &rhs) {
  return rhs == lhs;
}
inline bool operator!=(const char *lhs, const String &rhs) {
  return rhs != lhs;
}
inline bool operator<(const char *lhs, const String &rhs) {
  return rhs > lhs;
}
inline bool operator>(const char *lhs, const String &rhs) {
  return rhs < lhs;
}
inline bool operator<=(const char *lhs, const String &rhs) {
  return rhs >= lhs;
}
inline bool operator>=(const char *lhs, const String &rhs) {
  return rhs <= lhs;
}

}  // namespace grnxx

#endif  // GRNXX_TYPES_STRING_HPP
