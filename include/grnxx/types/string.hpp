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

}  // namespace grnxx

#endif  // GRNXX_TYPES_STRING_HPP
