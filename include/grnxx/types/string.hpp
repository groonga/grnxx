#ifndef GRNXX_TYPES_STRING_HPP
#define GRNXX_TYPES_STRING_HPP

#include <cstring>

#include "grnxx/types/base_types.hpp"

namespace grnxx {

// Reference to a byte string.
class String {
 public:
  // The default constructor does nothing.
  String() = default;

  // Refer to a zero-terminated string.
  String(const char *str) : data_(str), size_(str ? std::strlen(str) : 0) {}

  // Refer to an arbitrary byte string.
  String(const char *data, Int size) : data_(data), size_(size) {}

  const char &operator[](Int i) const {
    return data_[i];
  }

  const char *data() const {
    return data_;
  }
  Int size() const {
    return size_;
  }

 private:
  const char *data_;
  Int size_;
};

inline bool operator==(String lhs, String rhs) {
  return (lhs.size() == rhs.size()) &&
         (std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0);
}

inline bool operator!=(String lhs, String rhs) {
  return (lhs.size() != rhs.size()) ||
         (std::memcmp(lhs.data(), rhs.data(), lhs.size()) != 0);
}

inline bool operator<(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() < rhs.size()));
}

inline bool operator>(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() > rhs.size()));
}

inline bool operator<=(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() <= rhs.size()));
}

inline bool operator>=(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() >= rhs.size()));
}

using Text = String;

}  // namespace grnxx

#endif  // GRNXX_TYPES_STRING_HPP
