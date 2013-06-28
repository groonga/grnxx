/*
  Copyright (C) 2013  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef GRNXX_BYTES_HPP
#define GRNXX_BYTES_HPP

#include "grnxx/features.hpp"

#include <cstring>

#include "grnxx/types.hpp"

namespace grnxx {

// A reference to a sequence of bytes.
class Bytes {
 public:
  // Trivial default constructor.
  Bytes() = default;
  // Create a reference to an empty (zero-size) sequence.
  Bytes(nullptr_t) : data_(nullptr), size_(0) {}
  // Create a reference to a zero-terminated string.
  Bytes(const char *str)
      : data_(reinterpret_cast<const uint8_t *>(str)),
        size_(std::strlen(str)) {}
  // Create a reference to a sequence of bytes.
  Bytes(const void *data, uint64_t size)
      : data_(static_cast<const uint8_t *>(data)),
        size_(size) {}

  // Return true iff the sequence is not empty.
  explicit operator bool() const {
    return size_ != 0;
  }

  // Skip the first "n" bytes and extract the subsequent "m" bytes.
  Bytes extract(uint64_t n, uint64_t m) const {
    return Bytes(data_ + n, m);
  }
  // Remove the first "n" bytes and the last "m" bytes.
  Bytes trim(uint64_t n, uint64_t m) const {
    return Bytes(data_ + n, size_ - n - m);
  }

  // Extract the first "n" bytes.
  Bytes prefix(uint64_t n) const {
    return Bytes(data_, n);
  }
  // Extract the last "n" bytes.
  Bytes suffix(uint64_t n) const {
    return Bytes(data_ + size_ - n, n);
  }

  // Remove the first "n" bytes.
  Bytes except_prefix(uint64_t n) const {
    return Bytes(data_ + n, size_ - n);
  }
  // Remove the last "n" bytes.
  Bytes except_suffix(uint64_t n) const {
    return Bytes(data_, size_ - n);
  }

  // Return true iff "*this" == "rhs".
  bool operator==(const Bytes &rhs) const {
    return (size_ == rhs.size_) && (std::memcmp(data_, rhs.data_, size_) == 0);
  }
  // Return true iff "*this" != "rhs".
  bool operator!=(const Bytes &rhs) const {
    return !operator==(rhs);
  }
  // Return true iff "*this" < "rhs".
  bool operator<(const Bytes &rhs) const {
    const uint64_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ < rhs.size_));
  }
  // Return true iff "*this" > "rhs".
  bool operator>(const Bytes &rhs) const {
    return rhs.operator<(*this);
  }
  // Return true iff "*this" <= "rhs".
  bool operator<=(const Bytes &rhs) const {
    return !operator>(rhs);
  }
  // Return true iff "*this" >= "rhs".
  bool operator>=(const Bytes &rhs) const {
    return !operator<(rhs);
  }

  // Compare "*this" and "bytes" and return a negative value
  // if "*this" < "bytes", zero if "*this" == "bytes", or a positive value
  // otherwise (if "*this" > "bytes").
  int compare(const Bytes &bytes) const {
    const uint64_t min_size = (size_ < bytes.size_) ? size_ : bytes.size_;
    int result = std::memcmp(data_, bytes.data_, min_size);
    if (result != 0) {
      return result;
    }
    return (size_ < bytes.size_) ? -1 : (size_ > bytes.size_);
  }

  // Return true iff "bytes" is a prefix of "*this".
  bool starts_with(const Bytes &bytes) const {
    return (size_ >= bytes.size_) && (prefix(bytes.size_) == bytes);
  }
  // Return true iff "bytes" is a suffix of "*this".
  bool ends_with(const Bytes &bytes) const {
    return (size_ >= bytes.size_) && (suffix(bytes.size_) == bytes);
  }

  // Return the "i"-th byte.
  uint8_t operator[](uint64_t i) const {
    return data_[i];
  }
  // Return the starting address.
  const void *address() const {
    return data_;
  }
  // Return a pointer to the sequence.
  const uint8_t *data() const {
    return data_;
  }
  // Return the number of bytes.
  uint64_t size() const {
    return size_;
  }

 private:
  const uint8_t *data_;
  uint64_t size_;
};

inline bool operator==(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) == rhs;
}
inline bool operator!=(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) != rhs;
}
inline bool operator<(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) < rhs;
}
inline bool operator>(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) > rhs;
}
inline bool operator<=(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) <= rhs;
}
inline bool operator>=(const char *lhs, const Bytes &rhs) {
  return Bytes(lhs) >= rhs;
}

}  // namespace

#endif  // GRNXX_BYTES_HPP
