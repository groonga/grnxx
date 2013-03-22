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
#ifndef GRNXX_SLICE_HPP
#define GRNXX_SLICE_HPP

#include "grnxx/basic.hpp"

namespace grnxx {

class Slice {
 public:
  // Create an empty (zero-size) slice.
  Slice() : ptr_(nullptr), size_(0) {}
  // Create a slice that refers to a zero-terminated string.
  Slice(const char *str)
    : ptr_(reinterpret_cast<const uint8_t *>(str)),
      size_(std::strlen(str)) {}
  // Create a slice.
  Slice(const void *ptr, size_t size)
    : ptr_(static_cast<const uint8_t *>(ptr)),
      size_(size) {}

  // Return true iff "*this" is not empty.
  explicit operator bool() const {
    return size_ != 0;
  }

  // Make "*this" empty.
  void clear() {
    ptr_ = nullptr;
    size_ = 0;
  }

  // Create a slice for the first "n" bytes.
  Slice prefix(size_t n) const {
    return Slice(ptr_, n);
  }
  // Create a slice for the last "n" bytes.
  Slice suffix(size_t n) const {
    return Slice(ptr_ + size_ - n, n);
  }
  // Create a subslice.
  Slice subslice(size_t offset, size_t size) const {
    return Slice(ptr_ + offset, size);
  }

  // Ignore the first "n" bytes of "*this".
  void remove_prefix(size_t n) {
    ptr_ += n;
    size_ -= n;
  }
  // Ignore the last "n" bytes of "*this".
  void remove_suffix(size_t n) {
    size_ -= n;
  }

  // Compare "*this" and "s" and return a negative value if "*this" < "s",
  // zero if "*this" == "s", or a positive value otherwise (if "*this" > "s").
  int compare(const Slice &s) const {
    const size_t min_size = (size_ < s.size_) ? size_ : s.size_;
    int result = std::memcmp(ptr_, s.ptr_, min_size);
    if (result != 0) {
      return result;
    }
    return (size_ < s.size_) ? -1 : (size_ > s.size_);
  }
  // Compare "*this" and "s" and return a negative value if "*this" < "s",
  // zero if "*this" == "s", or a positive value otherwise (if "*this" > "s").
  // The starting "offset" bytes of "*this" and "s" are ignored. Note that too
  // large "offset" results in undefined behavior.
  int compare(const Slice &s, size_t offset) const {
    const size_t min_size = (size_ < s.size_) ? size_ : s.size_;
    int result = std::memcmp(ptr_ + offset, s.ptr_ + offset, min_size - offset);
    if (result != 0) {
      return result;
    }
    return (size_ < s.size_) ? -1 : (size_ > s.size_);
  }

  // Return true iff "s" is a prefix of "*this".
  bool starts_with(const Slice &s) const {
    return (size_ >= s.size_) && (std::memcmp(ptr_, s.ptr_, s.size_) == 0);
  }
  // Return true iff "s" is a suffix of "*this".
  bool ends_with(const Slice &s) const {
    return (size_ >= s.size_) &&
           (std::memcmp(ptr_ + size_ - s.size_, s.ptr_, s.size_) == 0);
  }

  // Return the "i"-th byte of "*this".
  uint8_t operator[](size_t i) const {
    return ptr_[i];
  }

  // Return the starting address of "*this".
  const void *address() const {
    return ptr_;
  }
  // Return a pointer that refers to the starting address of "*this".
  const uint8_t *ptr() const {
    return ptr_;
  }
  // Return the size of "*this".
  size_t size() const {
    return size_;
  }

 private:
  const uint8_t *ptr_;
  size_t size_;
};

inline bool operator==(const Slice &lhs, const Slice &rhs) {
  return (lhs.size() == rhs.size()) &&
         (std::memcmp(lhs.ptr(), rhs.ptr(), lhs.size()) == 0);
}

inline bool operator!=(const Slice &lhs, const Slice &rhs) {
  return !(lhs == rhs);
}

std::ostream &operator<<(std::ostream &stream, const Slice &s);

}  // namespace grnxx

#endif  // GRNXX_SLICE_HPP
