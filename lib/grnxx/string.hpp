/*
  Copyright (C) 2012  Brazil, Inc.

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
#ifndef GRNXX_STRING_HPP
#define GRNXX_STRING_HPP

#include "grnxx/features.hpp"

#include <utility>

#include "grnxx/intrinsic.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class StringImpl {
 public:
  static StringImpl *create(const char *ptr, size_t length);

  // An instance is freed when its reference count becomes 0.
  void increment_reference_count() {
    if (this != default_instance()) {
      atomic_fetch_and_add(1, &reference_count_);
    }
  }
  void decrement_reference_count() {
    // The default instance must not be deleted.
    if (this != default_instance()) {
      if (atomic_fetch_and_add(-1, &reference_count_) == 1) {
        delete[] reinterpret_cast<char *>(this);
      }
    }
  }

  const char &operator[](size_t i) const {
    return buf_[i];
  }
  size_t length() const {
    return length_;
  }
  const char *c_str() const {
    return buf_;
  }

  // A pointer to a persistent instance (an empty string) is returned.
  static StringImpl *default_instance() {
    static StringImpl empty_string;
    return &empty_string;
  }

 private:
  size_t length_;
  volatile uint32_t reference_count_;
  char buf_[1];

  StringImpl() : length_(0), reference_count_(1), buf_{'\0'} {}

  static size_t buf_offset() {
    return static_cast<StringImpl *>(nullptr)->buf_
        - static_cast<char *>(nullptr);
  }
};

class String {
 public:
  String() : impl_(StringImpl::default_instance()) {}
  String(const char *str);
  String(const char *ptr, size_t length);
  ~String() {
    impl_->decrement_reference_count();
  }

  String(const String &str) : impl_(str.impl_) {
    impl_->increment_reference_count();
  }
  String &operator=(const String &str) {
    str.impl_->increment_reference_count();
    impl_->decrement_reference_count();
    impl_ = str.impl_;
    return *this;
  }

  // Note: a moved instance must not be used.
  String(String &&str) : impl_(str.impl_) {
    str.impl_ = StringImpl::default_instance();
  }
  String &operator=(String &&str) {
    impl_->decrement_reference_count();
    impl_ = str.impl_;
    str.impl_ = StringImpl::default_instance();
    return *this;
  }

  String &operator=(const char *str);

  explicit operator bool() const {
    return impl_->length() != 0;
  }

  bool contains(int byte) const {
    for (size_t i = 0; i < length(); ++i) {
      if ((*impl_)[i] == static_cast<char>(byte)) {
        return true;
      }
    }
    return false;
  }

  bool starts_with(const char *str) const {
    for (size_t i = 0; str[i] != '\0'; ++i) {
      if ((i >= length()) || (str[i] != (*impl_)[i])) {
        return false;
      }
    }
    return true;
  }
  bool ends_with(const char *str) const {
    size_t str_length = 0;
    while (str[str_length] != '\0') {
      if (str_length >= length()) {
        return false;
      }
      ++str_length;
    }
    const char *impl_ptr = c_str() + length() - str_length;
    for (size_t i = 0; i < str_length; ++i) {
      if (str[i] != impl_ptr[i]) {
        return false;
      }
    }
    return true;
  }

  const char &operator[](size_t i) const {
    return (*impl_)[i];
  }
  const char *c_str() const {
    return impl_->c_str();
  }
  size_t length() const {
    return impl_->length();
  }

  void swap(String &rhs) {
    using std::swap;
    swap(impl_, rhs.impl_);
  }

 private:
  StringImpl *impl_;

  // Copyable.
};

inline bool operator==(const String &lhs, const String &rhs) {
  if (lhs.c_str() == rhs.c_str()) {
    return true;
  }
  if (lhs.length() != rhs.length()) {
    return false;
  }
  for (size_t i = 0; i < lhs.length(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}

inline bool operator==(const String &lhs, const char *rhs) {
  if (!rhs) {
    return false;
  }
  for (size_t i = 0; i < lhs.length(); ++i, ++rhs) {
    if ((lhs[i] != *rhs) || (*rhs == '\0')) {
      return false;
    }
  }
  return *rhs == '\0';
}

inline bool operator==(const char *lhs, String &rhs) {
  return rhs == lhs;
}

inline bool operator!=(const String &lhs, const String &rhs) {
  return !(lhs == rhs);
}

inline bool operator!=(const String &lhs, const char *rhs) {
  return !(lhs == rhs);
}

inline bool operator!=(const char *lhs, const String &rhs) {
  return !(lhs == rhs);
}

inline void swap(String &lhs, String &rhs) {
  lhs.swap(rhs);
}

}  // namespace grnxx

#endif  // GRNXX_STRING_HPP
