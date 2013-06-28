/*
  Copyright (C) 2012-2013  Brazil, Inc.

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
#ifndef GRNXX_STRING_BUILDER_HPP
#define GRNXX_STRING_BUILDER_HPP

#include "grnxx/features.hpp"

#include <cstring>
#include <exception>
#include <memory>

#include "grnxx/bytes.hpp"
#include "grnxx/flags_impl.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

constexpr size_t STRING_BUILDER_BUF_SIZE_MIN = 64;

class StringBuilder;
typedef FlagsImpl<StringBuilder> StringBuilderFlags;

constexpr StringBuilderFlags STRING_BUILDER_AUTO_RESIZE =
    StringBuilderFlags::define(0x01);

class StringBuilder {
 public:
  explicit StringBuilder(StringBuilderFlags flags = StringBuilderFlags::none())
      : buf_(),
        begin_(nullptr),
        end_(nullptr),
        ptr_(nullptr),
        flags_(flags),
        failed_(false) {}

  explicit StringBuilder(size_t size,
                         StringBuilderFlags flags = StringBuilderFlags::none());

  template <size_t T>
  explicit StringBuilder(char (&buf)[T],
                         StringBuilderFlags flags = StringBuilderFlags::none())
      : buf_(),
        begin_(buf),
        end_(buf + T - 1),
        ptr_(buf),
        flags_(flags),
        failed_(false) {
    *ptr_ = '\0';
  }

  StringBuilder(char *buf, size_t size,
                StringBuilderFlags flags = StringBuilderFlags::none())
      : buf_(),
        begin_(buf),
        end_(buf + size - 1),
        ptr_(buf),
        flags_(flags),
        failed_(false) {
    *ptr_ = '\0';
  }

  ~StringBuilder() {}

  StringBuilder(StringBuilder &&rhs)
      : buf_(std::move(rhs.buf_)),
        begin_(std::move(rhs.begin_)),
        end_(std::move(rhs.end_)),
        ptr_(std::move(rhs.ptr_)),
        flags_(std::move(rhs.flags_)),
        failed_(std::move(rhs.failed_)) {}

  StringBuilder &operator=(StringBuilder &&rhs) {
    buf_ = std::move(rhs.buf_);
    begin_ = std::move(rhs.begin_);
    end_ = std::move(rhs.end_);
    ptr_ = std::move(rhs.ptr_);
    flags_ = std::move(rhs.flags_);
    failed_ = std::move(rhs.failed_);
    return *this;
  }

  explicit operator bool() const {
    return !failed_;
  }

  // For one-liners.
  StringBuilder &builder() {
    return *this;
  }

  // Append a character.
  StringBuilder &append(int byte) {
    if (failed_) {
      return *this;
    }

    if (ptr_ == end_) {
      if ((~flags_ & STRING_BUILDER_AUTO_RESIZE) ||
          !resize_buf(this->length() + 2)) {
        failed_ = true;
        return *this;
      }
    }

    *ptr_ = static_cast<char>(byte);
    *++ptr_ = '\0';
    return *this;
  }

  // Append a sequence of the same character.
  StringBuilder &append(int byte, size_t length) {
    if (failed_ || (length == 0)) {
      return *this;
    }

    const size_t size_left = end_ - ptr_;
    if (length > size_left) {
      if ((~flags_ & STRING_BUILDER_AUTO_RESIZE) ||
          !resize_buf(this->length() + length + 1)) {
        length = size_left;
        failed_ = true;
        if (length == 0) {
          return *this;
        }
      }
    }

    std::memset(ptr_, byte, length);
    ptr_ += length;
    *ptr_ = '\0';
    return *this;
  }

  // Append a sequence of length characters.
  StringBuilder &append(const char *ptr, size_t length) {
    if (failed_ || !ptr || (length == 0)) {
      return *this;
    }

    const size_t size_left = end_ - ptr_;
    if (length > size_left) {
      if ((~flags_ & STRING_BUILDER_AUTO_RESIZE) ||
          !resize_buf(this->length() + length + 1)) {
        length = size_left;
        failed_ = true;
      }
    }

    std::memcpy(ptr_, ptr, length);
    ptr_ += length;
    *ptr_ = '\0';
    return *this;
  }

  StringBuilder &resize(size_t length) {
    const size_t size_left = end_ - ptr_;
    if (length > size_left) {
      if ((~flags_ & STRING_BUILDER_AUTO_RESIZE) ||
          !resize_buf(length + 1)) {
        length = size_left;
        failed_ = true;
      }
    }
    ptr_ = begin_ + length;
    *ptr_ = '\0';
    return *this;
  }

  const char &operator[](size_t i) const {
    return begin_[i];
  }
  char &operator[](size_t i) {
    return begin_[i];
  }

  Bytes bytes() const {
    return Bytes(c_str(), length());
  }
  const char *c_str() const {
    return begin_ ? begin_ : "";
  }
  size_t length() const {
    return ptr_ - begin_;
  }

  void swap(StringBuilder &rhs) {
    using std::swap;
    swap(buf_, rhs.buf_);
    swap(begin_, rhs.begin_);
    swap(end_, rhs.end_);
    swap(ptr_, rhs.ptr_);
    swap(flags_, rhs.flags_);
    swap(failed_, rhs.failed_);
  }

 private:
  std::unique_ptr<char[]> buf_;
  char *begin_;
  char *end_;
  char *ptr_;
  StringBuilderFlags flags_;
  bool failed_;

  // Resize buf_ to greater than or equal to size bytes.
  bool resize_buf(size_t size);

  StringBuilder(const StringBuilder &);
  StringBuilder &operator=(const StringBuilder &);
};

inline void swap(StringBuilder &lhs, StringBuilder &rhs) {
  lhs.swap(rhs);
}

// Characters.
inline StringBuilder &operator<<(StringBuilder &builder, char value) {
  return builder.append(value);
}

// Signed integers.
StringBuilder &operator<<(StringBuilder &builder, long long value);

inline StringBuilder &operator<<(StringBuilder &builder, signed char value) {
  return builder << static_cast<long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, short value) {
  return builder << static_cast<long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, int value) {
  return builder << static_cast<long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, long value) {
  return builder << static_cast<long long>(value);
}

// Unsigned integers.
StringBuilder &operator<<(StringBuilder &builder, unsigned long long value);

inline StringBuilder &operator<<(StringBuilder &builder, unsigned char value) {
  return builder << static_cast<unsigned long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, unsigned short value) {
  return builder << static_cast<unsigned long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, unsigned int value) {
  return builder << static_cast<unsigned long long>(value);
}
inline StringBuilder &operator<<(StringBuilder &builder, unsigned long value) {
  return builder << static_cast<unsigned long long>(value);
}

// Floating point numbers.
StringBuilder &operator<<(StringBuilder &builder, float value);
StringBuilder &operator<<(StringBuilder &builder, double value);

// Boolean values (true/false).
inline StringBuilder &operator<<(StringBuilder &builder, bool value) {
  return value ? builder.append("true", 4) : builder.append("false", 5);
}

// Pointers.
StringBuilder &operator<<(StringBuilder &builder, const void *value);

// Zero-terminated strings.
inline StringBuilder &operator<<(StringBuilder &builder, const char *value) {
  if (!builder) {
    return builder;
  }
  if (!value) {
    return builder.append("nullptr", 7);
  }
  return builder.append(value, std::strlen(value));
}

StringBuilder &operator<<(StringBuilder &builder, const Bytes &bytes);

// Exceptions.
StringBuilder &operator<<(StringBuilder &builder,
                          const std::exception &exception);

}  // namespace grnxx

#endif  // GRNXX_STRING_BUILDER_HPP
