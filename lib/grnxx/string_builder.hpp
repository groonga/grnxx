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

#include <exception>
#include <memory>

#include "grnxx/bytes.hpp"
#include "grnxx/flags_impl.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class StringBuilder;
typedef FlagsImpl<StringBuilder> StringBuilderFlags;

// Use the default settings.
constexpr StringBuilderFlags STRING_BUILDER_DEFAULT     =
    StringBuilderFlags::define(0x00);
// Automatically resize the buffer.
constexpr StringBuilderFlags STRING_BUILDER_AUTO_RESIZE =
    StringBuilderFlags::define(0x01);
// Don't throw even if memory allocation fails.
constexpr StringBuilderFlags STRING_BUILDER_NOEXCEPT    =
    StringBuilderFlags::define(0x02);

class StringBuilder {
 public:
  // Create an empty StringBuilder.
  explicit StringBuilder(StringBuilderFlags flags = STRING_BUILDER_DEFAULT);
  // Allocate "size" bytes to the internal buffer.
  explicit StringBuilder(size_t size,
                         StringBuilderFlags flags = STRING_BUILDER_DEFAULT);
  // Use "buf" ("T" bytes) as the internal buffer.
  template <size_t T>
  explicit StringBuilder(char (&buf)[T],
                         StringBuilderFlags flags = STRING_BUILDER_DEFAULT)
      : buf_(),
        begin_(buf),
        end_(buf + T - 1),
        ptr_(buf),
        flags_(flags),
        failed_(false) {
    *ptr_ = '\0';
  }
  // Use "buf" ("size" bytes) as the internal buffer.
  StringBuilder(char *buf, size_t size,
                StringBuilderFlags flags = STRING_BUILDER_DEFAULT);
  ~StringBuilder();

  // TODO: To be removed if these are not used.
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

  // Return true iff the builder is appendable.
  explicit operator bool() const {
    return !failed_;
  }

  // Return "*this" for one-liners.
  StringBuilder &builder() {
    return *this;
  }

  // Append a character.
  StringBuilder &append(int byte);
  // Append a sequence of the same character.
  StringBuilder &append(int byte, size_t length);
  // Append a sequence of length characters.
  StringBuilder &append(const char *ptr, size_t length);

  // Resize the string.
  // Note that the contents of the extended part are undefined.
  StringBuilder &resize(size_t length);

  // Move the pointer to the beginning and clear the failure flag.
  void clear();

  // Return the "i"-th byte.
  const char &operator[](size_t i) const {
    return begin_[i];
  }
  // Return the "i"-th byte.
  char &operator[](size_t i) {
    return begin_[i];
  }

  // Return the string as a sequence of bytes.
  Bytes bytes() const {
    return Bytes(begin_, ptr_ - begin_);
  }
  // Return the address of the string.
  const char *c_str() const {
    return begin_ ? begin_ : "";
  }
  // Return the length of the string.
  size_t length() const {
    return ptr_ - begin_;
  }

 private:
  std::unique_ptr<char[]> buf_;
  char *begin_;
  char *end_;
  char *ptr_;
  StringBuilderFlags flags_;
  bool failed_;

  // Resize the internal buffer.
  bool auto_resize(size_t size);
  // Resize the internal buffer to greater than or equal to "size" bytes.
  bool resize_buf(size_t size);

  StringBuilder(const StringBuilder &) = delete;
  StringBuilder &operator=(const StringBuilder &) = delete;
};

// Append a character.
inline StringBuilder &operator<<(StringBuilder &builder, char value) {
  return builder.append(value);
}

// Append a signed integer.
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

// Append an unsigned integer.
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

// Append a floating point number.
StringBuilder &operator<<(StringBuilder &builder, double value);

inline StringBuilder &operator<<(StringBuilder &builder, float value) {
  return builder << static_cast<double>(value);
}

// Append a boolean value (true/false).
StringBuilder &operator<<(StringBuilder &builder, bool value);

// Append a pointer.
StringBuilder &operator<<(StringBuilder &builder, const void *value);

// Append a zero-terminated string.
StringBuilder &operator<<(StringBuilder &builder, const char *value);

// Append a sequence of bytes.
StringBuilder &operator<<(StringBuilder &builder, const Bytes &bytes);

// Append an exception.
StringBuilder &operator<<(StringBuilder &builder,
                          const std::exception &exception);

}  // namespace grnxx

#endif  // GRNXX_STRING_BUILDER_HPP
