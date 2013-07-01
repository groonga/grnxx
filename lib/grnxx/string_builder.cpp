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
#include "grnxx/string_builder.hpp"

#include <cmath>
#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include "grnxx/exception.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {
namespace {

constexpr size_t STRING_BUILDER_MIN_BUF_SIZE = 64;

}  // namespace

StringBuilder::StringBuilder(StringBuilderFlags flags)
    : buf_(),
      begin_(nullptr),
      end_(nullptr),
      ptr_(nullptr),
      flags_(flags),
      failed_(false) {}

StringBuilder::StringBuilder(size_t size, StringBuilderFlags flags)
    : buf_(),
      begin_(nullptr),
      end_(nullptr),
      ptr_(nullptr),
      flags_(flags),
      failed_(false) {
  if (size != 0) {
    buf_.reset(new (std::nothrow) char[size]);
    if (!buf_) {
      if (~flags_ & STRING_BUILDER_NOEXCEPT) {
        GRNXX_ERROR() << "new char[" << size << "] failed";
        throw MemoryError();
      }
      failed_ = true;
    } else {
      begin_ = buf_.get();
      end_ = begin_ + size - 1;
      ptr_ = begin_;
      *ptr_ = '\0';
    }
  }
}

StringBuilder::StringBuilder(char *buf, size_t size, StringBuilderFlags flags)
    : buf_(),
      begin_(buf),
      end_(buf + size - 1),
      ptr_(buf),
      flags_(flags),
      failed_(false) {
  *ptr_ = '\0';
}

StringBuilder::~StringBuilder() {}

StringBuilder &StringBuilder::append(int byte) {
  if (failed_) {
    return *this;
  }
  if (ptr_ == end_) {
    if (!auto_resize(length() + 2)) {
      return *this;
    }
  }
  *ptr_ = static_cast<char>(byte);
  *++ptr_ = '\0';
  return *this;
}

StringBuilder &StringBuilder::append(int byte, size_t length) {
  if (failed_ || (length == 0)) {
    return *this;
  }
  const size_t size_left = end_ - ptr_;
  if (length > size_left) {
    if (!auto_resize(this->length() + length + 1)) {
      length = size_left;
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

StringBuilder &StringBuilder::append(const char *ptr, size_t length) {
  if (failed_ || !ptr || (length == 0)) {
    return *this;
  }
  const size_t size_left = end_ - ptr_;
  if (length > size_left) {
    if (!auto_resize(this->length() + length + 1)) {
      length = size_left;
      if (length == 0) {
        return *this;
      }
    }
  }
  std::memcpy(ptr_, ptr, length);
  ptr_ += length;
  *ptr_ = '\0';
  return *this;
}

StringBuilder &StringBuilder::resize(size_t length) {
  const size_t size = end_ - ptr_;
  if (length > size) {
    if (!resize_buf(length + 1)) {
      return *this;
    }
  }
  ptr_ = begin_ + length;
  *ptr_ = '\0';
  return *this;
}

void StringBuilder::clear() {
  ptr_ = begin_;
  if (ptr_) {
    *ptr_ = '\0';
  }
  failed_ = false;
}

bool StringBuilder::auto_resize(size_t size) {
  if (~flags_ & STRING_BUILDER_AUTO_RESIZE) {
    failed_ = true;
    return false;
  }
  return resize_buf(size);
}

bool StringBuilder::resize_buf(size_t size) {
  if (size < STRING_BUILDER_MIN_BUF_SIZE) {
    size = STRING_BUILDER_MIN_BUF_SIZE;
  } else {
    size = size_t(2) << bit_scan_reverse(size - 1);
  }
  std::unique_ptr<char[]> new_buf(new (std::nothrow) char[size]);
  if (!new_buf) {
    if (~flags_ & STRING_BUILDER_NOEXCEPT) {
      GRNXX_ERROR() << "new char [" << size << "] failed";
      throw MemoryError();
    }
    failed_ = true;
    return false;
  }
  const size_t length = ptr_ - begin_;
  std::memcpy(new_buf.get(), begin_, length);
  buf_ = std::move(new_buf);
  begin_ = buf_.get();
  end_ = begin_ + size - 1;
  ptr_ = begin_ + length;
  return true;
}

StringBuilder &operator<<(StringBuilder &builder, long long value) {
  if (!builder) {
    return builder;
  }
  char buf[32];
  char *ptr = buf;
  char *left = ptr;
  if (value >= 0) {
    do {
      *ptr++ = static_cast<char>('0' + (value % 10));
      value /= 10;
    } while (value != 0);
  } else {
    *ptr++ = '-';
    ++left;
    do {
      // C++11 always rounds the result toward 0.
      *ptr++ = static_cast<char>('0' - (value % 10));
      value /= 10;
    } while (value != 0);
  }
  char *right = ptr - 1;
  while (left < right) {
    using std::swap;
    swap(*left++, *right--);
  }
  return builder.append(buf, ptr - buf);
}
StringBuilder &operator<<(StringBuilder &builder, unsigned long long value) {
  if (!builder) {
    return builder;
  }
  char buf[32];
  char *ptr = buf;
  do {
    *ptr++ = static_cast<char>('0' + (value % 10));
    value /= 10;
  } while (value != 0);
  char *left = buf;
  char *right = ptr - 1;
  while (left < right) {
    using std::swap;
    swap(*left++, *right--);
  }
  return builder.append(buf, ptr - buf);
}

StringBuilder &operator<<(StringBuilder &builder, double value) {
  if (!builder) {
    return builder;
  }
  switch (std::fpclassify(value)) {
    case FP_NORMAL:
    case FP_SUBNORMAL:
    case FP_ZERO: {
      break;
    }
    case FP_INFINITE: {
      if (value > 0.0) {
        return builder.append("inf", 3);
      } else {
        return builder.append("-inf", 4);
      }
    }
    case FP_NAN:
    default: {
      return builder.append("nan", 3);
    }
  }
  // The maximum value of double-precision floating point number (IEEE754)
  // is 1.797693134862316E+308.
  char buf[512];
  int length = std::snprintf(buf, sizeof(buf), "%f", value);
  if (length < 0) {
    return builder.append("n/a", 3);
  }
  if (static_cast<size_t>(length) >= sizeof(buf)) {
    length = sizeof(buf) - 1;
  }
  return builder.append(buf, length);
}

StringBuilder &operator<<(StringBuilder &builder, bool value) {
  return value ? builder.append("true", 4) : builder.append("false", 5);
}

StringBuilder &operator<<(StringBuilder &builder, const void *value) {
  if (!builder) {
    return builder;
  }
  if (!value) {
    return builder.append("nullptr", 7);
  }
  char buf[(sizeof(value) * 2) + 2];
  buf[0] = '0';
  buf[1] = 'x';
  uintptr_t address = reinterpret_cast<uintptr_t>(value);
  for (size_t i = 2; i < sizeof(buf); ++i) {
    const uintptr_t digit = address >> ((sizeof(value) * 8) - 4);
    buf[i] = static_cast<char>(
        (digit < 10) ? ('0' + digit) : ('A' + digit - 10));
    address <<= 4;
  }
  return builder.append(buf, sizeof(buf));
}

StringBuilder &operator<<(StringBuilder &builder, const char *value) {
  if (!builder) {
    return builder;
  }
  if (!value) {
    return builder.append("nullptr", 7);
  }
  return builder.append(value, std::strlen(value));
}

StringBuilder &operator<<(StringBuilder &builder, const Bytes &bytes) {
  return builder.append(reinterpret_cast<const char *>(bytes.data()),
                        bytes.size());
}

StringBuilder &operator<<(StringBuilder &builder,
                          const std::exception &exception) {
  return builder << "{ what = " << exception.what() << " }";
}

}  // namespace grnxx
