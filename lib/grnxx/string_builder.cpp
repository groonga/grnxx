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
#include "grnxx/string_builder.hpp"

#include <cmath>

#include "grnxx/intrinsic.hpp"

namespace grnxx {

StringBuilder::StringBuilder(size_t size, StringBuilderFlags flags)
    : buf_((size != 0) ? (new (std::nothrow) char[size]) : nullptr),
      begin_(buf_.get()),
      end_(buf_ ? (begin_ + size - 1) : nullptr),
      ptr_(begin_),
      flags_(flags),
      failed_(!buf_) {
  if (buf_) {
    *ptr_ = '\0';
  }
}

bool StringBuilder::resize_buf(size_t size) {
  if (size < STRING_BUILDER_BUF_SIZE_MIN) {
    size = STRING_BUILDER_BUF_SIZE_MIN;
  } else {
    size = size_t(1) << (bit_scan_reverse(size - 1) + 1);
  }

  std::unique_ptr<char[]> new_buf(new (std::nothrow) char[size]);
  if (!new_buf) {
    return false;
  }

  const size_t length = ptr_ - begin_;
  std::memcpy(new_buf.get(), begin_, length);
  ptr_ = new_buf.get() + length;
  begin_ = new_buf.get();
  end_ = new_buf.get() + size - 1;
  buf_ = std::move(new_buf);
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

StringBuilder &operator<<(StringBuilder &builder, float value) {
  return builder << static_cast<double>(value);
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
      if (value > 0) {
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

StringBuilder &operator<<(StringBuilder &builder, const Bytes &bytes) {
  // TODO: StringBuilder should support const uint_8 *.
  return builder.append(reinterpret_cast<const char *>(bytes.ptr()),
                        bytes.size());
}

StringBuilder &operator<<(StringBuilder &builder,
                          const std::exception &exception) {
  return builder << "{ what = " << exception.what() << " }";
}

}  // namespace grnxx
