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
#ifndef GRNXX_STRING_FORMAT_HPP
#define GRNXX_STRING_FORMAT_HPP

#include "basic.hpp"
#include "string_builder.hpp"

namespace grnxx {

enum StringFormatAlignmentAttribute {
  STRING_FORMAT_ALIGNMENT_LEFT,
  STRING_FORMAT_ALIGNMENT_RIGHT,
  STRING_FORMAT_ALIGNMENT_CENTER
};

template <typename T>
class StringFormatAlignment {
 public:
  StringFormatAlignment(const T &value, size_t width, int pad,
                        StringFormatAlignmentAttribute attribute)
    : value_(value), width_(width), pad_(pad), attribute_(attribute) {}

  const T &value() const {
    return value_;
  }
  size_t width() const {
    return width_;
  }
  int pad() const {
    return pad_;
  }
  StringFormatAlignmentAttribute attribute() const {
    return attribute_;
  }

 private:
  const T &value_;
  const size_t width_;
  const int pad_;
  const StringFormatAlignmentAttribute attribute_;
};

class StringFormat {
 public:
  template <typename T>
  static StringFormatAlignment<T> align_left(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_LEFT);
  }
  template <typename T>
  static StringFormatAlignment<T> align_right(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_RIGHT);
  }
  template <typename T>
  static StringFormatAlignment<T> align_center(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_CENTER);
  }

  template <typename T>
  static StringFormatAlignment<T> align(
      const T &value, size_t width, int pad = ' ',
      StringFormatAlignmentAttribute attribute = STRING_FORMAT_ALIGNMENT_LEFT) {
    return StringFormatAlignment<T>(value, width, pad, attribute);
  }

 private:
  StringFormat();
  ~StringFormat();

  StringFormat(const StringFormat &);
  StringFormat &operator=(const StringFormat &);
};

template <typename T>
StringBuilder &operator<<(StringBuilder &builder,
                          const StringFormatAlignment<T> &alignment) {
  char local_buf[STRING_BUILDER_BUF_SIZE_MIN];
  const StringBuilderFlags local_flags =
      (alignment.width() >= sizeof(local_buf)) ?
          STRING_BUILDER_AUTO_RESIZE : StringBuilderFlags();

  StringBuilder local_builder(local_buf, local_flags);
  local_builder << alignment.value();
  if (local_builder.length() >= alignment.width()) {
    return builder.append(local_builder.c_str(), alignment.width());
  }

  const size_t unused_size = alignment.width() - local_builder.length();
  switch (alignment.attribute()) {
    case STRING_FORMAT_ALIGNMENT_LEFT: {
      builder.append(local_builder.c_str(), local_builder.length());
      builder.append(alignment.pad(), unused_size);
      break;
    }
    case STRING_FORMAT_ALIGNMENT_RIGHT: {
      builder.append(alignment.pad(), unused_size);
      builder.append(local_builder.c_str(), local_builder.length());
      break;
    }
    case STRING_FORMAT_ALIGNMENT_CENTER: {
      const size_t offset = unused_size / 2;
      builder.append(alignment.pad(), offset);
      builder.append(local_builder.c_str(), local_builder.length());
      builder.append(alignment.pad(), unused_size - offset);
      break;
    }
  }
  return builder;
}

}  // namespace grnxx

#endif  // GRNXX_STRING_FORMAT_HPP
