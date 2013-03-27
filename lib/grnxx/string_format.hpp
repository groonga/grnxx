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

#include "grnxx/basic.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {

enum StringFormatAlignmentAttribute {
  STRING_FORMAT_ALIGNMENT_LEFT,
  STRING_FORMAT_ALIGNMENT_RIGHT,
  STRING_FORMAT_ALIGNMENT_CENTER
};

template <typename T>
class StringFormatAlignment {
 public:
  constexpr StringFormatAlignment(const T &value, size_t width, int pad,
                                  StringFormatAlignmentAttribute attribute)
    : value_(value), width_(width), pad_(pad), attribute_(attribute) {}

  constexpr const T &value() {
    return value_;
  }
  constexpr size_t width() {
    return width_;
  }
  constexpr int pad() {
    return pad_;
  }
  constexpr StringFormatAlignmentAttribute attribute() {
    return attribute_;
  }

 private:
  const T &value_;
  size_t width_;
  int pad_;
  StringFormatAlignmentAttribute attribute_;
};

class StringFormat {
 public:
  StringFormat() = delete;
  ~StringFormat() = delete;

  StringFormat(const StringFormat &) = delete;
  StringFormat &operator=(const StringFormat &) = delete;

  template <typename T>
  static constexpr StringFormatAlignment<T> align_left(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_LEFT);
  }
  template <typename T>
  static constexpr StringFormatAlignment<T> align_right(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_RIGHT);
  }
  template <typename T>
  static constexpr StringFormatAlignment<T> align_center(
      const T &value, size_t width, int pad = ' ') {
    return align<T>(value, width, pad, STRING_FORMAT_ALIGNMENT_CENTER);
  }

  template <typename T>
  static constexpr StringFormatAlignment<T> align(
      const T &value, size_t width, int pad = ' ',
      StringFormatAlignmentAttribute attribute = STRING_FORMAT_ALIGNMENT_LEFT) {
    return StringFormatAlignment<T>(value, width, pad, attribute);
  }
};

template <typename T>
StringBuilder &operator<<(StringBuilder &builder,
                          const StringFormatAlignment<T> &alignment) {
  char local_buf[STRING_BUILDER_BUF_SIZE_MIN];
  const StringBuilderFlags local_flags =
      (alignment.width() >= sizeof(local_buf)) ?
          STRING_BUILDER_AUTO_RESIZE : StringBuilderFlags::none();

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