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
#include "grnxx/string.hpp"

#include <ostream>

#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {

StringImpl *StringImpl::create(const char *ptr, size_t length) {
  const size_t size = buf_offset() + length + 1;
  StringImpl * const new_impl =
      reinterpret_cast<StringImpl *>(new (std::nothrow) char[size]);
  if (!new_impl) {
    GRNXX_ERROR() << "memory allocation failed: size = " << size;
    GRNXX_THROW();
  }
  new_impl->length_ = length;
  new_impl->reference_count_ = 1;
  std::memcpy(new_impl->buf_, ptr, length);
  new_impl->buf_[length] = '\0';
  return new_impl;
}

String::String(const char *str) : impl_(StringImpl::default_instance()) {
  if (str) {
    const size_t length = std::strlen(str);
    if (length != 0) {
      impl_ = StringImpl::create(str, length);
    }
  }
}

String::String(const char *ptr, size_t length)
  : impl_(StringImpl::default_instance()) {
  if (ptr) {
    if (length != 0) {
      impl_ = StringImpl::create(ptr, length);
    }
  } else if (length != 0) {
    GRNXX_ERROR() << "invalid argument: ptr = " << ptr
                  << ", length = " << length;
    GRNXX_THROW();
  }
}

String &String::operator=(const char *str) {
  StringImpl *new_impl = StringImpl::default_instance();
  if (str) {
    const size_t length = std::strlen(str);
    if (length != 0) {
      new_impl = StringImpl::create(str, length);
    }
  }
  impl_->decrement_reference_count();
  impl_ = new_impl;
  return *this;
}

std::ostream &operator<<(std::ostream &stream, const String &str) {
  return stream.write(str.c_str(), str.length());
}

}  // namespace grnxx
