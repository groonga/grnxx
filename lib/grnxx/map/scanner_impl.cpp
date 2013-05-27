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
#include "grnxx/map/scanner_impl.hpp"

#include <memory>
#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/charset.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"

// TODO: To be removed in future.
#include "grnxx/slice.hpp"

namespace grnxx {
namespace map {

template <typename T>
ScannerImpl<T>::ScannerImpl() : map_(), query_(), charset_() {}

template <typename T>
ScannerImpl<T>::~ScannerImpl() {}

template <typename T>
ScannerImpl<T> *ScannerImpl<T>::create(Map<T> *map, KeyArg query,
                                       const Charset *charset) {
  std::unique_ptr<ScannerImpl> scanner(new (std::nothrow) ScannerImpl);
  if (!scanner) {
    GRNXX_ERROR() << "new grnxx::map::ScannerImpl failed";
    return nullptr;
  }
  scanner->map_ = map;
  scanner->query_ = query;
  scanner->charset_ = charset;
  return scanner.release();
}

template <typename T>
bool ScannerImpl<T>::next() {
  this->offset_ += this->size_;
  while (this->offset_ < query_.size()) {
    const T rest = query_.except_prefix(this->offset_);
    if (map_->find_longest_prefix_match(rest, &this->key_id_, &this->key_)) {
      this->size_ = this->key_.size();
      return true;
    }
    // Move to the next character.
    if (charset_) {
      // TODO: Charset should support Bytes.
      this->offset_ += charset_->get_char_size(Slice(rest.ptr(), rest.size()));
    } else {
      ++this->offset_;
    }
  }
  this->size_ = 0;
  return false;
}

template class ScannerImpl<Bytes>;

}  // namespace map
}  // namespace grnxx
