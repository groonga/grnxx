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
#include "grnxx/alpha/map/scan.hpp"

#include "grnxx/charset.hpp"

namespace grnxx {
namespace alpha {
namespace map {

Scan::Scan(Map<Slice> *map, const Slice &query, const Charset *charset)
    : MapScan<Slice>(),
      map_(map),
      query_(query),
      charset_(charset) {}

Scan::~Scan() {}

bool Scan::next() {
  this->offset_ += this->size_;
  while (this->offset_ < query_.size()) {
    const Slice query_left =
       query_.subslice(this->offset_, query_.size() - this->offset_);
    if (map_->find_longest_prefix_match(query_left,
                                        &this->key_id_, &this->key_)) {
      this->size_ = this->key_.size();
      return true;
    }
    // Move to the next character.
    if (charset_) {
      this->offset_ += charset_->get_char_size(query_left);
    } else {
      ++this->offset_;
    }
  }
  this->size_ = 0;
  return false;
}

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
