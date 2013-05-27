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
#ifndef GRNXX_MAP_SCANNER_HPP
#define GRNXX_MAP_SCANNER_HPP

#include "grnxx/features.hpp"

#include "grnxx/flags_impl.hpp"
#include "grnxx/traits.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Charset;
class Storage;

template <typename T>
class MapScanner {
 public:
  using Key = typename Traits<T>::Type;
  using KeyArg = typename Traits<T>::ArgumentType;

  MapScanner();
  virtual ~MapScanner();

  // Find the next key from the rest of the query and return true on success.
  virtual bool next() = 0;

  // Return the start position of the found key.
  uint64_t offset() const {
    return offset_;
  }
  // Return the size of the found key.
  uint64_t size() const {
    return size_;
  }
  // Return the ID of the found key.
  int64_t key_id() const {
    return key_id_;
  }
  // Return the found key.
  const Key &key() const {
    return key_;
  }

 protected:
  uint64_t offset_;
  uint64_t size_;
  int64_t key_id_;
  Key key_;
};

}  // namespace grnxx

#endif  // GRNXX_MAP_SCANNER_HPP
