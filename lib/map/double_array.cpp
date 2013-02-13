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
#include "double_array.hpp"

//#include "../exception.hpp"
//#include "../lock.hpp"
//#include "../logger.hpp"

namespace grnxx {
namespace map {

DoubleArrayHeader::DoubleArrayHeader()
  : map_header(),
    type(DOUBLE_ARRAY_UNKNOWN) {
  map_header.type = MAP_DOUBLE_ARRAY;
}

DoubleArray::~DoubleArray() {
  // TODO
}

DoubleArray *DoubleArray::create(const MapOptions &, io::Pool pool) {
  // TODO
  return nullptr;
}

DoubleArray *DoubleArray::open(io::Pool pool, uint32_t block_id) {
  // TODO
  return nullptr;
}

uint32_t DoubleArray::block_id() const {
  // TODO
  return 0;
}

bool DoubleArray::search(int64_t key_id, Slice *key) {
  // TODO
  return false;
}

bool DoubleArray::search(const Slice &key, int64_t *key_id) {
  // TODO
  return false;
}

bool DoubleArray::insert(const Slice &key, int64_t *key_id) {
  // TODO
  return false;
}

bool DoubleArray::remove(int64_t key_id) {
  // TODO
  return false;
}

bool DoubleArray::remove(const Slice &key) {
  // TODO
  return false;
}

bool DoubleArray::update(int64_t key_id, const Slice &dest_key) {
  // TODO
  return false;
}

bool DoubleArray::update(const Slice &src_key, const Slice &dest_key,
                         int64_t *key_id) {
  // TODO
  return false;
}

DoubleArray::DoubleArray() {
  // TODO
}

}  // namespace map
}  // namespace grnxx
