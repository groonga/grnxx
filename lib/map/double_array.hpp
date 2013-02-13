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
#ifndef GRNXX_MAP_DOUBLE_ARRAY_HPP
#define GRNXX_MAP_DOUBLE_ARRAY_HPP

#include "../map.hpp"

namespace grnxx {
namespace map {

enum DoubleArrayType : int32_t {
  DOUBLE_ARRAY_UNKNOWN = 0,
  DOUBLE_ARRAY_BASIC   = 1,
  DOUBLE_ARRAY_LARGE   = 2
};

struct DoubleArrayHeader {
  MapHeader map_header;
  DoubleArrayType type;

  DoubleArrayHeader();
};

class DoubleArray : public Map {
 public:
  ~DoubleArray();

  static DoubleArray *create(const MapOptions &options, io::Pool pool);
  static DoubleArray *open(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const;

  bool search(int64_t key_id, Slice *key);
  bool search(const Slice &key, int64_t *key_id);

  bool insert(const Slice &key, int64_t *key_id);

  bool remove(int64_t key_id);
  bool remove(const Slice &key);

  bool update(int64_t key_id, const Slice &dest_key);
  bool update(const Slice &src_key, const Slice &dest_key, int64_t *key_id);

 private:
  DoubleArray();
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_HPP
