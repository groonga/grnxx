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
#ifndef GRNXX_MAP_HPP
#define GRNXX_MAP_HPP

// Tentative version.

#include "io/pool.hpp"
#include "slice.hpp"

namespace grnxx {

enum MapType : int32_t {
  MAP_UNKNOWN      = 0,
  MAP_DOUBLE_ARRAY = 1
};

struct MapOptions {
  MapType type;

  MapOptions();
};

struct MapHeader {
  MapType type;

  MapHeader();
};

class Map {
 public:
  Map();
  virtual ~Map();

  static Map *create(const MapOptions &options, io::Pool pool);
  static Map *open(io::Pool pool, uint32_t block_id);

  virtual uint32_t block_id() const = 0;

  virtual bool search(int64_t key_id, Slice *key = nullptr) = 0;
  virtual bool search(const Slice &key, int64_t *key_id = nullptr) = 0;

  virtual bool insert(const Slice &key, int64_t *key_id = nullptr) = 0;

  virtual bool remove(int64_t key_id) = 0;
  virtual bool remove(const Slice &key) = 0;

  virtual bool update(int64_t key_id, const Slice &dest_key) = 0;
  virtual bool update(const Slice &src_key, const Slice &dest_key,
                      int64_t *key_id = nullptr) = 0;

  // TODO
};

}  // namespace grnxx

#endif  // GRNXX_MAP_HPP
