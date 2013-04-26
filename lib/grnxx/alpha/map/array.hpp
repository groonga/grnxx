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
#ifndef GRNXX_ALPHA_MAP_ARRAY_HPP
#define GRNXX_ALPHA_MAP_ARRAY_HPP

#include "grnxx/alpha/map.hpp"
#include "grnxx/db/vector.hpp"
#include "grnxx/db/blob_vector.hpp"

namespace grnxx {
namespace alpha {
namespace map {

struct ArrayHeader {
  MapType map_type;
  uint32_t bits_block_id;
  uint32_t keys_block_id;
  int64_t max_key_id;
  int64_t next_key_id;
  uint64_t num_keys;

  ArrayHeader();
};

template <typename T>
class Array : public grnxx::alpha::Map<T> {
 public:
  ~Array();

  static Array *create(io::Pool pool,
                       const MapOptions &options = MapOptions());
  static Array *open(io::Pool pool, uint32_t block_id);

  static bool unlink(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const;
  MapType type() const;

  int64_t max_key_id() const;
  int64_t next_key_id() const;
  uint64_t num_keys() const;

  bool get(int64_t key_id, T *key = nullptr);
  bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
                T *next_key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, T dest_key);

  bool find(T key, int64_t *key_id = nullptr);
  bool insert(T key, int64_t *key_id = nullptr);
  bool remove(T key);
  bool update(T src_key, T dest_key, int64_t *key_id = nullptr);

  bool truncate();

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  ArrayHeader *header_;
  db::Vector<uint32_t> bits_;
  db::Vector<T> keys_;

  Array();

  bool get_bit(int64_t key_id) {
    return bits_[key_id / 32] & (1U << (key_id % 32));
  }
  void set_bit(int64_t key_id, bool bit) {
    if (bit) {
      bits_[key_id / 32] |= 1U << (key_id % 32);
    } else {
      bits_[key_id / 32] &= ~(1U << (key_id % 32));
    }
  }
};

template <>
class Array<Slice> : public grnxx::alpha::Map<Slice> {
 public:
  ~Array();

  static Array *create(io::Pool pool,
                       const MapOptions &options = MapOptions());
  static Array *open(io::Pool pool, uint32_t block_id);

  static bool unlink(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const;
  MapType type() const;

  int64_t max_key_id() const;
  int64_t next_key_id() const;
  uint64_t num_keys() const;

  bool get(int64_t key_id, Slice *key = nullptr);
  bool get_next(int64_t key_id, int64_t *next_key_id = nullptr,
                Slice *next_key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, Slice dest_key);

  bool find(Slice key, int64_t *key_id = nullptr);
  bool insert(Slice key, int64_t *key_id = nullptr);
  bool remove(Slice key);
  bool update(Slice src_key, Slice dest_key, int64_t *key_id = nullptr);

  bool truncate();

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  ArrayHeader *header_;
  db::BlobVector keys_;

  Array();
};

}  // namespace map
}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_ARRAY_HPP
