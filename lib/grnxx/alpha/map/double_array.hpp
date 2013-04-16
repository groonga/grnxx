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
#ifndef GRNXX_ALPHA_MAP_DOUBLE_ARRAY_HPP
#define GRNXX_ALPHA_MAP_DOUBLE_ARRAY_HPP

#include "grnxx/alpha/map.hpp"
#include "grnxx/exception.hpp"

namespace grnxx {
namespace alpha {
namespace map {

// Forward declarations.
struct DoubleArrayHeaderForOthers;
class DoubleArrayNodeForOthers;
class DoubleArrayChunkForOthers;
class DoubleArrayEntryForOthers;

// Forward declarations.
struct DoubleArrayHeaderForSlice;
class DoubleArrayNodeForSlice;
class DoubleArrayChunkForSlice;
class DoubleArrayEntryForSlice;
class DoubleArrayKeyForSlice;

class DoubleArrayException : Exception {
 public:
  DoubleArrayException() noexcept : Exception() {}
  ~DoubleArrayException() noexcept {}

  DoubleArrayException(const DoubleArrayException &x) noexcept : Exception(x) {}
  DoubleArrayException &operator=(const DoubleArrayException &) noexcept {
    return *this;
  }

  const char *what() const noexcept {
    return "";
  }
};

template <typename T>
class DoubleArray : public Map<T> {
 public:
  typedef DoubleArrayHeaderForOthers DoubleArrayHeader;
  typedef DoubleArrayNodeForOthers DoubleArrayNode;
  typedef DoubleArrayChunkForOthers DoubleArrayChunk;
  typedef DoubleArrayEntryForOthers DoubleArrayEntry;

  ~DoubleArray();

  static DoubleArray<T> *create(io::Pool pool,
                                const MapOptions &options = MapOptions());
  static DoubleArray<T> *open(io::Pool pool, uint32_t block_id);

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

  // TODO
  void truncate();

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  DoubleArrayHeader *header_;
  DoubleArrayNode *nodes_;
  DoubleArrayChunk *chunks_;
  DoubleArrayEntry *entries_;
  T *keys_;
  bool initialized_;

  DoubleArray();

  void create_double_array(io::Pool pool, const MapOptions &options);
  void open_double_array(io::Pool pool, uint32_t block_id);

  void create_arrays();

  bool remove_key(T key);
  bool update_key(int32_t key_id, const Slice &src_key,
                  const Slice &dest_key);

  bool find_leaf(const uint8_t *key_buf, uint32_t &node_id, size_t &query_pos);
  bool insert_leaf(const Slice &key, uint32_t &node_id, size_t query_pos);

  uint32_t insert_node(uint32_t node_id, uint16_t label);

  uint32_t separate(const Slice &key, uint32_t node_id, size_t i);
  void resolve(uint32_t node_id, uint16_t label);
  void migrate_nodes(uint32_t node_id, uint32_t dest_offset,
                     const uint16_t *labels, uint16_t num_labels);

  uint32_t find_offset(const uint16_t *labels, uint16_t num_labels);

  void reserve_node(uint32_t node_id);
  void reserve_chunk(uint32_t chunk_id);

  void update_chunk_level(uint32_t chunk_id, uint32_t level);
  void set_chunk_level(uint32_t chunk_id, uint32_t level);
  void unset_chunk_level(uint32_t chunk_id);
};

template <>
class DoubleArray<Slice> : public Map<Slice> {
 public:
  typedef DoubleArrayHeaderForSlice DoubleArrayHeader;
  typedef DoubleArrayNodeForSlice DoubleArrayNode;
  typedef DoubleArrayChunkForSlice DoubleArrayChunk;
  typedef DoubleArrayEntryForSlice DoubleArrayEntry;
  typedef DoubleArrayKeyForSlice DoubleArrayKey;

  ~DoubleArray();

  static DoubleArray<Slice> *create(io::Pool pool,
                                    const MapOptions &options = MapOptions());
  static DoubleArray<Slice> *open(io::Pool pool, uint32_t block_id);

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

  bool find_longest_prefix_match(Slice query, int64_t *key_id = nullptr,
                                 Slice *key = nullptr);

  // TODO
  void truncate();

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  DoubleArrayHeader *header_;
  DoubleArrayNode *nodes_;
  DoubleArrayChunk *chunks_;
  DoubleArrayEntry *entries_;
  uint32_t *keys_;
  bool initialized_;

  DoubleArray();

  void create_double_array(io::Pool pool, const MapOptions &options);
  void open_double_array(io::Pool pool, uint32_t block_id);

  void create_arrays();

  const DoubleArrayKey &get_key(uint32_t key_pos) const {
    return *reinterpret_cast<const DoubleArrayKey *>(&keys_[key_pos]);
  }

  bool remove_key(const Slice &key);
  bool update_key(int32_t key_id, const Slice &src_key,
                  const Slice &dest_key);

  bool find_leaf(const Slice &key, uint32_t &node_id, size_t &query_pos);
  bool insert_leaf(const Slice &key, uint32_t &node_id, size_t query_pos);

  uint32_t insert_node(uint32_t node_id, uint16_t label);
  uint32_t append_key(const Slice &key, int32_t key_id);

  uint32_t separate(const Slice &key, uint32_t node_id, size_t i);
  void resolve(uint32_t node_id, uint16_t label);
  void migrate_nodes(uint32_t node_id, uint32_t dest_offset,
                     const uint16_t *labels, uint16_t num_labels);

  uint32_t find_offset(const uint16_t *labels, uint16_t num_labels);

  void reserve_node(uint32_t node_id);
  void reserve_chunk(uint32_t chunk_id);

  void update_chunk_level(uint32_t chunk_id, uint32_t level);
  void set_chunk_level(uint32_t chunk_id, uint32_t level);
  void unset_chunk_level(uint32_t chunk_id);
};

}  // namespace map
}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_DOUBLE_ARRAY_HPP
