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

namespace grnxx {
namespace alpha {
namespace map {

struct DoubleArrayHeader;
struct DoubleArrayNode;
struct DoubleArrayChunk;
struct DoubleArrayEntry;
struct DoubleArrayKey;

template <typename T>
class DoubleArray : public Map<T> {
 public:
  ~DoubleArray();

  static DoubleArray<T> *create(io::Pool pool,
                                const MapOptions &options = MapOptions());
  static DoubleArray<T> *open(io::Pool pool, uint32_t block_id);

  static bool unlink(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const;
  MapType type() const;

  int64_t max_key_id() const;

  bool get(int64_t key_id, T *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, T dest_key);

  bool find(T key, int64_t *key_id = nullptr);
  bool insert(T key, int64_t *key_id = nullptr);
  bool remove(T key);
  bool update(T src_key, T dest_key, int64_t *key_id = nullptr);

  bool find_longest_prefix_match(T query, int64_t *key_id = nullptr,
                                 T *key = nullptr);

  // TODO
  void truncate();

  // TODO
//  DoubleArray<T> *defrag();

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

  // TODO
//  void defrag_trie(const TrieOptions &options, const Trie &trie,
//                   io::Pool pool);
//  void defrag_trie(const Trie &trie, uint32_t src, uint32_t dest);
};

}  // namespace map
}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_MAP_DOUBLE_ARRAY_HPP
