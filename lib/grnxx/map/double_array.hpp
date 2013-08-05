/*
  Copyright (C) 2012-2013  Brazil, Inc.

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

#include "grnxx/features.hpp"

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/map.hpp"
#include "grnxx/map_cursor.hpp"
#include "grnxx/map_cursor_query.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

template <typename T> class KeyPool;

struct DoubleArrayHeader;
class DoubleArrayBlock;
class DoubleArrayNode;

template <typename T>
class DoubleArray {
 public:
  static Map<T> *create(Storage *storage, uint32_t storage_node_id,
                        const MapOptions &options = MapOptions());
  static Map<T> *open(Storage *storage, uint32_t storage_node_id);
};

template <>
class DoubleArray<Bytes> : public Map<Bytes> {
  using Header = DoubleArrayHeader;
  using Node = DoubleArrayNode;
  using Block = DoubleArrayBlock;

  using NodeArray    = Array<Node,     65536, 8192>;  // 42-bit
  using SiblingArray = Array<uint8_t, 262144, 4096>;  // 42-bit
  using BlockArray   = Array<Block,     8192, 1024>;  // 33-bit

  static constexpr uint64_t NODE_ARRAY_SIZE    = 1ULL << 42;
  static constexpr uint64_t SIBLING_ARRAY_SIZE = 1ULL << 42;
  static constexpr uint64_t BLOCK_ARRAY_SIZE   = 1ULL << 33;

 public:
  using Key = typename Map<Bytes>::Key;
  using KeyArg = typename Map<Bytes>::KeyArg;
  using Cursor = typename Map<Bytes>::Cursor;

  DoubleArray();
  ~DoubleArray();

  static DoubleArray *create(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &options = MapOptions());
  static DoubleArray *open(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const;
  MapType type() const;

  int64_t max_key_id() const;
  uint64_t num_keys() const;

  bool get(int64_t key_id, Key *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id = nullptr);

  void truncate();

  bool find_longest_prefix_match(KeyArg query,
                                 int64_t *key_id = nullptr,
                                 Key *key = nullptr);

//  Cursor *create_cursor(
//      MapCursorAllKeys<Bytes> query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyIDRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<NodeArray> nodes_;
  std::unique_ptr<SiblingArray> siblings_;
  std::unique_ptr<BlockArray> blocks_;
  std::unique_ptr<KeyPool<Bytes>> pool_;

  void create_map(Storage *storage, uint32_t storage_node_id,
                  const MapOptions &options);
  void open_map(Storage *storage, uint32_t storage_node_id);

  bool replace_key(int64_t key_id, KeyArg src_key, KeyArg dest_key);

  bool find_leaf(KeyArg key, Node **leaf_node, uint64_t *leaf_key_pos);
  bool insert_leaf(KeyArg key, Node *node, uint64_t key_pos, Node **leaf_node);

  Node *insert_node(Node *node, uint64_t label);
  Node *separate(Node *node, uint64_t labels[2]);

  void resolve(Node *node, uint64_t label);
  void migrate_nodes(Node *node, uint64_t dest_offset,
                     const uint64_t *labels, uint64_t num_labels);

  uint64_t find_offset(const uint64_t *labels, uint64_t num_labels);

  Node *reserve_node(uint64_t node_id);
  Block *reserve_block(uint64_t block_id);

  void update_block_level(uint64_t block_id, Block *block, uint64_t level);
  void set_block_level(uint64_t block_id, Block *block, uint64_t level);
  void unset_block_level(uint64_t block_id, Block *block);
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_HPP
