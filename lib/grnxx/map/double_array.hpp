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
#include "grnxx/map/double_array/block.hpp"
#include "grnxx/map/double_array/entry.hpp"
#include "grnxx/map/double_array/node.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Storage;

namespace map {

class BytesStore;

namespace double_array {

struct Header;

}  // namespace double_array

enum DoubleArrayResult {
  DOUBLE_ARRAY_FOUND,
  DOUBLE_ARRAY_NOT_FOUND,
  DOUBLE_ARRAY_INSERTED,
  DOUBLE_ARRAY_FAILED
};

template <typename T>
class DoubleArray {
 public:
  static Map<T> *create(Storage *storage, uint32_t storage_node_id,
                        const MapOptions &options = MapOptions());
  static Map<T> *open(Storage *storage, uint32_t storage_node_id);
};

template <>
class DoubleArray<Bytes> : public Map<Bytes> {
  using Node = double_array::Node;
  using Block = double_array::Block;
  using Entry = double_array::Entry;

  using NodeArray    = Array<Node,     65536, 8192, 8192>;  // 42-bit
  using SiblingArray = Array<uint8_t, 262144, 4096, 4096>;  // 42-bit
  using BlockArray   = Array<Block,     8192, 1024, 1024>;  // 33-bit
  using EntryArray   = Array<Entry,    65536, 4096, 4096>;  // 40-bit

 public:
  using Header = double_array::Header;
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

  bool truncate();

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
  std::unique_ptr<EntryArray> entries_;
  std::unique_ptr<BytesStore> store_;

  bool create_map(Storage *storage, uint32_t storage_node_id,
                  const MapOptions &options);
  bool open_map(Storage *storage, uint32_t storage_node_id);

  DoubleArrayResult get_key(int64_t key_id, Key *key);

  bool replace_key(int64_t key_id, KeyArg src_key, KeyArg dest_key);

  DoubleArrayResult find_leaf(KeyArg key, Node **leaf_node,
                              uint64_t *leaf_key_pos);
  DoubleArrayResult insert_leaf(KeyArg key, Node *node, uint64_t key_pos,
                                Node **leaf_node);

  bool insert_node(Node *node, uint64_t label, Node **dest_node);
  bool separate(Node *node, uint64_t labels[2], Node **dest_node);

  bool resolve(Node *node, uint64_t label);
  bool migrate_nodes(Node *node, uint64_t dest_offset,
                     const uint64_t *labels, uint64_t num_labels);

  bool find_offset(const uint64_t *labels, uint64_t num_labels,
                   uint64_t *found_offset);

  Node *reserve_node(uint64_t node_id);
  Block *reserve_block(uint64_t block_id);

  bool update_block_level(uint64_t block_id, Block *block, uint64_t level);
  bool set_block_level(uint64_t block_id, Block *block, uint64_t level);
  bool unset_block_level(uint64_t block_id, Block *block);
};

}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DOUBLE_ARRAY_HPP
