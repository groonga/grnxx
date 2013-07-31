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
#include "grnxx/map/double_array.hpp"

#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/common_header.hpp"
#include "grnxx/map/double_array/block.hpp"
#include "grnxx/map/double_array/node.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/key_pool.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::DoubleArray";

using double_array::BLOCK_MAX_FAILURE_COUNT;
using double_array::BLOCK_MAX_LEVEL;
using double_array::BLOCK_INVALID_ID;
using double_array::BLOCK_SIZE;
using double_array::BLOCK_MAX_COUNT;

using double_array::NODE_TERMINAL_LABEL;
using double_array::NODE_MAX_LABEL;
using double_array::NODE_INVALID_LABEL;
using double_array::NODE_INVALID_OFFSET;

constexpr uint64_t ROOT_NODE_ID = 0;

}  // namespace

struct DoubleArrayHeader {
  CommonHeader common_header;
  uint32_t nodes_storage_node_id;
  uint32_t siblings_storage_node_id;
  uint32_t blocks_storage_node_id;
  uint32_t pool_storage_node_id;
  uint64_t num_blocks;
  uint64_t num_phantoms;
  uint64_t num_zombies;
  uint64_t latest_blocks[BLOCK_MAX_LEVEL + 1];

  // Initialize the member variables.
  DoubleArrayHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

DoubleArrayHeader::DoubleArrayHeader()
    : common_header(FORMAT_STRING, MAP_DOUBLE_ARRAY),
      nodes_storage_node_id(STORAGE_INVALID_NODE_ID),
      siblings_storage_node_id(STORAGE_INVALID_NODE_ID),
      blocks_storage_node_id(STORAGE_INVALID_NODE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      num_blocks(0),
      num_phantoms(0),
      num_zombies(0),
      latest_blocks() {
  for (uint64_t i = 0; i <= BLOCK_MAX_LEVEL; ++i) {
    latest_blocks[i] = BLOCK_INVALID_ID;
  }
}

DoubleArrayHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
Map<T> *DoubleArray<T>::create(Storage *, uint32_t, const MapOptions &) {
  GRNXX_ERROR() << "unsupported type";
  throw LogicError();
}

template <typename T>
Map<T> *DoubleArray<T>::open(Storage *, uint32_t) {
  GRNXX_ERROR() << "unsupported type";
  throw LogicError();
}

template class DoubleArray<int8_t>;
template class DoubleArray<uint8_t>;
template class DoubleArray<int16_t>;
template class DoubleArray<uint16_t>;
template class DoubleArray<int32_t>;
template class DoubleArray<uint32_t>;
template class DoubleArray<int64_t>;
template class DoubleArray<uint64_t>;
template class DoubleArray<double>;
template class DoubleArray<GeoPoint>;

DoubleArray<Bytes>::DoubleArray()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      nodes_(),
      siblings_(),
      blocks_(),
      pool_() {}

DoubleArray<Bytes>::~DoubleArray() {}

DoubleArray<Bytes> *DoubleArray<Bytes>::create(Storage *storage,
                                               uint32_t storage_node_id,
                                               const MapOptions &options) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    throw MemoryError();
  }
  map->create_map(storage, storage_node_id, options);
  return map.release();
}

DoubleArray<Bytes> *DoubleArray<Bytes>::open(Storage *storage,
                                             uint32_t storage_node_id) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    throw MemoryError();
  }
  map->open_map(storage, storage_node_id);
  return map.release();
}

uint32_t DoubleArray<Bytes>::storage_node_id() const {
  return storage_node_id_;
}

MapType DoubleArray<Bytes>::type() const {
  return MAP_DOUBLE_ARRAY;
}

int64_t DoubleArray<Bytes>::max_key_id() const {
  return pool_->max_key_id();
}

uint64_t DoubleArray<Bytes>::num_keys() const {
  return pool_->num_keys();
}

bool DoubleArray<Bytes>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

bool DoubleArray<Bytes>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found.
    return false;
  }
  return remove(key);
}

bool DoubleArray<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  return replace(src_key, dest_key);
}

bool DoubleArray<Bytes>::find(KeyArg key, int64_t *key_id) {
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node.is_leaf()) {
      // Found.
      break;
    }
    node_id = node.offset() ^ key[key_pos];
    node = nodes_->get(node_id);
    if (node.label() != key[key_pos]) {
      // Not found.
      return false;
    }
  }
  if (!node.is_leaf()) {
    if (node.child() != NODE_TERMINAL_LABEL) {
      // Not found.
      return false;
    }
    node_id = node.offset() ^ NODE_TERMINAL_LABEL;
    node = nodes_->get(node_id);
    if (!node.is_leaf()) {
      // Not found.
      return false;
    }
  }
  Key stored_key;
  if (!pool_->get(node.key_id(), &stored_key)) {
    // Not found.
    return false;
  }
  if (key.except_prefix(key_pos) != stored_key.except_prefix(key_pos)) {
    // Not found.
    return false;
  }
  if (key_id) {
    *key_id = node.key_id();
  }
  return true;
}

bool DoubleArray<Bytes>::add(KeyArg key, int64_t *key_id) {
  Node *node;
  uint64_t key_pos;
  find_leaf(key, &node, &key_pos);
  if (!insert_leaf(key, node, key_pos, &node)) {
    if (key_id) {
      *key_id = node->key_id();
    }
    return false;
  }
  const int64_t next_key_id = pool_->add(key);
  node->set_key_id(next_key_id);
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

bool DoubleArray<Bytes>::remove(KeyArg key) {
  Node *node;
  uint64_t key_pos;
  if (!find_leaf(key, &node, &key_pos)) {
    // Not found.
    return false;
  }
  Key stored_key;
  if (!pool_->get(node->key_id(), &stored_key)) {
    // Not found.
    return false;
  }
  if (key.except_prefix(key_pos) != stored_key.except_prefix(key_pos)) {
    // Not found.
    return false;
  }
  pool_->unset(node->key_id());
  node->set_offset(NODE_INVALID_OFFSET);
  return true;
}

bool DoubleArray<Bytes>::replace(KeyArg src_key, KeyArg dest_key,
                                 int64_t *key_id) {
  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    // Not found.
    return false;
  }
  if (!replace_key(src_key_id, src_key, dest_key)) {
    // Found.
    return false;
  }
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

void DoubleArray<Bytes>::truncate() {
  // TODO: How to recycle nodes.
  Node * const node = &nodes_->get_value(ROOT_NODE_ID);
  node->set_child(NODE_INVALID_LABEL);
  node->set_offset(NODE_INVALID_OFFSET);
  pool_->truncate();
}

bool DoubleArray<Bytes>::find_longest_prefix_match(KeyArg query,
                                                   int64_t *key_id,
                                                   Key *key) {
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  uint64_t query_pos;
  for (query_pos = 0; query_pos < query.size(); ++query_pos) {
    if (node.is_leaf()) {
      Key stored_key;
      if (pool_->get(node.key_id(), &stored_key)) {
        if ((stored_key.size() <= query.size()) &&
            (stored_key.except_prefix(query_pos) ==
             query.prefix(stored_key.size()).except_prefix(query_pos))) {
          if (key_id) {
            *key_id = node.key_id();
          }
          if (key) {
            *key = stored_key;
          }
          found = true;
        }
      }
      return found;
    }

    if (node.child() == NODE_TERMINAL_LABEL) {
      Node leaf_node = nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL);
      if (leaf_node.is_leaf()) {
        if (pool_->get(leaf_node.key_id(), key)) {
          if (key_id) {
            *key_id = leaf_node.key_id();
          }
          found = true;
        }
      }
    }

    node_id = node.offset() ^ query[query_pos];
    node = nodes_->get(node_id);
    if (node.label() != query[query_pos]) {
      return found;
    }
  }

  if (node.is_leaf()) {
    Key stored_key;
    if (pool_->get(node.key_id(), &stored_key)) {
      if (stored_key.size() <= query.size()) {
        if (key_id) {
          *key_id = node.key_id();
        }
        if (key) {
          *key = stored_key;
        }
        found = true;
      }
    }
  } else if (node.child() == NODE_TERMINAL_LABEL) {
    node = nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL);
    if (pool_->get(node.key_id(), key)) {
      if (key_id) {
        *key_id = node.key_id();
      }
      found = true;
    }
  }
  return found;
}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    MapCursorAllKeys<Bytes> query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    const MapCursorKeyIDRange<Bytes> &query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    const MapCursorKeyRange<Bytes> &query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

void DoubleArray<Bytes>::create_map(Storage *storage, uint32_t storage_node_id,
                                    const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    nodes_.reset(NodeArray::create(storage, storage_node_id_,
                                   NODE_ARRAY_SIZE));
    siblings_.reset(SiblingArray::create(storage, storage_node_id_,
                                         SIBLING_ARRAY_SIZE));
    blocks_.reset(BlockArray::create(storage, storage_node_id_,
                                     BLOCK_ARRAY_SIZE));
    pool_.reset(KeyPool<Bytes>::create(storage, storage_node_id_));
    header_->nodes_storage_node_id = nodes_->storage_node_id();
    header_->siblings_storage_node_id = siblings_->storage_node_id();
    header_->blocks_storage_node_id = blocks_->storage_node_id();
    header_->pool_storage_node_id = pool_->storage_node_id();
    Node * const root_node = reserve_node(ROOT_NODE_ID);
    root_node[NODE_INVALID_OFFSET - ROOT_NODE_ID].set_is_origin(true);
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void DoubleArray<Bytes>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  storage_node_id_ = storage_node_id;
  StorageNode storage_node = storage->open_node(storage_node_id_);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  siblings_.reset(
      SiblingArray::open(storage, header_->siblings_storage_node_id));
  blocks_.reset(BlockArray::open(storage, header_->blocks_storage_node_id));
  pool_.reset(KeyPool<Bytes>::open(storage, header_->pool_storage_node_id));
}

bool DoubleArray<Bytes>::replace_key(int64_t key_id, KeyArg src_key,
                                     KeyArg dest_key) {
  Node *dest_node;
  uint64_t key_pos;
  find_leaf(dest_key, &dest_node, &key_pos);
  if (!insert_leaf(dest_key, dest_node, key_pos, &dest_node)) {
    return false;
  }
  Node *src_node;
  if (!find_leaf(src_key, &src_node, &key_pos)) {
    // Critical error.
    GRNXX_ERROR() << "not found: src_key = " << src_key;
    throw LogicError();
  }
  pool_->reset(key_id, dest_key);
  dest_node->set_key_id(key_id);
  src_node->set_offset(NODE_INVALID_OFFSET);
  return true;
}

bool DoubleArray<Bytes>::find_leaf(KeyArg key, Node **leaf_node,
                                   uint64_t *leaf_key_pos) {
  Node *node = &nodes_->get_value(ROOT_NODE_ID);
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node->is_leaf()) {
      // Found.
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return true;
    }
    const uint64_t child_node_id = node->offset() ^ key[key_pos];
    Node * const child_node = &nodes_->get_value(child_node_id);
    if (child_node->label() != key[key_pos]) {
      // Not found.
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return false;
    }
    node = child_node;
  }
  *leaf_node = node;
  *leaf_key_pos = key_pos;
  if (node->is_leaf()) {
    // Found.
    return true;
  }
  if (node->child() != NODE_TERMINAL_LABEL) {
    // Not found.
    return false;
  }
  const uint64_t node_id = node->offset() ^ NODE_TERMINAL_LABEL;
  node = &nodes_->get_value(node_id);
  *leaf_node = node;
  return node->is_leaf();
}

bool DoubleArray<Bytes>::insert_leaf(KeyArg key, Node *node,
                                     uint64_t key_pos, Node **leaf_node) {
  if (node->is_leaf()) {
    Key stored_key;
    if (!pool_->get(node->key_id(), &stored_key)) {
      GRNXX_ERROR() << "not found: key = " << key << ", key_pos = " << key_pos;
      throw LogicError();
    }
    uint64_t i = key_pos;
    while ((i < key.size()) && (i < stored_key.size())) {
      if (key[i] != stored_key[i]) {
        break;
      }
      ++i;
    }
    if ((i == key.size()) && (i == stored_key.size())) {
      return false;
    }
    while (key_pos < i) {
      node = insert_node(node, key[key_pos]);
      ++key_pos;
    }
    uint64_t labels[2];
    labels[0] = (key_pos < stored_key.size()) ?
        stored_key[key_pos] : NODE_TERMINAL_LABEL;
    labels[1] = (key_pos < key.size()) ? key[key_pos] : NODE_TERMINAL_LABEL;
    *leaf_node = separate(node, labels);
    return true;
  } else if (node->label() == NODE_TERMINAL_LABEL) {
    *leaf_node = node;
    return true;
  } else {
    const uint64_t label = (key_pos < key.size()) ?
        key[key_pos] : NODE_TERMINAL_LABEL;
    resolve(node, label);
    *leaf_node = insert_node(node, label);
    return true;
  }
}

auto DoubleArray<Bytes>::insert_node(Node *node, uint64_t label) -> Node * {
  uint64_t offset = node->offset();
  if (node->is_leaf() || (offset == NODE_INVALID_OFFSET)) {
    offset = find_offset(&label, 1);
  }
  const uint64_t next_node_id = offset ^ label;
  uint8_t *next_sibling = &siblings_->get_value(next_node_id);
  Node * const next_node = reserve_node(next_node_id);
  next_node->set_label(label);
  if (node->is_leaf()) {
    next_node[offset - next_node_id].set_is_origin(true);
    next_node->set_key_id(node->key_id());
  } else if (node->offset() == NODE_INVALID_OFFSET) {
    next_node[offset - next_node_id].set_is_origin(true);
  }
  node->set_offset(offset);
  const uint64_t child_label = node->child();
  if (child_label == NODE_INVALID_LABEL) {
    node->set_child(label);
  } else if ((label == NODE_TERMINAL_LABEL) ||
             ((child_label != NODE_TERMINAL_LABEL) && (label < child_label))) {
    // The child node becomes the first child.
    *next_sibling = child_label;
    next_node->set_has_sibling();
    node->set_child(label);
  } else {
    uint64_t prev_node_id = offset ^ child_label;
    Node *prev_node = &next_node[prev_node_id - next_node_id];
    uint8_t *prev_sibling = &next_sibling[prev_node_id - next_node_id];
    uint64_t sibling_label = prev_node->has_sibling() ?
        *prev_sibling : NODE_INVALID_LABEL;
    while (label > sibling_label) {
      prev_node_id = offset ^ sibling_label;
      prev_node = &next_node[prev_node_id - next_node_id];
      prev_sibling = &next_sibling[prev_node_id - next_node_id];
      sibling_label = prev_node->has_sibling() ?
          *prev_sibling : NODE_INVALID_LABEL;
    }
    *next_sibling = *prev_sibling;
    *prev_sibling = label;
    if (prev_node->has_sibling()) {
      next_node->set_has_sibling();
    }
    prev_node->set_has_sibling();
  }
  return next_node;
}

auto DoubleArray<Bytes>::separate(Node *node, uint64_t labels[2]) -> Node * {
  const uint64_t offset = find_offset(labels, 2);
  uint64_t node_ids[2] = { offset ^ labels[0], offset ^ labels[1] };
  Node *nodes[2];
  nodes[0] = reserve_node(node_ids[0]);
  nodes[1] = reserve_node(node_ids[1]);
  uint8_t * const sibling_block =
      &siblings_->get_value(offset & ~(BLOCK_SIZE - 1));
  nodes[0]->set_label(labels[0]);
  nodes[0]->set_key_id(node->key_id());
  nodes[1]->set_label(labels[1]);
  nodes[0][offset - node_ids[0]].set_is_origin(true);
  node->set_offset(offset);
  if ((labels[0] == NODE_TERMINAL_LABEL) ||
      ((labels[1] != NODE_TERMINAL_LABEL) && (labels[0] < labels[1]))) {
    sibling_block[node_ids[0] % BLOCK_SIZE] = static_cast<uint8_t>(labels[1]);
    nodes[0]->set_has_sibling();
    node->set_child(labels[0]);
  } else {
    sibling_block[node_ids[1] % BLOCK_SIZE] = static_cast<uint8_t>(labels[0]);
    nodes[1]->set_has_sibling();
    node->set_child(labels[1]);
  }
  return nodes[1];
}

void DoubleArray<Bytes>::resolve(Node *node, uint64_t label) {
  uint64_t offset = node->offset();
  if (offset == NODE_INVALID_OFFSET) {
    return;
  }
  uint64_t dest_node_id = offset ^ label;
  Node * const dest_node = &nodes_->get_value(dest_node_id);
  if (dest_node->is_phantom()) {
    return;
  }
  Node * const node_block = dest_node - (dest_node_id % BLOCK_SIZE);
  uint8_t * const sibling_block =
      &siblings_->get_value(dest_node_id & ~(BLOCK_SIZE - 1));
  uint64_t labels[NODE_MAX_LABEL + 1];
  uint64_t num_labels = 0;
  uint64_t child_label = node->child();
  while (child_label != NODE_INVALID_LABEL) {
    labels[num_labels++] = child_label;
    const uint64_t child_node_id = offset ^ child_label;
    if (node_block[child_node_id % BLOCK_SIZE].has_sibling()) {
      child_label = sibling_block[child_node_id % BLOCK_SIZE];
    } else {
      child_label = NODE_INVALID_LABEL;
    }
  }
  labels[num_labels] = label;
  offset = find_offset(labels, num_labels + 1);
  migrate_nodes(node, offset, labels, num_labels);
}

void DoubleArray<Bytes>::migrate_nodes(Node *node, uint64_t dest_offset,
                                       const uint64_t *labels,
                                       uint64_t num_labels) {
  const uint64_t src_offset = node->offset();
  Node * const src_node_block =
      &nodes_->get_value(src_offset & ~(BLOCK_SIZE - 1));
  uint8_t * const src_sibling_block =
      &siblings_->get_value(src_offset & ~(BLOCK_SIZE - 1));
  Node * const dest_node_block =
      &nodes_->get_value(dest_offset & ~(BLOCK_SIZE - 1));
  uint8_t * const dest_sibling_block =
      &siblings_->get_value(dest_offset & ~(BLOCK_SIZE - 1));
  for (uint64_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    Node * const src_node = &src_node_block[src_node_id % BLOCK_SIZE];
    uint8_t * const src_sibling = &src_sibling_block[src_node_id % BLOCK_SIZE];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
    Node * const dest_node = reserve_node(dest_node_id);
    uint8_t * const dest_sibling =
        &dest_sibling_block[dest_node_id % BLOCK_SIZE];
    Node dummy_node = *src_node;
    dummy_node.set_is_origin(dest_node->is_origin());
    *dest_node = dummy_node;
    *dest_sibling = *src_sibling;
  }
  header_->num_zombies += num_labels;
  dest_node_block[dest_offset % BLOCK_SIZE].set_is_origin(true);
  node->set_offset(dest_offset);
}

uint64_t DoubleArray<Bytes>::find_offset(const uint64_t *labels,
                                         uint64_t num_labels) {
  // Blocks are tested in descending order of level.
  // Generally, a lower level contains more phantom nodes.
  uint64_t level = bit_scan_reverse(num_labels) + 1;
  level = (level < BLOCK_MAX_LEVEL) ? (BLOCK_MAX_LEVEL - level) : 0;
  uint64_t block_count = 0;
  do {
    uint64_t latest_block_id = header_->latest_blocks[level];
    if (latest_block_id == BLOCK_INVALID_ID) {
      // This level group is skipped because it is empty.
      continue;
    }
    uint64_t block_id = latest_block_id;
    do {
      Block * const block = &blocks_->get_value(block_id);
      Node * const node_block = &nodes_->get_value(block_id * BLOCK_SIZE);
      const uint64_t first_phantom_node_id = block->first_phantom();
      uint64_t node_id = first_phantom_node_id;
      do {
        const uint64_t offset = node_id ^ labels[0];
        if (!node_block[offset].is_origin()) {
          uint64_t i;
          for (i = 1; i < num_labels; ++i) {
            if (!node_block[offset ^ labels[i]].is_phantom()) {
              break;
            }
          }
          if (i >= num_labels) {
            // Found.
            return (block_id * BLOCK_SIZE) | offset;
          }
        }
        node_id = node_block[node_id].next();
      } while (node_id != first_phantom_node_id);

      Block * const prev_block = block;
      const uint64_t prev_block_id = block_id;
      const uint64_t next_block_id = block->next();
      block_id = next_block_id;

      // A block level is updated if this function fails many times.
      prev_block->set_failure_count(prev_block->failure_count() + 1);
      if (prev_block->failure_count() >= BLOCK_MAX_FAILURE_COUNT) {
        update_block_level(prev_block_id, prev_block, level + 1);
        if (next_block_id == latest_block_id) {
          // All the blocks are tested.
          break;
        } else {
          latest_block_id = header_->latest_blocks[level];
          continue;
        }
      }
    } while ((++block_count < BLOCK_MAX_COUNT) &&
             (block_id != latest_block_id));
  } while ((block_count < BLOCK_MAX_COUNT) && (level-- != 0));
  // Use a new block.
  return (header_->num_blocks * BLOCK_SIZE) ^ labels[0];
}

auto DoubleArray<Bytes>::reserve_node(uint64_t node_id) -> Node * {
  const uint64_t block_id = node_id / BLOCK_SIZE;
  Block *block;
  if (node_id >= (header_->num_blocks * BLOCK_SIZE)) {
    block = reserve_block(block_id);
  } else {
    block = &blocks_->get_value(block_id);
  }
  Node * const node = &nodes_->get_value(node_id);
  Node * const node_block = node - (node_id % BLOCK_SIZE);
  Node * const next_node = &node_block[node->next()];
  Node * const prev_node = &node_block[node->prev()];
  if ((node_id % BLOCK_SIZE) == block->first_phantom()) {
    block->set_first_phantom(node->next());
  }
  prev_node->set_next(node->next());
  next_node->set_prev(node->prev());
  if (block->level() != BLOCK_MAX_LEVEL) {
    const uint64_t threshold =
        1ULL << ((BLOCK_MAX_LEVEL - block->level() - 1) * 2);
    if (block->num_phantoms() == threshold) {
      update_block_level(block_id, block, block->level() + 1);
    }
  }
  block->set_num_phantoms(block->num_phantoms() - 1);
  node->unset_is_phantom();
  --header_->num_phantoms;
  return node;
}

auto DoubleArray<Bytes>::reserve_block(uint64_t block_id) -> Block * {
  if (block_id >= blocks_->size()) {
    GRNXX_ERROR() << "too many blocks: block_id = " << block_id
                  << ", max_block_id = " << (blocks_->size() - 1);
    throw LogicError();
  }
  Block * const block = &blocks_->get_value(block_id);
  Node * const node = &nodes_->get_value(block_id * BLOCK_SIZE);
  *block = Block::empty_block();
  for (uint64_t i = 0; i < BLOCK_SIZE; ++i) {
    node[i] = Node::phantom_node(i + 1, i - 1);
  }
  // The level of a new block is 0.
  set_block_level(block_id, block, 0);
  header_->num_blocks = block_id + 1;
  header_->num_phantoms += BLOCK_SIZE;
  return block;
}

void DoubleArray<Bytes>::update_block_level(uint64_t block_id, Block *block,
                                            uint64_t level) {
  // FIXME: If set_block_level() fails, the block gets lost.
  unset_block_level(block_id, block);
  set_block_level(block_id, block, level);
}

void DoubleArray<Bytes>::set_block_level(uint64_t block_id, Block *block,
                                         uint64_t level) {
  if (header_->latest_blocks[level] == BLOCK_INVALID_ID) {
    // The block becomes the only one member of the level group.
    block->set_next(block_id);
    block->set_prev(block_id);
    header_->latest_blocks[level] = block_id;
  } else {
    // The block is appended to the level group.
    const uint64_t next_block_id = header_->latest_blocks[level];
    Block * const next_block = &blocks_->get_value(next_block_id);
    const uint64_t prev_block_id = next_block->prev();
    Block * const prev_block = &blocks_->get_value(prev_block_id);
    block->set_next(next_block_id);
    block->set_prev(prev_block_id);
    prev_block->set_next(block_id);
    next_block->set_prev(block_id);
  }
  block->set_level(level);
  block->set_failure_count(0);
}

void DoubleArray<Bytes>::unset_block_level(uint64_t block_id, Block *block) {
  const uint64_t level = block->level();
  const uint64_t next_block_id = block->next();
  const uint64_t prev_block_id = block->prev();
  if (next_block_id == prev_block_id) {
    // The level group becomes empty.
    header_->latest_blocks[level] = BLOCK_INVALID_ID;
  } else {
    Block * const next_block = &blocks_->get_value(next_block_id);
    Block * const prev_block = &blocks_->get_value(prev_block_id);
    prev_block->set_next(next_block_id);
    next_block->set_prev(prev_block_id);
    if (block_id == header_->latest_blocks[level]) {
      // The next block becomes the latest block of the level group.
      header_->latest_blocks[level] = next_block_id;
    }
  }
}

}  // namespace map
}  // namespace grnxx
