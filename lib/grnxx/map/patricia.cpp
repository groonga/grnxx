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
#include "grnxx/map/patricia.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/common_header.hpp"
#include "grnxx/map/hash.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/key_pool.hpp"
#include "grnxx/map/patricia/node.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::Patricia";

constexpr uint64_t ROOT_NODE_ID = 0;

using patricia::NODE_INVALID_OFFSET;

using patricia::NODE_DEAD;
using patricia::NODE_LEAF;
using patricia::NODE_BRANCH;
using patricia::NODE_TERMINAL;

}  // namespace

struct PatriciaHeader {
  CommonHeader common_header;
  MapType map_type;
  uint64_t next_node_id;
  uint32_t nodes_storage_node_id;
  uint32_t pool_storage_node_id;
  uint32_t cache_storage_node_id;

  // Initialize the member variables.
  PatriciaHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

PatriciaHeader::PatriciaHeader()
    : common_header(FORMAT_STRING, MAP_PATRICIA),
      next_node_id(2),
      nodes_storage_node_id(STORAGE_INVALID_NODE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      cache_storage_node_id(STORAGE_INVALID_NODE_ID) {}

PatriciaHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
Patricia<T>::Patricia()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      nodes_(),
      pool_() {}

template <typename T>
Patricia<T>::~Patricia() {}

template <typename T>
Patricia<T> *Patricia<T>::create(Storage *storage,
                                 uint32_t storage_node_id,
                                 const MapOptions &options) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    throw MemoryError();
  }
  map->create_map(storage, storage_node_id, options);
  return map.release();
}

template <typename T>
Patricia<T> *Patricia<T>::open(Storage *storage,
                               uint32_t storage_node_id) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    throw MemoryError();
  }
  map->open_map(storage, storage_node_id);
  return map.release();
}

template <typename T>
uint32_t Patricia<T>::storage_node_id() const {
  return storage_node_id_;
}

template <typename T>
MapType Patricia<T>::type() const {
  return MAP_PATRICIA;
}

template <typename T>
int64_t Patricia<T>::max_key_id() const {
  return pool_->max_key_id();
}

template <typename T>
uint64_t Patricia<T>::num_keys() const {
  return pool_->num_keys();
}

template <typename T>
bool Patricia<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > pool_->max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

template <typename T>
bool Patricia<T>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found.
    return false;
  }
  // The root node must not be dead because the above get() has succeeded.
  uint64_t node_id = ROOT_NODE_ID;
  Node *prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = &nodes_->get_value(node_id);
    if (node->status() == NODE_LEAF) {
      if (node->key_id() != key_id) {
        // Not found.
        return false;
      }
      pool_->unset(key_id);
      if (prev_node) {
        Node * const sibling_node = node + (node_id ^ 1) - node_id;
        *prev_node = *sibling_node;
      } else {
        *node = Node::dead_node();
      }
      return true;
    }
    prev_node = node;
    node_id = node->offset() + get_ith_bit(key, node->bit_pos());
  }
}

template <typename T>
bool Patricia<T>::reset(int64_t key_id, KeyArg dest_key) {
  // Find the source key.
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  // The root node must not be dead because the above get() has succeeded.
  uint64_t node_id = ROOT_NODE_ID;
  Node *src_node;
  Node *src_prev_node = nullptr;
  Node *src_sibling_node = nullptr;
  for ( ; ; ) {
    src_node = &nodes_->get_value(node_id);
    if (src_node->status() == NODE_LEAF) {
      if (src_node->key_id() != key_id) {
        // Not found.
        return false;
      }
      if (src_prev_node) {
        src_sibling_node = src_node + (node_id ^ 1) - node_id;
      }
      break;
    }
    src_prev_node = src_node;
    node_id = src_node->offset() + get_ith_bit(src_key, src_node->bit_pos());
  }
  // Add the destination key.
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  node_id = ROOT_NODE_ID;
  Node *history[(sizeof(Key) * 8) + 1];
  int depth = -1;
  for ( ; ; ) {
    Node * const node = &nodes_->get_value(node_id);
    history[++depth] = node;
    if (node->status() == NODE_LEAF) {
      break;
    }
    node_id = node->offset() +
              get_ith_bit(dest_normalized_key, node->bit_pos());
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(history[depth]->key_id());
  const uint64_t count =
      count_common_prefix_bits(dest_normalized_key, stored_key);
  if (count == (sizeof(Key) * 8)) {
    // Found.
    return false;
  }
  // Find the branching point in "history".
  while (depth > 0) {
    if (history[depth - 1]->bit_pos() < count) {
      break;
    }
    --depth;
  }
  Node * const dest_prev_node = history[depth];
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  pool_->reset(key_id, dest_normalized_key);
  Node *dest_node;
  Node *dest_sibling_node;
  if (get_ith_bit(dest_normalized_key, count)) {
    dest_node = &next_nodes[1];
    dest_sibling_node = &next_nodes[0];
  } else {
    dest_node = &next_nodes[0];
    dest_sibling_node = &next_nodes[1];
  }
  if (dest_prev_node == src_prev_node) {
    src_prev_node = dest_sibling_node;
  } else if (dest_prev_node == src_node) {
    src_sibling_node = dest_node;
    src_prev_node = dest_prev_node;
  }
  *dest_sibling_node = *dest_prev_node;
  *dest_node = Node::leaf_node(key_id);
  *dest_prev_node = Node::branch_node(count, header_->next_node_id);
  *src_prev_node = *src_sibling_node;
  header_->next_node_id += 2;
  return true;
}

template <typename T>
bool Patricia<T>::find(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  if (node.status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  for ( ; ; ) {
    if (node.status() == NODE_LEAF) {
      Key stored_key = pool_->get_key(node.key_id());
      if (!Helper<T>::equal_to(normalized_key, stored_key)) {
        // Not found.
        return false;
      }
      if (key_id) {
        *key_id = node.key_id();
      }
      return true;
    }
    node_id = node.offset() + get_ith_bit(normalized_key, node.bit_pos());
    node = nodes_->get(node_id);
  }
}

template <typename T>
bool Patricia<T>::add(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  uint64_t node_id = ROOT_NODE_ID;
  Node *node = &nodes_->get_value(node_id);
  if (node->status() == NODE_DEAD) {
    // The patricia is empty.
    int64_t next_key_id = pool_->add(normalized_key);
    *node = Node::leaf_node(next_key_id);
    if (key_id) {
      *key_id = next_key_id;
    }
    return true;
  }
  Node *history[(sizeof(Key) * 8) + 1];
  int depth = 0;
  history[0] = node;
  while (node->status() != NODE_LEAF) {
    node_id = node->offset() + get_ith_bit(normalized_key, node->bit_pos());
    node = &nodes_->get_value(node_id);
    history[++depth] = node;
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(node->key_id());
  const uint64_t count = count_common_prefix_bits(normalized_key, stored_key);
  if (count == (sizeof(Key) * 8)) {
    // Found.
    if (key_id) {
      *key_id = node->key_id();
    }
    return false;
  }
  // Find the branching point in "history".
  while (depth > 0) {
    if (history[depth - 1]->bit_pos() < count) {
      break;
    }
    --depth;
  }
  node = history[depth];
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  int64_t next_key_id = pool_->add(normalized_key);
  if (get_ith_bit(normalized_key, count)) {
    next_nodes[0] = *node;
    next_nodes[1] = Node::leaf_node(next_key_id);
  } else {
    next_nodes[0] = Node::leaf_node(next_key_id);
    next_nodes[1] = *node;
  }
  *node = Node::branch_node(count, header_->next_node_id);
  header_->next_node_id += 2;
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

template <typename T>
bool Patricia<T>::remove(KeyArg key) {
  const Key normalized_key = Helper<T>::normalize(key);
  uint64_t node_id = ROOT_NODE_ID;
  Node *node = &nodes_->get_value(node_id);
  if (node->status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  Node *prev_node = nullptr;
  for ( ; ; ) {
    if (node->status() == NODE_LEAF) {
      Key stored_key = pool_->get_key(node->key_id());
      if (!Helper<T>::equal_to(normalized_key, stored_key)) {
        // Not found.
        return false;
      }
      pool_->unset(node->key_id());
      if (prev_node) {
        Node * const sibling_node = node + (node_id ^ 1) - node_id;
        *prev_node = *sibling_node;
      } else {
        *node = Node::dead_node();
      }
      return true;
    }
    prev_node = node;
    node_id = node->offset() + get_ith_bit(normalized_key, node->bit_pos());
    node = &nodes_->get_value(node_id);
  }
}

template <typename T>
bool Patricia<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
  const Key src_normalized_key = Helper<T>::normalize(src_key);
  uint64_t node_id = ROOT_NODE_ID;
  Node *src_node = &nodes_->get_value(node_id);
  if (src_node->status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  int64_t src_key_id;
  Node *src_prev_node = nullptr;
  Node *src_sibling_node = nullptr;
  for ( ; ; ) {
    if (src_node->status() == NODE_LEAF) {
      src_key_id = src_node->key_id();
      Key stored_key = pool_->get_key(src_key_id);
      if (!Helper<T>::equal_to(src_normalized_key, stored_key)) {
        // Not found.
        return false;
      }
      if (src_prev_node) {
        src_sibling_node = src_node + (node_id ^ 1) - node_id;
      }
      break;
    }
    src_prev_node = src_node;
    node_id = src_node->offset() +
              get_ith_bit(src_normalized_key, src_node->bit_pos());
    src_node = &nodes_->get_value(node_id);
  }
  // Add the destination key.
  const Key dest_normalized_key = Helper<T>::normalize(dest_key);
  node_id = ROOT_NODE_ID;
  Node *history[(sizeof(Key) * 8) + 1];
  int depth = -1;
  for ( ; ; ) {
    Node * const node = &nodes_->get_value(node_id);
    history[++depth] = node;
    if (node->status() == NODE_LEAF) {
      break;
    }
    node_id = node->offset() +
              get_ith_bit(dest_normalized_key, node->bit_pos());
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(history[depth]->key_id());
  const uint64_t count =
      count_common_prefix_bits(dest_normalized_key, stored_key);
  if (count == (sizeof(Key) * 8)) {
    // Found.
    return false;
  }
  // Find the branching point in "history".
  while (depth > 0) {
    if (history[depth - 1]->bit_pos() < count) {
      break;
    }
    --depth;
  }
  Node * const dest_prev_node = history[depth];
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  pool_->reset(src_key_id, dest_normalized_key);
  Node *dest_node;
  Node *dest_sibling_node;
  if (get_ith_bit(dest_normalized_key, count)) {
    dest_node = &next_nodes[1];
    dest_sibling_node = &next_nodes[0];
  } else {
    dest_node = &next_nodes[0];
    dest_sibling_node = &next_nodes[1];
  }
  if (dest_prev_node == src_prev_node) {
    src_prev_node = dest_sibling_node;
  } else if (dest_prev_node == src_node) {
    src_sibling_node = dest_node;
    src_prev_node = dest_prev_node;
  }
  *dest_sibling_node = *dest_prev_node;
  *dest_node = Node::leaf_node(src_key_id);
  *dest_prev_node = Node::branch_node(count, header_->next_node_id);
  *src_prev_node = *src_sibling_node;
  header_->next_node_id += 2;
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

template <typename T>
bool Patricia<T>::truncate() {
  Node * const root_node = &nodes_->get_value(ROOT_NODE_ID);
  if (!root_node) {
    return false;
  }
  pool_->truncate();
  *root_node = Node::dead_node();
  return true;
}

template <typename T>
void Patricia<T>::create_map(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    // TODO: Remove a magic number.
    nodes_.reset(NodeArray::create(storage, storage_node_id_, 1ULL << 41));
    pool_.reset(KeyPool<T>::create(storage, storage_node_id_));
    header_->nodes_storage_node_id = nodes_->storage_node_id();
    header_->pool_storage_node_id = pool_->storage_node_id();
    Node * const root_node = &nodes_->get_value(ROOT_NODE_ID);
    *root_node = Node::dead_node();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

template <typename T>
void Patricia<T>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  pool_.reset(KeyPool<T>::open(storage, header_->pool_storage_node_id));
}

template <typename T>
uint64_t Patricia<T>::get_ith_bit(KeyArg key, uint64_t bit_pos) {
  return (key >> ((sizeof(Key) * 8) - 1 - bit_pos)) & 1;
}

template <>
uint64_t Patricia<double>::get_ith_bit(KeyArg key, uint64_t bit_pos) {
  constexpr uint64_t MASK[2] = { 1ULL << 63, ~0ULL };
  uint64_t x = *reinterpret_cast<const uint64_t *>(&key);
  x ^= MASK[x >> 63];
  return (x >> ((sizeof(Key) * 8) - 1 - bit_pos)) & 1;
}

template <>
uint64_t Patricia<GeoPoint>::get_ith_bit(KeyArg key, uint64_t bit_pos) {
  const uint32_t x = reinterpret_cast<const uint32_t *>(&key)[bit_pos & 1];
  return (x >> (31 - (bit_pos >> 1))) & 1;
}

template <typename T>
uint64_t Patricia<T>::count_common_prefix_bits(KeyArg lhs, KeyArg rhs) {
  if (lhs == rhs) {
    return sizeof(Key) * 8;
  }
  return (sizeof(Key) * 8) - 1 - bit_scan_reverse(static_cast<Key>(lhs ^ rhs));
}

template <>
uint64_t Patricia<double>::count_common_prefix_bits(KeyArg lhs, KeyArg rhs) {
  const uint64_t x = *reinterpret_cast<const uint64_t *>(&lhs);
  const uint64_t y = *reinterpret_cast<const uint64_t *>(&rhs);
  if (x == y) {
    return sizeof(Key) * 8;
  }
  return (sizeof(Key) * 8) - 1 - bit_scan_reverse(x ^ y);
}

template <>
uint64_t Patricia<GeoPoint>::count_common_prefix_bits(KeyArg lhs, KeyArg rhs) {
  if (lhs == rhs) {
    return sizeof(GeoPoint) * 8;
  }
  const GeoPoint x = GeoPoint(lhs.value() ^ rhs.value());
  const uint32_t latitude = x.latitude();
  const uint32_t longitude = x.longitude();
  const uint8_t y = bit_scan_reverse(latitude | longitude);
  return ((31 - y) << 1) + 1 - (latitude >> y);
}

template class Patricia<int8_t>;
template class Patricia<uint8_t>;
template class Patricia<int16_t>;
template class Patricia<uint16_t>;
template class Patricia<int32_t>;
template class Patricia<uint32_t>;
template class Patricia<int64_t>;
template class Patricia<uint64_t>;
template class Patricia<double>;
template class Patricia<GeoPoint>;

Patricia<Bytes>::Patricia()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      nodes_(),
      pool_(),
      cache_() {}

Patricia<Bytes>::~Patricia() {}

Patricia<Bytes> *Patricia<Bytes>::create(Storage *storage,
                                         uint32_t storage_node_id,
                                         const MapOptions &options) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    throw MemoryError();
  }
  map->create_map(storage, storage_node_id, options);
  return map.release();
}

Patricia<Bytes> *Patricia<Bytes>::open(Storage *storage,
                                       uint32_t storage_node_id) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    throw MemoryError();
  }
  map->open_map(storage, storage_node_id);
  return map.release();
}

uint32_t Patricia<Bytes>::storage_node_id() const {
  return storage_node_id_;
}

MapType Patricia<Bytes>::type() const {
  return MAP_PATRICIA;
}

int64_t Patricia<Bytes>::max_key_id() const {
  return pool_->max_key_id();
}

uint64_t Patricia<Bytes>::num_keys() const {
  return pool_->num_keys();
}

bool Patricia<Bytes>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > pool_->max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

bool Patricia<Bytes>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found.
    return false;
  }
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  Node *prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = &nodes_->get_value(node_id);
    switch (node->status()) {
      case NODE_LEAF: {
        if (node->key_id() != key_id) {
          // Not found.
          return false;
        }
        pool_->unset(key_id);
        if (prev_node) {
          Node * const sibling_node = node + (node_id ^ 1) - node_id;
          *prev_node = *sibling_node;
        } else {
          *node = Node::dead_node();
        }
        return true;
      }
      case NODE_BRANCH: {
        if (node->bit_pos() >= bit_size) {
          // Not found.
          return false;
        }
        node_id = node->offset() + get_ith_bit(key, node->bit_pos());
        break;
      }
      case NODE_TERMINAL: {
        if (node->bit_size() > bit_size) {
          // Not found.
          return false;
        }
        node_id = node->offset() + (node->bit_size() < bit_size);
        break;
      }
    }
    prev_node = node;
  }
}

bool Patricia<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  // Find the source key.
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  const uint64_t src_bit_size = src_key.size() * 8;
  uint64_t src_node_id = ROOT_NODE_ID;
  Node *src_node;
  Node *src_prev_node = nullptr;
  Node *src_sibling_node = nullptr;
  for ( ; ; ) {
    src_node = &nodes_->get_value(src_node_id);
    if (src_node->status() == NODE_LEAF) {
      if (src_node->key_id() != key_id) {
        // Not found.
        return false;
      }
      if (src_prev_node) {
        src_sibling_node = src_node + (src_node_id ^ 1) - src_node_id;
      }
      break;
    } else if (src_node->status() == NODE_BRANCH) {
      if (src_node->bit_pos() >= src_bit_size) {
        // Not found.
        return false;
      }
      src_node_id = src_node->offset() +
                    get_ith_bit(src_key, src_node->bit_pos());
    } else if (src_node->status() == NODE_TERMINAL) {
      if (src_node->bit_size() > src_bit_size) {
        // Not found.
        return false;
      }
      src_node_id = src_node->offset() +
                    (src_node->bit_size() < src_bit_size);
    }
    src_prev_node = src_node;
  }
  // Add the destination key.
  constexpr std::size_t HISTORY_SIZE = 8;
  uint64_t dest_node_id = ROOT_NODE_ID;
  const uint64_t dest_bit_size = dest_key.size() * 8;
  Node *history[HISTORY_SIZE];
  int depth = -1;
  for ( ; ; ) {
    Node *node = &nodes_->get_value(dest_node_id);
    history[++depth % HISTORY_SIZE] = node;
    if (node->status() == NODE_LEAF) {
      break;
    } else if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() >= dest_bit_size) {
        break;
      }
      dest_node_id = node->offset() + get_ith_bit(dest_key, node->bit_pos());
    } else {
      if (node->bit_size() >= dest_bit_size) {
        break;
      }
      dest_node_id = node->offset() + 1;
    }
  }
  // Find a leaf node.
  Node *leaf_node = history[depth % HISTORY_SIZE];
  while (leaf_node->status() != NODE_LEAF) {
    leaf_node = &nodes_->get_value(leaf_node->offset());
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(leaf_node->key_id());
  const uint64_t min_size = (dest_key.size() < stored_key.size()) ?
                            dest_key.size() : stored_key.size();
  uint64_t count;
  for (count = 0; count < min_size; ++count) {
    if (dest_key[count] != stored_key[count]) {
      break;
    }
  }
  if (count == min_size) {
    if (dest_key.size() == stored_key.size()) {
      // Found.
      return false;
    }
    Node * const dest_prev_node = history[depth % HISTORY_SIZE];
    Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
    pool_->reset(key_id, dest_key);
    Node *dest_node;
    Node *dest_sibling_node;
    if (count == dest_key.size()) {
      // "dest_key" is a prefix of "stored_key".
      dest_node = &next_nodes[0];
      dest_sibling_node = &next_nodes[1];
    } else {
      // "stored_key" is a prefix of "dest_key".
      dest_node = &next_nodes[1];
      dest_sibling_node = &next_nodes[0];
    }
    if (dest_prev_node == src_prev_node) {
      src_prev_node = dest_sibling_node;
    } else if (dest_prev_node == src_node) {
      src_sibling_node = dest_node;
      src_prev_node = dest_prev_node;
    }
    *dest_sibling_node = *dest_prev_node;
    *dest_node = Node::leaf_node(key_id);
    *dest_prev_node = Node::terminal_node(count * 8, header_->next_node_id);
    *src_prev_node = *src_sibling_node;
    header_->next_node_id += 2;
    return true;
  }
  count = (count * 8) + 7 -
          bit_scan_reverse(dest_key[count] ^ stored_key[count]);
  // Find the branching point in "history".
  int min_depth = (depth < 8) ? 0 : depth - 7;
  while (--depth >= min_depth) {
    Node * const node = history[depth % HISTORY_SIZE];
    if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() < count) {
        break;
      }
    } else if (node->bit_size() < count) {
      break;
    }
  }
  Node *dest_prev_node;
  if (depth >= min_depth) {
    // The branching point exists in "history".
    dest_prev_node = history[(depth + 1) % HISTORY_SIZE];
  } else {
    // Find the branching point with the naive method.
    dest_node_id = ROOT_NODE_ID;
    for ( ; ; ) {
      dest_prev_node = &nodes_->get_value(dest_node_id);
      if (dest_prev_node->status() == NODE_LEAF) {
        break;
      } else if (dest_prev_node->status() == NODE_BRANCH) {
        if (dest_prev_node->bit_pos() >= count) {
          break;
        }
        dest_node_id = dest_prev_node->offset() +
                       get_ith_bit(dest_key, dest_prev_node->bit_pos());
      } else {
        if (dest_prev_node->bit_size() > count) {
          break;
        }
        dest_node_id = dest_prev_node->offset() + 1;
      }
    }
  }
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  pool_->reset(key_id, dest_key);
  Node *dest_node;
  Node *dest_sibling_node;
  if (get_ith_bit(dest_key, count)) {
    dest_node = &next_nodes[1];
    dest_sibling_node = &next_nodes[0];
  } else {
    dest_node = &next_nodes[0];
    dest_sibling_node = &next_nodes[1];
  }
  if (dest_prev_node == src_prev_node) {
    src_prev_node = dest_sibling_node;
  } else if (dest_prev_node == src_node) {
    src_sibling_node = dest_node;
    src_prev_node = dest_prev_node;
  }
  *dest_sibling_node = *dest_prev_node;
  *dest_node = Node::leaf_node(key_id);
  *dest_prev_node = Node::branch_node(count, header_->next_node_id);
  *src_prev_node = *src_sibling_node;
  header_->next_node_id += 2;
  return true;
}

bool Patricia<Bytes>::find(KeyArg key, int64_t *key_id) {
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  if (node.status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  for ( ; ; ) {
    switch (node.status()) {
      case NODE_LEAF: {
        Key stored_key = pool_->get_key(node.key_id());
        if (key != stored_key) {
          // Not found.
          return false;
        }
        if (key_id) {
          *key_id = node.key_id();
        }
        return true;
      }
      case NODE_BRANCH: {
        if (node.bit_pos() >= bit_size) {
          // Not found.
          return false;
        }
        node_id = node.offset() + get_ith_bit(key, node.bit_pos());
        break;
      }
      case NODE_TERMINAL: {
        if (node.bit_size() > bit_size) {
          // Not found.
          return false;
        }
        node_id = node.offset() + (node.bit_size() < bit_size);
        break;
      }
    }
    node = nodes_->get(node_id);
  }
}

bool Patricia<Bytes>::add(KeyArg key, int64_t *key_id) {
  constexpr std::size_t HISTORY_SIZE = 8;
  int64_t * const cache = nullptr;
//  int64_t * const cache =
//      &cache_->get_value(Hash<Key>()(key) % cache_->size());
//  if ((*cache >= 0) && (*cache < pool_->max_key_id())) {
//    Key cached_key;
//    if (pool_->get(*cache, &cached_key)) {
//      if (key == cached_key) {
//        if (key_id) {
//          *key_id = *cache;
//        }
//        return false;
//      }
//    }
//  }
  uint64_t node_id = ROOT_NODE_ID;
  Node *node = &nodes_->get_value(node_id);
  if (node->status() == NODE_DEAD) {
    // The patricia is empty.
    int64_t next_key_id = pool_->add(key);
    *node = Node::leaf_node(next_key_id);
    if (key_id) {
      *key_id = next_key_id;
    }
    if (cache) {
      *cache = next_key_id;
    }
    return true;
  }
  const uint64_t bit_size = key.size() * 8;
  Node *history[HISTORY_SIZE];
  int depth = 0;
  history[0] = node;
  while (node->status() != NODE_LEAF) {
    if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() >= bit_size) {
        break;
      }
      node_id = node->offset() + get_ith_bit(key, node->bit_pos());
    } else {
      if (node->bit_size() >= bit_size) {
        break;
      }
      node_id = node->offset() + 1;
    }
    node = &nodes_->get_value(node_id);
    history[++depth % HISTORY_SIZE] = node;
  }
  // Find a leaf node.
  while (node->status() != NODE_LEAF) {
    node_id = node->offset();
    node = &nodes_->get_value(node_id);
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(node->key_id());
  const uint64_t min_size =
      (key.size() < stored_key.size()) ? key.size() : stored_key.size();
  uint64_t count;
  for (count = 0; count < min_size; ++count) {
    if (key[count] != stored_key[count]) {
      break;
    }
  }
  if (count == min_size) {
    if (key.size() == stored_key.size()) {
      // Found.
      if (key_id) {
        *key_id = node->key_id();
      }
      if (cache) {
        *cache = node->key_id();
      }
      return false;
    }
    node = history[depth % HISTORY_SIZE];
    Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
    int64_t next_key_id = pool_->add(key);
    if (count == key.size()) {
      // "key" is a prefix of "stored_key".
      next_nodes[0] = Node::leaf_node(next_key_id);
      next_nodes[1] = *node;
      *node = Node::terminal_node(count * 8, header_->next_node_id);
    } else {
      // "stored_key" is a prefix of "key".
      next_nodes[0] = *node;
      next_nodes[1] = Node::leaf_node(next_key_id);
      *node = Node::terminal_node(count * 8, header_->next_node_id);
    }
    header_->next_node_id += 2;
    if (key_id) {
      *key_id = next_key_id;
    }
    if (cache) {
      *cache = next_key_id;
    }
    return true;
  }
  count = (count * 8) + 7 - bit_scan_reverse(key[count] ^ stored_key[count]);
  // Find the branching point in "history".
  int min_depth = (depth < 8) ? 0 : depth - 7;
  while (--depth >= min_depth) {
    node = history[depth % HISTORY_SIZE];
    if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() < count) {
        break;
      }
    } else if (node->bit_size() <= count) {
      break;
    }
  }
  if (depth >= min_depth) {
    // The branching point exists in "history".
    node = history[(depth + 1) % HISTORY_SIZE];
  } else {
    // Find the branching point with the naive method.
    node_id = ROOT_NODE_ID;
    for ( ; ; ) {
      node = &nodes_->get_value(node_id);
      if (node->status() == NODE_LEAF) {
        break;
      } else if (node->status() == NODE_BRANCH) {
        if (node->bit_pos() >= count) {
          break;
        }
        node_id = node->offset() + get_ith_bit(key, node->bit_pos());
      } else {
        if (node->bit_size() > count) {
          break;
        }
        node_id = node->offset() + 1;
      }
    }
  }
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  int64_t next_key_id = pool_->add(key);
  if (get_ith_bit(key, count)) {
    next_nodes[0] = *node;
    next_nodes[1] = Node::leaf_node(next_key_id);
  } else {
    next_nodes[0] = Node::leaf_node(next_key_id);
    next_nodes[1] = *node;
  }
  *node = Node::branch_node(count, header_->next_node_id);
  header_->next_node_id += 2;
  if (key_id) {
    *key_id = next_key_id;
  }
  if (cache) {
    *cache = next_key_id;
  }
  return true;
}

bool Patricia<Bytes>::remove(KeyArg key) {
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  Node *node = &nodes_->get_value(node_id);
  if (node->status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  Node *prev_node = nullptr;
  for ( ; ; ) {
    switch (node->status()) {
      case NODE_LEAF: {
        Key stored_key = pool_->get_key(node->key_id());
        if (stored_key != key) {
          // Not found.
          return false;
        }
        pool_->unset(node->key_id());
        if (prev_node) {
          Node * const sibling_node = node + (node_id ^ 1) - node_id;
          *prev_node = *sibling_node;
        } else {
          *node = Node::dead_node();
        }
        return true;
      }
      case NODE_BRANCH: {
        if (node->bit_pos() >= bit_size) {
          // Not found.
          return false;
        }
        node_id = node->offset() + get_ith_bit(key, node->bit_pos());
        break;
      }
      case NODE_TERMINAL: {
        if (node->bit_size() > bit_size) {
          // Not found.
          return false;
        }
        node_id = node->offset() + (node->bit_size() < bit_size);
        break;
      }
    }
    prev_node = node;
    node = &nodes_->get_value(node_id);
  }
}

bool Patricia<Bytes>::replace(KeyArg src_key, KeyArg dest_key,
                              int64_t *key_id) {
  // Find the source key.
  const uint64_t src_bit_size = src_key.size() * 8;
  int64_t src_key_id;
  uint64_t src_node_id = ROOT_NODE_ID;
  Node *src_node = &nodes_->get_value(src_node_id);
  if (src_node->status() == NODE_DEAD) {
    // Not found.
    return false;
  }
  Node *src_prev_node = nullptr;
  Node *src_sibling_node = nullptr;
  for ( ; ; ) {
    if (src_node->status() == NODE_LEAF) {
      src_key_id = src_node->key_id();
      Key stored_key = pool_->get_key(src_key_id);
      if (stored_key != src_key) {
        // Not found.
        return false;
      }
      if (key_id) {
        *key_id = src_key_id;
      }
      if (src_prev_node) {
        src_sibling_node = src_node + (src_node_id ^ 1) - src_node_id;
      }
      break;
    } else if (src_node->status() == NODE_BRANCH) {
      if (src_node->bit_pos() >= src_bit_size) {
        // Not found.
        return false;
      }
      src_node_id = src_node->offset() +
                    get_ith_bit(src_key, src_node->bit_pos());
    } else if (src_node->status() == NODE_TERMINAL) {
      if (src_node->bit_size() > src_bit_size) {
        // Not found.
        return false;
      }
      src_node_id = src_node->offset() +
                    (src_node->bit_size() < src_bit_size);
    }
    src_prev_node = src_node;
    src_node = &nodes_->get_value(src_node_id);
  }
  // Add the destination key.
  constexpr std::size_t HISTORY_SIZE = 8;
  uint64_t dest_node_id = ROOT_NODE_ID;
  const uint64_t dest_bit_size = dest_key.size() * 8;
  Node *history[HISTORY_SIZE];
  int depth = -1;
  for ( ; ; ) {
    Node *node = &nodes_->get_value(dest_node_id);
    history[++depth % HISTORY_SIZE] = node;
    if (node->status() == NODE_LEAF) {
      break;
    } else if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() >= dest_bit_size) {
        break;
      }
      dest_node_id = node->offset() + get_ith_bit(dest_key, node->bit_pos());
    } else {
      if (node->bit_size() >= dest_bit_size) {
        break;
      }
      dest_node_id = node->offset() + 1;
    }
  }
  // Find a leaf node.
  Node *leaf_node = history[depth % HISTORY_SIZE];
  while (leaf_node->status() != NODE_LEAF) {
    leaf_node = &nodes_->get_value(leaf_node->offset());
  }
  // Count the number of the common prefix bits.
  Key stored_key = pool_->get_key(leaf_node->key_id());
  const uint64_t min_size = (dest_key.size() < stored_key.size()) ?
                            dest_key.size() : stored_key.size();
  uint64_t count;
  for (count = 0; count < min_size; ++count) {
    if (dest_key[count] != stored_key[count]) {
      break;
    }
  }
  if (count == min_size) {
    if (dest_key.size() == stored_key.size()) {
      // Found.
      return false;
    }
    Node * const dest_prev_node = history[depth % HISTORY_SIZE];
    Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
    pool_->reset(src_key_id, dest_key);
    Node *dest_node;
    Node *dest_sibling_node;
    if (count == dest_key.size()) {
      // "dest_key" is a prefix of "stored_key".
      dest_node = &next_nodes[0];
      dest_sibling_node = &next_nodes[1];
    } else {
      // "stored_key" is a prefix of "dest_key".
      dest_node = &next_nodes[1];
      dest_sibling_node = &next_nodes[0];
    }
    if (dest_prev_node == src_prev_node) {
      src_prev_node = dest_sibling_node;
    } else if (dest_prev_node == src_node) {
      src_sibling_node = dest_node;
      src_prev_node = dest_prev_node;
    }
    *dest_sibling_node = *dest_prev_node;
    *dest_node = Node::leaf_node(src_key_id);
    *dest_prev_node = Node::terminal_node(count * 8, header_->next_node_id);
    *src_prev_node = *src_sibling_node;
    header_->next_node_id += 2;
    return true;
  }
  count = (count * 8) + 7 -
          bit_scan_reverse(dest_key[count] ^ stored_key[count]);
  // Find the branching point in "history".
  int min_depth = (depth < 8) ? 0 : depth - 7;
  while (--depth >= min_depth) {
    Node * const node = history[depth % HISTORY_SIZE];
    if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() < count) {
        break;
      }
    } else if (node->bit_size() < count) {
      break;
    }
  }
  Node *dest_prev_node;
  if (depth >= min_depth) {
    // The branching point exists in "history".
    dest_prev_node = history[(depth + 1) % HISTORY_SIZE];
  } else {
    // Find the branching point with the naive method.
    dest_node_id = ROOT_NODE_ID;
    for ( ; ; ) {
      dest_prev_node = &nodes_->get_value(dest_node_id);
      if (dest_prev_node->status() == NODE_LEAF) {
        break;
      } else if (dest_prev_node->status() == NODE_BRANCH) {
        if (dest_prev_node->bit_pos() >= count) {
          break;
        }
        dest_node_id = dest_prev_node->offset() +
                       get_ith_bit(dest_key, dest_prev_node->bit_pos());
      } else {
        if (dest_prev_node->bit_size() > count) {
          break;
        }
        dest_node_id = dest_prev_node->offset() + 1;
      }
    }
  }
  Node * const next_nodes = &nodes_->get_value(header_->next_node_id);
  pool_->reset(src_key_id, dest_key);
  Node *dest_node;
  Node *dest_sibling_node;
  if (get_ith_bit(dest_key, count)) {
    dest_node = &next_nodes[1];
    dest_sibling_node = &next_nodes[0];
  } else {
    dest_node = &next_nodes[0];
    dest_sibling_node = &next_nodes[1];
  }
  if (dest_prev_node == src_prev_node) {
    src_prev_node = dest_sibling_node;
  } else if (dest_prev_node == src_node) {
    src_sibling_node = dest_node;
    src_prev_node = dest_prev_node;
  }
  *dest_sibling_node = *dest_prev_node;
  *dest_node = Node::leaf_node(src_key_id);
  *dest_prev_node = Node::branch_node(count, header_->next_node_id);
  *src_prev_node = *src_sibling_node;
  header_->next_node_id += 2;
  return true;
}

bool Patricia<Bytes>::find_longest_prefix_match(KeyArg query, int64_t *key_id,
                                                Key *key) {
  const uint64_t bit_size = query.size() * 8;
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  for ( ; ; ) {
    Node node = nodes_->get(node_id);
    switch (node.status()) {
      case NODE_DEAD: {
        // Not found.
        return found;
      }
      case NODE_LEAF: {
        Key stored_key = pool_->get_key(node.key_id());
        if (query.starts_with(stored_key)) {
          if (key_id) {
            *key_id = node.key_id();
          }
          if (key) {
            *key = stored_key;
          }
          found = true;
        }
        return found;
      }
      case NODE_BRANCH: {
        if (node.bit_pos() >= bit_size) {
          return found;
        }
        node_id = node.offset() + get_ith_bit(query, node.bit_pos());
        break;
      }
      case NODE_TERMINAL: {
        if (node.bit_size() > bit_size) {
          return found;
        } else if (node.bit_size() < bit_size) {
          Node leaf_node = nodes_->get(node.offset());
          Key stored_key = pool_->get_key(leaf_node.key_id());
          if (query.starts_with(stored_key)) {
            if (key_id) {
              *key_id = leaf_node.key_id();
            }
            if (key) {
              *key = stored_key;
            }
            found = true;
          }
        }
        node_id = node.offset() + (node.bit_size() < bit_size);
        break;
      }
    }
  }
}

bool Patricia<Bytes>::truncate() {
  Node * const root_node = &nodes_->get_value(ROOT_NODE_ID);
  pool_->truncate();
  *root_node = Node::dead_node();
  return true;
}

void Patricia<Bytes>::create_map(Storage *storage, uint32_t storage_node_id,
                                 const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    // TODO: Remove a magic number.
    nodes_.reset(NodeArray::create(storage, storage_node_id_, 1ULL << 41));
    pool_.reset(KeyPool<Bytes>::create(storage, storage_node_id_));
    // TODO: Remove a magic number.
    cache_.reset(Cache::create(storage, storage_node_id_, 1ULL << 20, -1));
    header_->nodes_storage_node_id = nodes_->storage_node_id();
    header_->pool_storage_node_id = pool_->storage_node_id();
    header_->cache_storage_node_id = cache_->storage_node_id();
    Node * const root_node = &nodes_->get_value(ROOT_NODE_ID);
    *root_node = Node::dead_node();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Patricia<Bytes>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  pool_.reset(KeyPool<Bytes>::open(storage, header_->pool_storage_node_id));
  cache_.reset(Cache::open(storage, header_->cache_storage_node_id));
}

uint64_t Patricia<Bytes>::get_ith_bit(KeyArg key, uint64_t bit_pos) {
  return (key[bit_pos / 8] >> (7 - (bit_pos % 8))) & 1;
}

}  // namespace map
}  // namespace grnxx
