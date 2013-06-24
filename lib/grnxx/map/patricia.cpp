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
#include "grnxx/geo_point.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/patricia/header.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr uint64_t ROOT_NODE_ID = 0;

using patricia::NODE_INVALID_OFFSET;

using patricia::NODE_DEAD;
using patricia::NODE_LEAF;
using patricia::NODE_BRANCH;
using patricia::NODE_TERMINAL;

}  // namespace

template <typename T>
Patricia<T>::Patricia()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      nodes_(),
      keys_() {}

template <typename T>
Patricia<T>::~Patricia() {}

template <typename T>
Patricia<T> *Patricia<T>::create(Storage *storage,
                                 uint32_t storage_node_id,
                                 const MapOptions &options) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

template <typename T>
Patricia<T> *Patricia<T>::open(Storage *storage,
                               uint32_t storage_node_id) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
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
  return keys_->max_key_id();
}

template <typename T>
uint64_t Patricia<T>::num_keys() const {
  return keys_->num_keys();
}

template <typename T>
bool Patricia<T>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > keys_->max_key_id())) {
    // Out of range.
    return false;
  }
  bool bit;
  if (!keys_->get_bit(key_id, &bit)) {
    // Error.
    return false;
  }
  if (!bit) {
    // Not found.
    return false;
  }
  if (!keys_->get_key(key_id, key)) {
    // Error.
    return false;
  }
  return true;
}

template <typename T>
bool Patricia<T>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found or error.
    return false;
  }
  uint64_t node_id = ROOT_NODE_ID;
  Node *prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
    switch (node->status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        if (node->key_id() != key_id) {
          // Not found.
          return false;
        }
        keys_->unset(key_id);
        if (prev_node) {
          Node * const sibling_node = node + (node_id ^ 1) - node_id;
          *prev_node = *sibling_node;
        } else {
          *node = Node::dead_node();
        }
        return true;
      }
      case NODE_BRANCH: {
        node_id = node->offset() + get_ith_bit(key, node->bit_pos());
        break;
      }
    }
    prev_node = node;
  }
}

//template <typename T>
//bool Patricia<T>::reset(int64_t key_id, KeyArg dest_key) {
//  // TODO
//  return false;
//}

template <typename T>
bool Patricia<T>::find(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  uint64_t node_id = ROOT_NODE_ID;
  for ( ; ; ) {
    Node node;
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
    switch (node.status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        Key stored_key;
        if (!keys_->get_key(node.key_id(), &stored_key)) {
          // Error.
          return false;
        }
        if (!Helper<T>::equal_to(normalized_key, stored_key)) {
          // Not found.
          return false;
        }
        if (key_id) {
          *key_id = node.key_id();
        }
        return true;
      }
      case NODE_BRANCH: {
        node_id = node.offset() + get_ith_bit(normalized_key, node.bit_pos());
        break;
      }
    }
  }
}

template <typename T>
bool Patricia<T>::add(KeyArg key, int64_t *key_id) {
  const Key normalized_key = Helper<T>::normalize(key);
  uint64_t node_id = ROOT_NODE_ID;
  Node * node;
  for (node = nodes_->get_pointer(node_id);
       node && (node->status() != NODE_LEAF);
       node = nodes_->get_pointer(node_id)) {
    switch (node->status()) {
      case NODE_DEAD: {
        // The patricia is empty.
        int64_t next_key_id;
        if (!keys_->add(normalized_key, &next_key_id)) {
          // Error.
          return false;
        }
        *node = Node::leaf_node(next_key_id);
        if (key_id) {
          *key_id = next_key_id;
        }
        return true;
      }
      case NODE_BRANCH: {
        node_id = node->offset() + get_ith_bit(normalized_key, node->bit_pos());
        break;
      }
    }
  }
  if (!node) {
    // Error.
    return false;
  }
  // "node" points to a leaf node.
  Key stored_key;
  if (!keys_->get_key(node->key_id(), &stored_key)) {
    // Error.
    return false;
  }
  const uint64_t count = count_common_prefix_bits(normalized_key, stored_key);
  if (count == (sizeof(Key) * 8)) {
    // Found.
    if (key_id) {
      *key_id = node->key_id();
    }
    return false;
  }
  Node * const next_nodes = nodes_->get_pointer(header_->next_node_id);
  node_id = ROOT_NODE_ID;
  for (node = nodes_->get_pointer(node_id);
       node && (node->status() != NODE_LEAF);
       node = nodes_->get_pointer(node_id)) {
    switch (node->status()) {
      case NODE_BRANCH: {
        if (count <= node->bit_pos()) {
          int64_t next_key_id;
          if (!keys_->add(normalized_key, &next_key_id)) {
            // Error.
            return false;
          }
          // Create a branch node.
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
        node_id = node->offset();
        if (node->bit_pos() < count) {
          node_id += get_ith_bit(normalized_key, node->bit_pos());
        }
        break;
      }
    }
  }
  if (!node) {
    // Error.
    return false;
  }
  int64_t next_key_id;
  if (!keys_->add(normalized_key, &next_key_id)) {
    // Error.
    return false;
  }
  // Create a branch node.
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
  Node * prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
    switch (node->status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        Key stored_key;
        if (!keys_->get_key(node->key_id(), &stored_key)) {
          // Error.
          return false;
        }
        if (!Helper<T>::equal_to(normalized_key, stored_key)) {
          // Not found.
          return false;
        }
        keys_->unset(node->key_id());
        if (prev_node) {
          Node * const sibling_node = node + (node_id ^ 1) - node_id;
          *prev_node = *sibling_node;
        } else {
          *node = Node::dead_node();
        }
        return true;
      }
      case NODE_BRANCH: {
        node_id = node->offset() + get_ith_bit(normalized_key, node->bit_pos());
        break;
      }
    }
    prev_node = node;
  }
}

//template <typename T>
//bool Patricia<T>::replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id) {
//  // TODO
//  return false;
//}

template <typename T>
bool Patricia<T>::truncate() {
  Node * const root_node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!root_node) {
    return false;
  }
  if (!keys_->truncate()) {
    return false;
  }
  *root_node = Node::dead_node();
  return true;
}

template <typename T>
bool Patricia<T>::create_map(Storage *storage, uint32_t storage_node_id,
                             const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  *header_ = Header();
  nodes_.reset(NodeArray::create(storage, storage_node_id_));
  keys_.reset(KeyStore<T>::create(storage, storage_node_id_));
  if (!nodes_ || !keys_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->nodes_storage_node_id = nodes_->storage_node_id();
  header_->keys_storage_node_id = keys_->storage_node_id();
  Node * const root_node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!root_node) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  *root_node = Node::dead_node();
  return true;
}

template <typename T>
bool Patricia<T>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  // TODO: Check the format.
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  keys_.reset(KeyStore<T>::open(storage, header_->keys_storage_node_id));
  if (!nodes_ || !keys_) {
    return false;
  }
  return true;
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
      keys_() {}

Patricia<Bytes>::~Patricia() {}

Patricia<Bytes> *Patricia<Bytes>::create(Storage *storage,
                                         uint32_t storage_node_id,
                                         const MapOptions &options) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->create_map(storage, storage_node_id, options)) {
    return nullptr;
  }
  return map.release();
}

Patricia<Bytes> *Patricia<Bytes>::open(Storage *storage,
                                       uint32_t storage_node_id) {
  std::unique_ptr<Patricia> map(new (std::nothrow) Patricia);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::Patricia failed";
    return nullptr;
  }
  if (!map->open_map(storage, storage_node_id)) {
    return nullptr;
  }
  return map.release();
}

uint32_t Patricia<Bytes>::storage_node_id() const {
  return storage_node_id_;
}

MapType Patricia<Bytes>::type() const {
  return MAP_PATRICIA;
}

int64_t Patricia<Bytes>::max_key_id() const {
  return keys_->max_key_id();
}

uint64_t Patricia<Bytes>::num_keys() const {
  return keys_->num_keys();
}

bool Patricia<Bytes>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > keys_->max_key_id())) {
    // Out of range.
    return false;
  }
  bool bit;
  if (!keys_->get_bit(key_id, &bit)) {
    // Error.
    return false;
  }
  if (!bit) {
    // Not found.
    return false;
  }
  if (!keys_->get_key(key_id, key)) {
    // Error.
    return false;
  }
  return true;
}

bool Patricia<Bytes>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found or error.
    return false;
  }
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  Node *prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
    switch (node->status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        if (node->key_id() != key_id) {
          // Not found.
          return false;
        }
        keys_->unset(key_id);
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

//bool Patricia<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
//  // TODO
//  return false;
//}

bool Patricia<Bytes>::find(KeyArg key, int64_t *key_id) {
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  for ( ; ; ) {
    Node node;
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
    switch (node.status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        Key stored_key;
        if (!keys_->get_key(node.key_id(), &stored_key)) {
          // Error.
          return false;
        }
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
  }
}

bool Patricia<Bytes>::add(KeyArg key, int64_t *key_id) {
  uint64_t node_id = ROOT_NODE_ID;
  Node *node = nodes_->get_pointer(node_id);
  if (!node) {
    // Error.
    return false;
  }
  if (node->status() == NODE_DEAD) {
    // The patricia is empty.
    int64_t next_key_id;
    if (!keys_->add(key, &next_key_id)) {
      // Error.
      return false;
    }
    *node = Node::leaf_node(next_key_id);
    if (key_id) {
      *key_id = next_key_id;
    }
    return true;
  }
  const uint64_t bit_size = key.size() * 8;
  Node *history[8];
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
    node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
    history[++depth % 8] = node;
  }
  // Find a leaf node.
  while (node->status() != NODE_LEAF) {
    node_id = node->offset();
    node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
  }
  // Count the number of the common prefix bits.
  Key stored_key;
  if (!keys_->get_key(node->key_id(), &stored_key)) {
    // Error.
    return false;
  }
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
      return false;
    }
    node = history[depth % 8];
    Node * const next_nodes = nodes_->get_pointer(header_->next_node_id);
    if (!next_nodes) {
      return false;
    }
    int64_t next_key_id;
    if (!keys_->add(key, &next_key_id)) {
      // Error.
      return false;
    }
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
    return true;
  }
  count = (count * 8) + 7 - bit_scan_reverse(key[count] ^ stored_key[count]);
  // Find the branching point in "history".
  int min_depth = (depth < 8) ? 0 : depth - 7;
  while (--depth >= min_depth) {
    node = history[depth % 8];
    if (node->status() == NODE_BRANCH) {
      if (node->bit_pos() < count) {
        break;
      }
    } else if (node->bit_size() < count) {
      break;
    }
  }
  if (depth >= min_depth) {
    // The branching point exists in "history".
    node = history[(depth + 1) % 8];
    Node * const next_nodes = nodes_->get_pointer(header_->next_node_id);
    if (!next_nodes) {
      return false;
    }
    int64_t next_key_id;
    if (!keys_->add(key, &next_key_id)) {
      // Error.
      return false;
    }
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
    return true;
  }
  // Find the branching point with the naive method.
  node_id = ROOT_NODE_ID;
  for ( ; ; ) {
    node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
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
  Node * const next_nodes = nodes_->get_pointer(header_->next_node_id);
  if (!next_nodes) {
    // Error.
    return false;
  }
  int64_t next_key_id;
  if (!keys_->add(key, &next_key_id)) {
    // Error.
    return false;
  }
  // Create a branch node.
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
  return true;
}

bool Patricia<Bytes>::remove(KeyArg key) {
  const uint64_t bit_size = key.size() * 8;
  uint64_t node_id = ROOT_NODE_ID;
  Node * prev_node = nullptr;
  for ( ; ; ) {
    Node * const node = nodes_->get_pointer(node_id);
    if (!node) {
      // Error.
      return false;
    }
    switch (node->status()) {
      case NODE_DEAD: {
        // Not found.
        return false;
      }
      case NODE_LEAF: {
        Key stored_key;
        if (!keys_->get_key(node->key_id(), &stored_key)) {
          // Error.
          return false;
        }
        if (stored_key != key) {
          // Not found.
          return false;
        }
        keys_->unset(node->key_id());
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

//bool Patricia<Bytes>::replace(KeyArg src_key, KeyArg dest_key,
//                              int64_t *key_id) {
//  // TODO
//  return false;
//}

bool Patricia<Bytes>::find_longest_prefix_match(KeyArg query, int64_t *key_id,
                                                Key *key) {
  const uint64_t bit_size = query.size() * 8;
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  for ( ; ; ) {
    Node node;
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
    switch (node.status()) {
      case NODE_DEAD: {
        // Not found.
        return found;
      }
      case NODE_LEAF: {
        Key stored_key;
        if (!keys_->get_key(node.key_id(), &stored_key)) {
          // Error.
          return false;
        }
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
          Node leaf_node;
          if (!nodes_->get(node.offset(), &leaf_node)) {
            // Error.
            return false;
          }
          Key stored_key;
          if (!keys_->get_key(leaf_node.key_id(), &stored_key)) {
            // Error.
            return false;
          }
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
  Node * const root_node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!root_node) {
    return false;
  }
  if (!keys_->truncate()) {
    return false;
  }
  *root_node = Node::dead_node();
  return true;
}

bool Patricia<Bytes>::create_map(Storage *storage, uint32_t storage_node_id,
                                 const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  *header_ = Header();
  nodes_.reset(NodeArray::create(storage, storage_node_id_));
  keys_.reset(KeyStore<Bytes>::create(storage, storage_node_id_));
  if (!nodes_ || !keys_) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  header_->nodes_storage_node_id = nodes_->storage_node_id();
  header_->keys_storage_node_id = keys_->storage_node_id();
  Node * const root_node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!root_node) {
    storage->unlink_node(storage_node_id_);
    return false;
  }
  *root_node = Node::dead_node();
  return true;
}

bool Patricia<Bytes>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    return false;
  }
  storage_node_id_ = storage_node_id;
  header_ = static_cast<Header *>(storage_node.body());
  // TODO: Check the format.
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  keys_.reset(KeyStore<Bytes>::open(storage, header_->keys_storage_node_id));
  if (!nodes_ || !keys_) {
    return false;
  }
  return true;
}

uint64_t Patricia<Bytes>::get_ith_bit(KeyArg key, uint64_t bit_pos) {
  return (key[bit_pos / 8] >> (7 - (bit_pos % 8))) & 1;
}

}  // namespace map
}  // namespace grnxx
