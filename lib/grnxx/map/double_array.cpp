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

#include "grnxx/bytes.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/bytes_store.hpp"
#include "grnxx/map/double_array/block.hpp"
#include "grnxx/map/double_array/entry.hpp"
#include "grnxx/map/double_array/header.hpp"
#include "grnxx/map/double_array/node.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

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

template <typename T>
Map<T> *DoubleArray<T>::create(Storage *, uint32_t, const MapOptions &) {
  GRNXX_ERROR() << "invalid combination";
  return nullptr;
}

template <typename T>
Map<T> *DoubleArray<T>::open(Storage *, uint32_t) {
  GRNXX_ERROR() << "invalid combination";
  return nullptr;
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
      entries_(),
      store_() {}

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
    return nullptr;
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
  return header_->max_key_id;
}

uint64_t DoubleArray<Bytes>::num_keys() const {
  return header_->num_keys;
}

bool DoubleArray<Bytes>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    // Out of range.
    return false;
  }
  return get_key(key_id, key) == DOUBLE_ARRAY_FOUND;
}

bool DoubleArray<Bytes>::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found or error.
    return false;
  }
  return remove(key);
}

bool DoubleArray<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found or error.
    return false;
  }
  return replace(src_key, dest_key);
}

bool DoubleArray<Bytes>::find(KeyArg key, int64_t *key_id) {
  uint64_t node_id = ROOT_NODE_ID;
  Node node;
  if (!nodes_->get(node_id, &node)) {
    // Error.
    return false;
  }
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node.is_leaf()) {
      // Found.
      break;
    }
    node_id = node.offset() ^ key[key_pos];
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
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
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
    if (!node.is_leaf()) {
      // Not found.
      return false;
    }
  }
  Key stored_key;
  if (get_key(node.key_id(), &stored_key) != DOUBLE_ARRAY_FOUND) {
    // Not found or error.
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
  if (find_leaf(key, &node, &key_pos) == DOUBLE_ARRAY_FAILED) {
    // Error.
    return false;
  }
  switch (insert_leaf(key, node, key_pos, &node)) {
    case DOUBLE_ARRAY_FOUND: {
      if (key_id) {
        *key_id = node->key_id();
      }
      return false;
    }
    case DOUBLE_ARRAY_INSERTED: {
      break;
    }
    default: {
      // Error.
      return false;
    }
  }
  const int64_t next_key_id = header_->next_key_id;
  if (next_key_id > MAP_MAX_KEY_ID) {
    // Error.
    GRNXX_ERROR() << "too many keys: next_key_id = " << next_key_id
                   << ", max_key_id = " << MAP_MAX_KEY_ID;
    return false;
  }
  Entry * const entry = entries_->get_pointer(next_key_id);
  if (!entry) {
    // Error.
    return false;
  }
  uint64_t bytes_id;
  if (!store_->add(key, &bytes_id)) {
    // Error.
    return false;
  }
  ++header_->num_keys;
  if (next_key_id > header_->max_key_id) {
    header_->max_key_id = next_key_id;
    header_->next_key_id = next_key_id + 1;
  } else {
    header_->next_key_id = entry->next();
  }
  *entry = Entry::valid_entry(bytes_id);
  node->set_key_id(next_key_id);
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

bool DoubleArray<Bytes>::remove(KeyArg key) {
  Node *node;
  uint64_t key_pos;
  if (find_leaf(key, &node, &key_pos) != DOUBLE_ARRAY_FOUND) {
    // Not found or error.
    return false;
  }
  Entry * const entry = entries_->get_pointer(node->key_id());
  if (!entry) {
    // Error.
    return false;
  }
  if (!*entry) {
    // Not found.
    return false;
  }
  Key stored_key;
  if (!store_->get(entry->bytes_id(), &stored_key)) {
    // Error.
    return false;
  }
  if (key.except_prefix(key_pos) != stored_key.except_prefix(key_pos)) {
    // Not found.
    return false;
  }
  *entry = Entry::invalid_entry(header_->next_key_id);
  node->set_offset(NODE_INVALID_OFFSET);
  header_->next_key_id = node->key_id();
  --header_->num_keys;
  return true;
}

bool DoubleArray<Bytes>::replace(KeyArg src_key, KeyArg dest_key,
                                 int64_t *key_id) {
  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    // Not found or error.
    return false;
  }
  if (!replace_key(src_key_id, src_key, dest_key)) {
    // Found or error.
    return false;
  }
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

bool DoubleArray<Bytes>::truncate() {
  Node * const node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!node) {
    // Error.
    return false;
  }
  node->set_child(NODE_INVALID_LABEL);
  node->set_offset(NODE_INVALID_OFFSET);
  header_->max_key_id = MAP_MIN_KEY_ID - 1;
  header_->num_keys = 0;
  header_->next_key_id = MAP_MIN_KEY_ID;
  return true;
}

bool DoubleArray<Bytes>::find_longest_prefix_match(KeyArg query,
                                                   int64_t *key_id,
                                                   Key *key) {
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  Node node;
  if (!nodes_->get(node_id, &node)) {
    // Error.
    return false;
  }
  uint64_t query_pos;
  for (query_pos = 0; query_pos < query.size(); ++query_pos) {
    if (node.is_leaf()) {
      Key stored_key;
      switch (get_key(node.key_id(), &stored_key)) {
        case DOUBLE_ARRAY_FOUND: {
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
          break;
        }
        case DOUBLE_ARRAY_NOT_FOUND: {
          break;
        }
        default: {
          // Error.
          return false;
        }
      }
      return found;
    }

    if (node.child() == NODE_TERMINAL_LABEL) {
      Node leaf_node;
      if (!nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL, &leaf_node)) {
        // Error.
        return false;
      }
      if (leaf_node.is_leaf()) {
        switch (get_key(leaf_node.key_id(), key)) {
          case DOUBLE_ARRAY_FOUND: {
            if (key_id) {
              *key_id = leaf_node.key_id();
            }
            found = true;
            break;
          }
          case DOUBLE_ARRAY_NOT_FOUND: {
            break;
          }
          default: {
            return false;
          }
        }
      }
    }

    node_id = node.offset() ^ query[query_pos];
    if (!nodes_->get(node_id, &node)) {
      // Error.
      return false;
    }
    if (node.label() != query[query_pos]) {
      return found;
    }
  }

  if (node.is_leaf()) {
    Key stored_key;
    switch (get_key(node.key_id(), &stored_key)) {
      case DOUBLE_ARRAY_FOUND: {
        if (stored_key.size() <= query.size()) {
          if (key_id) {
            *key_id = node.key_id();
          }
          if (key) {
            *key = stored_key;
          }
          found = true;
        }
        break;
      }
      case DOUBLE_ARRAY_NOT_FOUND: {
        break;
      }
      default: {
        return false;
      }
    }
  } else if (node.child() == NODE_TERMINAL_LABEL) {
    if (nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL, &node)) {
      switch (get_key(node.key_id(), key)) {
        case DOUBLE_ARRAY_FOUND: {
          if (key_id) {
            *key_id = node.key_id();
          }
          found = true;
          break;
        }
        case DOUBLE_ARRAY_NOT_FOUND: {
          break;
        }
        default: {
          return false;
        }
      }
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
    nodes_.reset(NodeArray::create(storage, storage_node_id_));
    siblings_.reset(SiblingArray::create(storage, storage_node_id_));
    blocks_.reset(BlockArray::create(storage, storage_node_id_));
    entries_.reset(EntryArray::create(storage, storage_node_id_));
    store_.reset(BytesStore::create(storage, storage_node_id_));
    header_->nodes_storage_node_id = nodes_->storage_node_id();
    header_->siblings_storage_node_id = siblings_->storage_node_id();
    header_->blocks_storage_node_id = blocks_->storage_node_id();
    header_->entries_storage_node_id = entries_->storage_node_id();
    header_->store_storage_node_id = store_->storage_node_id();
    Node * const root_node = reserve_node(ROOT_NODE_ID);
    if (!root_node) {
      // TODO
      storage->unlink_node(storage_node_id_);
      throw LogicError();
    }
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
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  siblings_.reset(
      SiblingArray::open(storage, header_->siblings_storage_node_id));
  blocks_.reset(BlockArray::open(storage, header_->blocks_storage_node_id));
  entries_.reset(EntryArray::open(storage, header_->entries_storage_node_id));
  store_.reset(BytesStore::open(storage, header_->store_storage_node_id));
}

DoubleArrayResult DoubleArray<Bytes>::get_key(int64_t key_id, Key *key) {
  Entry entry;
  if (!entries_->get(key_id, &entry)) {
    return DOUBLE_ARRAY_FAILED;
  }
  if (!entry) {
    return DOUBLE_ARRAY_NOT_FOUND;
  }
  if (!store_->get(entry.bytes_id(), key)) {
    return DOUBLE_ARRAY_FAILED;
  }
  return DOUBLE_ARRAY_FOUND;
}

bool DoubleArray<Bytes>::replace_key(int64_t key_id, KeyArg src_key,
                                     KeyArg dest_key) {
  Node *dest_node;
  uint64_t key_pos;
  if (find_leaf(dest_key, &dest_node, &key_pos) == DOUBLE_ARRAY_FAILED) {
    return false;
  }
  switch (insert_leaf(dest_key, dest_node, key_pos, &dest_node)) {
    case DOUBLE_ARRAY_FOUND: {
      return false;
    }
    case DOUBLE_ARRAY_INSERTED: {
      break;
    }
    default: {
      // Error.
      return false;
    }
  }
  Entry * const entry = entries_->get_pointer(key_id);
  if (!entry) {
    // Error.
    return false;
  }
  Node *src_node;
  if (find_leaf(src_key, &src_node, &key_pos) != DOUBLE_ARRAY_FOUND) {
    // Critical error.
    return false;
  }
  uint64_t bytes_id;
  if (!store_->add(dest_key, &bytes_id)) {
    // Error.
    return false;
  }
  dest_node->set_key_id(key_id);
  *entry = Entry::valid_entry(bytes_id);
  src_node->set_offset(NODE_INVALID_OFFSET);
  return true;
}

DoubleArrayResult DoubleArray<Bytes>::find_leaf(KeyArg key, Node **leaf_node,
                                                uint64_t *leaf_key_pos) {
  Node *node = nodes_->get_pointer(ROOT_NODE_ID);
  if (!node) {
    return DOUBLE_ARRAY_FAILED;
  }
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node->is_leaf()) {
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return DOUBLE_ARRAY_FOUND;
    }
    const uint64_t child_node_id = node->offset() ^ key[key_pos];
    Node * const child_node = nodes_->get_pointer(child_node_id);
    if (!child_node) {
      return DOUBLE_ARRAY_FAILED;
    }
    if (child_node->label() != key[key_pos]) {
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return DOUBLE_ARRAY_NOT_FOUND;
    }
    node = child_node;
  }
  *leaf_node = node;
  *leaf_key_pos = key_pos;
  if (node->is_leaf()) {
    return DOUBLE_ARRAY_FOUND;
  }
  if (node->child() != NODE_TERMINAL_LABEL) {
    return DOUBLE_ARRAY_NOT_FOUND;
  }
  const uint64_t node_id = node->offset() ^ NODE_TERMINAL_LABEL;
  node = nodes_->get_pointer(node_id);
  if (!node) {
    return DOUBLE_ARRAY_FAILED;
  }
  *leaf_node = node;
  return node->is_leaf() ? DOUBLE_ARRAY_FOUND : DOUBLE_ARRAY_NOT_FOUND;
}

DoubleArrayResult DoubleArray<Bytes>::insert_leaf(KeyArg key, Node *node,
                                                  uint64_t key_pos,
                                                  Node **leaf_node) {
  if (node->is_leaf()) {
    Key stored_key;
    const DoubleArrayResult result = get_key(node->key_id(), &stored_key);
    if (result != DOUBLE_ARRAY_FOUND) {
      // Not found or error.
      return result;
    }
    uint64_t i = key_pos;
    while ((i < key.size()) && (i < stored_key.size())) {
      if (key[i] != stored_key[i]) {
        break;
      }
      ++i;
    }
    if ((i == key.size()) && (i == stored_key.size())) {
      return DOUBLE_ARRAY_FOUND;
    }
    while (key_pos < i) {
      if (!insert_node(node, key[key_pos], &node)) {
        return DOUBLE_ARRAY_FAILED;
      }
      ++key_pos;
    }
    uint64_t labels[2];
    labels[0] = (key_pos < stored_key.size()) ?
        stored_key[key_pos] : NODE_TERMINAL_LABEL;
    labels[1] = (key_pos < key.size()) ? key[key_pos] : NODE_TERMINAL_LABEL;
    if (!separate(node, labels, leaf_node)) {
      return DOUBLE_ARRAY_FAILED;
    }
    return DOUBLE_ARRAY_INSERTED;
  } else if (node->label() == NODE_TERMINAL_LABEL) {
    *leaf_node = node;
    return DOUBLE_ARRAY_INSERTED;
  } else {
    const uint64_t label = (key_pos < key.size()) ?
        key[key_pos] : NODE_TERMINAL_LABEL;
    if (!resolve(node, label)) {
      return DOUBLE_ARRAY_FAILED;
    }
    if (!insert_node(node, label, leaf_node)) {
      return DOUBLE_ARRAY_FAILED;
    }
    return DOUBLE_ARRAY_INSERTED;
  }
}

bool DoubleArray<Bytes>::insert_node(Node *node, uint64_t label,
                                     Node **dest_node) {
  uint64_t offset = node->offset();
  if (node->is_leaf() || (offset == NODE_INVALID_OFFSET)) {
    if (!find_offset(&label, 1, &offset)) {
      // Error.
      return false;
    }
  }
  const uint64_t next_node_id = offset ^ label;
  uint8_t *next_sibling = siblings_->get_pointer(next_node_id);
  if (!next_sibling) {
    // Error.
    return false;
  }
  Node * const next_node = reserve_node(next_node_id);
  if (!next_node) {
    // Error.
    return false;
  }
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
  *dest_node = next_node;
  return true;
}

bool DoubleArray<Bytes>::separate(Node *node, uint64_t labels[2],
                                  Node **dest_node) {
  uint64_t offset;
  if (!find_offset(labels, 2, &offset)) {
    // Error.
    return false;
  }
  uint64_t node_ids[2] = { offset ^ labels[0], offset ^ labels[1] };
  Node *nodes[2];
  nodes[0] = reserve_node(node_ids[0]);
  if (!nodes[0]) {
    // Error.
    return false;
  }
  nodes[1] = reserve_node(node_ids[1]);
  if (!nodes[1]) {
    // Error.
    return false;
  }
  uint8_t * const sibling_block =
      siblings_->get_pointer(offset & ~(BLOCK_SIZE - 1));
  if (!sibling_block) {
    // Error.
    return false;
  }
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
  *dest_node = nodes[1];
  return true;
}

bool DoubleArray<Bytes>::resolve(Node *node, uint64_t label) {
  uint64_t offset = node->offset();
  if (offset == NODE_INVALID_OFFSET) {
    return true;
  }
  uint64_t dest_node_id = offset ^ label;
  Node * const dest_node = nodes_->get_pointer(dest_node_id);
  if (!dest_node) {
    // Error.
    return false;
  }
  if (dest_node->is_phantom()) {
    return true;
  }
  Node * const node_block = dest_node - (dest_node_id % BLOCK_SIZE);
  uint8_t * const sibling_block =
      siblings_->get_pointer(dest_node_id & ~(BLOCK_SIZE - 1));
  if (!sibling_block) {
    // Error.
    return false;
  }
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
  if (!find_offset(labels, num_labels + 1, &offset)) {
    // Error.
    return false;
  }
  if (!migrate_nodes(node, offset, labels, num_labels)) {
    // Error.
    return false;
  }
  return true;
}

bool DoubleArray<Bytes>::migrate_nodes(Node *node, uint64_t dest_offset,
                                       const uint64_t *labels,
                                       uint64_t num_labels) {
  const uint64_t src_offset = node->offset();
  Node * const src_node_block =
      nodes_->get_pointer(src_offset & ~(BLOCK_SIZE - 1));
  if (!src_node_block) {
    // Error.
    return false;
  }
  uint8_t * const src_sibling_block =
      siblings_->get_pointer(src_offset & ~(BLOCK_SIZE - 1));
  if (!src_sibling_block) {
    // Error.
    return false;
  }
  Node * const dest_node_block =
      nodes_->get_pointer(dest_offset & ~(BLOCK_SIZE - 1));
  if (!dest_node_block) {
    // Error.
    return false;
  }
  uint8_t * const dest_sibling_block =
      siblings_->get_pointer(dest_offset & ~(BLOCK_SIZE - 1));
  if (!dest_sibling_block) {
    // Error.
    return false;
  }
  for (uint64_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    Node * const src_node = &src_node_block[src_node_id % BLOCK_SIZE];
    uint8_t * const src_sibling = &src_sibling_block[src_node_id % BLOCK_SIZE];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
    Node * const dest_node = reserve_node(dest_node_id);
    if (!dest_node) {
      // Error.
      return false;
    }
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
  return true;
}

bool DoubleArray<Bytes>::find_offset(const uint64_t *labels,
                                     uint64_t num_labels,
                                     uint64_t *found_offset) {
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
      Block * const block = blocks_->get_pointer(block_id);
      if (!block) {
        // Error.
        return false;
      }
      Node * const node_block = nodes_->get_pointer(block_id * BLOCK_SIZE);
      if (!node_block) {
        // Error.
        return false;
      }
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
            *found_offset = (block_id * BLOCK_SIZE) | offset;
            return true;
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
  *found_offset = (header_->num_blocks * BLOCK_SIZE) ^ labels[0];
  return true;
}

auto DoubleArray<Bytes>::reserve_node(uint64_t node_id) -> Node * {
  const uint64_t block_id = node_id / BLOCK_SIZE;
  Block *block;
  if (node_id >= (header_->num_blocks * BLOCK_SIZE)) {
    block = reserve_block(block_id);
  } else {
    block = blocks_->get_pointer(block_id);
  }
  if (!block) {
    // Error.
    return nullptr;
  }
  Node * const node = nodes_->get_pointer(node_id);
  if (!node) {
    // Error.
    return nullptr;
  }
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
                  << ", blocks_size = " << blocks_->size();
    return nullptr;
  }
  Block * const block = blocks_->get_pointer(block_id);
  if (!block) {
    // Error.
    return nullptr;
  }
  Node * const node = nodes_->get_pointer(block_id * BLOCK_SIZE);
  if (!node) {
    // Error.
    return nullptr;
  }
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

bool DoubleArray<Bytes>::update_block_level(uint64_t block_id, Block *block,
                                            uint64_t level) {
  // TODO: If set_block_level() fails, the block gets lost.
  if (!unset_block_level(block_id, block) ||
      !set_block_level(block_id, block, level)) {
    // Error.
    return false;
  }
  return true;
}

bool DoubleArray<Bytes>::set_block_level(uint64_t block_id, Block *block,
                                         uint64_t level) {
  if (header_->latest_blocks[level] == BLOCK_INVALID_ID) {
    // The block becomes the only one member of the level group.
    block->set_next(block_id);
    block->set_prev(block_id);
    header_->latest_blocks[level] = block_id;
  } else {
    // The block is appended to the level group.
    const uint64_t next_block_id = header_->latest_blocks[level];
    Block * const next_block = blocks_->get_pointer(next_block_id);
    if (!next_block) {
      // Error.
      return false;
    }
    const uint64_t prev_block_id = next_block->prev();
    Block * const prev_block = blocks_->get_pointer(prev_block_id);
    if (!prev_block) {
      // Error.
      return false;
    }
    block->set_next(next_block_id);
    block->set_prev(prev_block_id);
    prev_block->set_next(block_id);
    next_block->set_prev(block_id);
  }
  block->set_level(level);
  block->set_failure_count(0);
  return true;
}

bool DoubleArray<Bytes>::unset_block_level(uint64_t block_id, Block *block) {
  const uint64_t level = block->level();
  const uint64_t next_block_id = block->next();
  const uint64_t prev_block_id = block->prev();
  if (next_block_id == prev_block_id) {
    // The level group becomes empty.
    header_->latest_blocks[level] = BLOCK_INVALID_ID;
  } else {
    Block * const next_block = blocks_->get_pointer(next_block_id);
    if (!next_block) {
      // Error.
      return false;
    }
    Block * const prev_block = blocks_->get_pointer(prev_block_id);
    if (!prev_block) {
      // Error.
      return false;
    }
    prev_block->set_next(next_block_id);
    next_block->set_prev(prev_block_id);
    if (block_id == header_->latest_blocks[level]) {
      // The next block becomes the latest block of the level group.
      header_->latest_blocks[level] = next_block_id;
    }
  }
  return true;
}

}  // namespace map
}  // namespace grnxx
