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
#include "double_array.hpp"

#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace alpha {

DoubleArrayCreate DOUBLE_ARRAY_CREATE;
DoubleArrayOpen DOUBLE_ARRAY_OPEN;

DoubleArrayHeader::DoubleArrayHeader()
  : nodes_block_id_(io::BLOCK_INVALID_ID),
    siblings_block_id_(io::BLOCK_INVALID_ID),
    chunks_block_id_(io::BLOCK_INVALID_ID),
    entries_block_id_(io::BLOCK_INVALID_ID),
    keys_block_id_(io::BLOCK_INVALID_ID),
    root_node_id_(0),
    total_key_length_(0),
    next_key_id_(0),
    next_key_pos_(0),
    max_key_id_(-1),
    num_keys_(0),
    num_chunks_(0),
    num_phantoms_(0),
    num_zombies_(0),
    leaders_(),
    inter_process_mutex_(MUTEX_UNLOCKED) {
  for (uint32_t i = 0; i < DOUBLE_ARRAY_MAX_CHUNK_LEVEL; ++i) {
    leaders_[i] = DOUBLE_ARRAY_INVALID_LEADER;
  }
}

DoubleArrayKey::DoubleArrayKey(uint64_t id, const void *address,
                               uint64_t length)
  : id_low_(static_cast<uint32_t>(id)),
    id_high_(static_cast<uint8_t>(id >> 32)),
    buf_{ '\0', '\0', '\0' } {
  std::memcpy(buf_, address, length);
}

DoubleArrayImpl::~DoubleArrayImpl() {
  if (!initialized_) try {
    // Allocated blocks are unlinked if initialization failed.
    if (header_->nodes_block_id() != io::BLOCK_INVALID_ID) {
      nodes_.unlink(pool_, header_->nodes_block_id());
    }
    if (header_->siblings_block_id() != io::BLOCK_INVALID_ID) {
      siblings_.unlink(pool_, header_->siblings_block_id());
    }
    if (header_->chunks_block_id() != io::BLOCK_INVALID_ID) {
      chunks_.unlink(pool_, header_->chunks_block_id());
    }
    if (header_->entries_block_id() != io::BLOCK_INVALID_ID) {
      entries_.unlink(pool_, header_->entries_block_id());
    }
    if (header_->keys_block_id() != io::BLOCK_INVALID_ID) {
      keys_.unlink(pool_, header_->keys_block_id());
    }
    if (block_info_) {
      pool_.free_block(*block_info_);
    }
  } catch (...) {
  }
}

std::unique_ptr<DoubleArrayImpl> DoubleArrayImpl::create(io::Pool pool) {
  std::unique_ptr<DoubleArrayImpl> impl(new (std::nothrow) DoubleArrayImpl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::alpha::DoubleArrayImpl failed";
    GRNXX_THROW();
  }
  impl->create_double_array(pool);
  return impl;
}

std::unique_ptr<DoubleArrayImpl> DoubleArrayImpl::open(io::Pool pool,
                                                       uint32_t block_id) {
  std::unique_ptr<DoubleArrayImpl> impl(new (std::nothrow) DoubleArrayImpl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::alpha::DoubleArrayImpl failed";
    GRNXX_THROW();
  }
  impl->open_double_array(pool, block_id);
  return impl;
}

bool DoubleArrayImpl::search(const uint8_t *ptr, uint64_t length,
                             uint64_t *key_pos) {
  uint64_t node_id = root_node_id();
  uint64_t query_pos = 0;
  if (!search_leaf(ptr, length, node_id, query_pos)) {
    return false;
  }

  // Note that nodes_[node_id] might be updated by other threads/processes.
  const DoubleArrayNode node = nodes_[node_id];
  if (!node.is_leaf()) {
    return false;
  }

  if (node.key_length() == length) {
    if (get_key(node.key_pos()).equals_to(ptr, length, query_pos)) {
      if (key_pos) {
        *key_pos = node.key_pos();
      }
      return true;
    }
  }
  return false;
}

bool DoubleArrayImpl::insert(const uint8_t *ptr, uint64_t length,
                             uint64_t *key_pos) {
  // TODO: Exclusive access control is required.

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, INSERTING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF(!ptr && (length != 0));

  uint64_t node_id = root_node_id();
  uint64_t query_pos = 0;

  search_leaf(ptr, length, node_id, query_pos);
  if (!insert_leaf(ptr, length, node_id, query_pos)) {
    if (key_pos) {
      *key_pos = nodes_[node_id].key_pos();
    }
    return false;
  }

  const int64_t new_key_id = header_->next_key_id();
  const uint64_t new_key_pos = append_key(ptr, length, new_key_id);

  header_->set_total_key_length(header_->total_key_length() + length);
  header_->set_num_keys(header_->num_keys() + 1);

  // TODO: The first key ID should be fixed to 0 or 1.
  //       Currently, 0 is used.
  if (new_key_id > header_->max_key_id()) {
    header_->set_max_key_id(new_key_id);
    header_->set_next_key_id(new_key_id + 1);
  } else {
    header_->set_next_key_id(entries_[new_key_id].next());
  }

  entries_[new_key_id].set_key(new_key_pos, length);
  nodes_[node_id].set_key(new_key_pos, length);
  if (key_pos) {
    *key_pos = new_key_pos;
  }
  return true;
}

bool DoubleArrayImpl::remove(int64_t key_id) {
  // TODO: Exclusive access control is required.
  if ((key_id < 0) || (key_id > header_->max_key_id())) {
    return false;
  }
  const DoubleArrayEntry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const DoubleArrayKey &key = get_key(entry.key_pos());
  return remove_key(static_cast<const uint8_t *>(
      key.ptr()), entry.key_length());
}

bool DoubleArrayImpl::remove(const uint8_t *ptr, uint64_t length) {
  // TODO: Exclusive access control is required.
//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, REMOVING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF((ptr == nullptr) && (length != 0));
  return remove_key(ptr, length);
}

bool DoubleArrayImpl::update(int64_t key_id, const uint8_t *ptr,
                             uint64_t length, uint64_t *key_pos) {
  // TODO: Exclusive access control is required.
  if ((key_id < 0) || (key_id > header_->max_key_id())) {
    return false;
  }
  const DoubleArrayEntry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const DoubleArrayKey &key = get_key(entry.key_pos());
  return update_key(static_cast<const uint8_t *>(key.ptr()), entry.key_length(),
                    key_id, ptr, length, key_pos);
}

bool DoubleArrayImpl::update(const uint8_t *src_ptr, uint64_t src_length,
                             const uint8_t *dest_ptr, uint64_t dest_length,
                             uint64_t *key_pos) {
  // TODO: Exclusive access control is required.
  uint64_t src_key_id;
  if (!search(src_ptr, src_length, &src_key_id)) {
    return false;
  }
  return update_key(src_ptr, src_length, src_key_id,
                    dest_ptr, dest_length, key_pos);
}

bool DoubleArrayImpl::update_key(const uint8_t *src_ptr, uint64_t src_length,
                                 uint64_t src_key_id, const uint8_t *dest_ptr,
                                 uint64_t dest_length, uint64_t *key_pos) {
//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, UPDATING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF((ptr == NULL) && (length != 0));

  uint64_t node_id = root_node_id();
  uint64_t query_pos = 0;

  search_leaf(dest_ptr, dest_length, node_id, query_pos);
  if (!insert_leaf(dest_ptr, dest_length, node_id, query_pos)) {
    if (key_pos) {
      *key_pos = nodes_[node_id].key_pos();
    }
    return false;
  }

  const uint64_t new_key_pos = append_key(dest_ptr, dest_length, src_key_id);
  header_->set_total_key_length(
      header_->total_key_length() + dest_length - src_length);
  entries_[src_key_id].set_key(new_key_pos, dest_length);
  nodes_[node_id].set_key(new_key_pos, dest_length);
  if (key_pos) {
    *key_pos = new_key_pos;
  }

  node_id = root_node_id();
  query_pos = 0;
  if (!search_leaf(src_ptr, src_length, node_id, query_pos)) {
    // TODO: Unexpected error!
  }
  nodes_[node_id].set_offset(DOUBLE_ARRAY_INVALID_OFFSET);
  return true;
}

DoubleArrayImpl::DoubleArrayImpl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    nodes_(),
    siblings_(),
    chunks_(),
    entries_(),
    keys_(),
    initialized_(false) {}

void DoubleArrayImpl::create_double_array(io::Pool pool) {
  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(DoubleArrayHeader));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);
  *header_ = DoubleArrayHeader();

  recycler_ = pool_.mutable_recycler();

  nodes_.create(pool);
  header_->set_nodes_block_id(nodes_.block_id());
  siblings_.create(pool);
  header_->set_siblings_block_id(siblings_.block_id());
  chunks_.create(pool);
  header_->set_chunks_block_id(chunks_.block_id());
  entries_.create(pool);
  header_->set_entries_block_id(entries_.block_id());
  keys_.create(pool);
  header_->set_keys_block_id(keys_.block_id());

  reserve_node(root_node_id());
  nodes_[DOUBLE_ARRAY_INVALID_OFFSET].set_is_origin(true);

  initialized_ = true;
}

void DoubleArrayImpl::open_double_array(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  initialized_ = true;

  block_info_ = pool_.get_block_info(block_id);

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);

  // TODO: Check the format.

  recycler_ = pool_.mutable_recycler();

  nodes_.open(pool_, header_->nodes_block_id());
  siblings_.open(pool_, header_->siblings_block_id());
  chunks_.open(pool_, header_->chunks_block_id());
  entries_.open(pool_, header_->entries_block_id());
  keys_.open(pool_, header_->keys_block_id());
}

bool DoubleArrayImpl::remove_key(const uint8_t *ptr, uint64_t length) {
  uint64_t node_id = root_node_id();
  uint64_t query_pos = 0;
  if (!search_leaf(ptr, length, node_id, query_pos)) {
    return false;
  }

  if (length != nodes_[node_id].key_length()) {
    return false;
  }

  const uint64_t key_pos = nodes_[node_id].key_pos();
  const DoubleArrayKey &key = get_key(key_pos);
  if (!key.equals_to(ptr, length, query_pos)) {
    return false;
  }

  const uint64_t key_id = key.id();
  nodes_[node_id].set_offset(DOUBLE_ARRAY_INVALID_OFFSET);
  entries_[key_id].set_next(header_->next_key_id());

  header_->set_next_key_id(key_id);
  header_->set_total_key_length(header_->total_key_length() - length);
  header_->set_num_keys(header_->num_keys() - 1);
  return true;
}

bool DoubleArrayImpl::search_leaf(const uint8_t *ptr, uint64_t length,
                                  uint64_t &node_id, uint64_t &query_pos) {
  for ( ; query_pos < length; ++query_pos) {
    const DoubleArrayNode node = nodes_[node_id];
    if (node.is_leaf()) {
      return true;
    }

    const uint64_t next = node.offset() ^ ptr[query_pos];
    if (nodes_[next].label() != ptr[query_pos]) {
      return false;
    }
    node_id = next;
  }

  const DoubleArrayNode node = nodes_[node_id];
  if (node.is_leaf()) {
    return true;
  }

  if (node.child() != DOUBLE_ARRAY_TERMINAL_LABEL) {
    return false;
  }
  node_id = node.offset() ^ DOUBLE_ARRAY_TERMINAL_LABEL;
  return nodes_[node_id].is_leaf();
}

bool DoubleArrayImpl::insert_leaf(const uint8_t *ptr, uint64_t length,
                                  uint64_t &node_id, uint64_t query_pos) {
  const DoubleArrayNode node = nodes_[node_id];
  if (node.is_leaf()) {
    const DoubleArrayKey &key = get_key(node.key_pos());
    uint64_t i = query_pos;
    while ((i < length) && (i < node.key_length())) {
      if (ptr[i] != key[i]) {
        break;
      }
      ++i;
    }
    if ((i == length) && (i == node.key_length())) {
      return false;
    }
    // TODO
//    GRN_DAT_THROW_IF(SIZE_ERROR, num_keys() >= max_num_keys());
//    GRN_DAT_DEBUG_THROW_IF(next_key_id() > max_num_keys());

    for (uint64_t j = query_pos; j < i; ++j) {
      node_id = insert_node(node_id, ptr[j]);
    }
    node_id = separate(ptr, length, node_id, i);
    return true;
  } else if (node.label() == DOUBLE_ARRAY_TERMINAL_LABEL) {
    return true;
  } else {
    // TODO
//    GRN_DAT_THROW_IF(SIZE_ERROR, num_keys() >= max_num_keys());
    const uint16_t label = (query_pos < length) ?
        static_cast<uint16_t>(ptr[query_pos]) : DOUBLE_ARRAY_TERMINAL_LABEL;
    if ((node.offset() == DOUBLE_ARRAY_INVALID_OFFSET) ||
        !nodes_[node.offset() ^ label].is_phantom()) {
      // The offset of this node must be updated.
      resolve(node_id, label);
    }
    // The new node will be the leaf node associated with the query.
    node_id = insert_node(node_id, label);
    return true;
  }
}

uint64_t DoubleArrayImpl::insert_node(uint64_t node_id, uint16_t label) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(label > MAX_LABEL);

  const DoubleArrayNode node = nodes_[node_id];
  uint64_t offset;
  if (node.is_leaf() || (node.offset() == DOUBLE_ARRAY_INVALID_OFFSET)) {
    offset = find_offset(&label, 1);
  } else {
    offset = node.offset();
  }

  const uint64_t next = offset ^ label;
  reserve_node(next);

  nodes_[next].set_label(label);
  if (node.is_leaf()) {
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
    nodes_[next].set_key(node.key_pos(), node.key_length());
    // TODO: Must be update at once.
  } else if (node.offset() == DOUBLE_ARRAY_INVALID_OFFSET) {
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
//  } else {
//    GRN_DAT_DEBUG_THROW_IF(!nodes_[offset].is_origin());
  }
  nodes_[node_id].set_offset(offset);

  const uint16_t child_label = nodes_[node_id].child();
//  GRN_DAT_DEBUG_THROW_IF(child_label == label);
  if (child_label == DOUBLE_ARRAY_INVALID_LABEL) {
    nodes_[node_id].set_child(label);
  } else if ((label == DOUBLE_ARRAY_TERMINAL_LABEL) ||
             ((child_label != DOUBLE_ARRAY_TERMINAL_LABEL) &&
              (label < child_label))) {
    // The next node becomes the first child.
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset ^ child_label).is_phantom());
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset ^ child_label).label() != child_label);
    siblings_[next] = child_label;
    nodes_[next].set_has_sibling(true);
    nodes_[node_id].set_child(label);
  } else {
    uint64_t prev = offset ^ child_label;
//    GRN_DAT_DEBUG_THROW_IF(nodes_[prev).label() != child_label);
    uint16_t sibling_label = nodes_[prev].has_sibling() ?
        siblings_[prev] : DOUBLE_ARRAY_INVALID_LABEL;
    while (label > sibling_label) {
      prev = offset ^ sibling_label;
//      GRN_DAT_DEBUG_THROW_IF(nodes_[prev].label() != sibling_label);
      sibling_label = nodes_[prev].has_sibling() ?
          siblings_[prev] : DOUBLE_ARRAY_INVALID_LABEL;
    }
//    GRN_DAT_DEBUG_THROW_IF(label == sibling_label);
    siblings_[next] = siblings_[prev];
    siblings_[prev] = label;
    nodes_[next].set_has_sibling(nodes_[prev].has_sibling());
    nodes_[prev].set_has_sibling(true);
  }
  return next;
}

uint64_t DoubleArrayImpl::append_key(const uint8_t *ptr, uint64_t length,
                                     uint64_t key_id) {
  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, key_id > max_num_keys());

  uint64_t key_pos = header_->next_key_pos();
  const uint64_t key_size = DoubleArrayKey::estimate_size(length);

  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, key_size > (key_buf_size() - key_pos));
  const uint64_t size_left_in_page =
      (~key_pos + 1) % DOUBLE_ARRAY_KEYS_PAGE_SIZE;
  if (size_left_in_page < key_size) {
    key_pos += size_left_in_page;
  }
  new (&keys_[key_pos]) DoubleArrayKey(key_id, ptr, length);

  header_->set_next_key_pos(key_pos + key_size);
  return key_pos;
}

uint64_t DoubleArrayImpl::separate(const uint8_t *ptr, uint64_t length,
                                   uint64_t node_id, uint64_t i) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(!nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(i > length);

  const DoubleArrayNode node = nodes_[node_id];
  const DoubleArrayKey &key = get_key(node.key_pos());

  uint16_t labels[2];
  labels[0] = (i < node.key_length()) ?
      static_cast<uint16_t>(key[i]) : DOUBLE_ARRAY_TERMINAL_LABEL;
  labels[1] = (i < length) ?
      static_cast<uint16_t>(ptr[i]) : DOUBLE_ARRAY_TERMINAL_LABEL;
//  GRN_DAT_DEBUG_THROW_IF(labels[0] == labels[1]);

  const uint64_t offset = find_offset(labels, 2);

  uint64_t next = offset ^ labels[0];
  reserve_node(next);
//  GRN_DAT_DEBUG_THROW_IF(nodes_[offset).is_origin());

  nodes_[next].set_label(labels[0]);
  nodes_[next].set_key(node.key_pos(), node.key_length());

  next = offset ^ labels[1];
  reserve_node(next);

  nodes_[next].set_label(labels[1]);

  nodes_[offset].set_is_origin(true);
  nodes_[node_id].set_offset(offset);

  if ((labels[0] == DOUBLE_ARRAY_TERMINAL_LABEL) ||
      ((labels[1] != DOUBLE_ARRAY_TERMINAL_LABEL) &&
       (labels[0] < labels[1]))) {
    siblings_[offset ^ labels[0]] = labels[1];
    nodes_[offset ^ labels[0]].set_has_sibling(true);
    nodes_[node_id].set_child(labels[0]);
  } else {
    siblings_[offset ^ labels[1]] = labels[0];
    nodes_[offset ^ labels[1]].set_has_sibling(true);
    nodes_[node_id].set_child(labels[1]);
  }
  return next;
}

void DoubleArrayImpl::resolve(uint64_t node_id, uint16_t label) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(label > MAX_LABEL);

  uint64_t offset = nodes_[node_id].offset();
  if (offset != DOUBLE_ARRAY_INVALID_OFFSET) {
    uint16_t labels[DOUBLE_ARRAY_MAX_LABEL + 1];
    uint16_t num_labels = 0;

    uint16_t next_label = nodes_[node_id].child();
    // FIXME: How to check if the node has children or not.
//    GRN_DAT_DEBUG_THROW_IF(next_label == INVALID_LABEL);
    while (next_label != DOUBLE_ARRAY_INVALID_LABEL) {
//      GRN_DAT_DEBUG_THROW_IF(next_label > MAX_LABEL);
      labels[num_labels++] = next_label;
      next_label = nodes_[offset ^ next_label].has_sibling() ?
          siblings_[offset ^ next_label] : DOUBLE_ARRAY_INVALID_LABEL;
    }
//    GRN_DAT_DEBUG_THROW_IF(num_labels == 0);

    labels[num_labels] = label;
    offset = find_offset(labels, num_labels + 1);
    migrate_nodes(node_id, offset, labels, num_labels);
  } else {
    offset = find_offset(&label, 1);
    if (offset >= header_->num_nodes()) {
//      GRN_DAT_DEBUG_THROW_IF((offset / BLOCK_SIZE) != num_blocks());
      reserve_chunk(header_->num_chunks());
    }
    nodes_[offset].set_is_origin(true);
    nodes_[node_id].set_offset(offset);
  }
}

void DoubleArrayImpl::migrate_nodes(uint64_t node_id, uint64_t dest_offset,
                                    const uint16_t *labels,
                                    uint16_t num_labels) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(labels == nullptr);
//  GRN_DAT_DEBUG_THROW_IF(num_labels == 0);
//  GRN_DAT_DEBUG_THROW_IF(num_labels > (DOUBLE_ARRAY_MAX_LABEL + 1));

  const uint64_t src_offset = nodes_[node_id].offset();
//  GRN_DAT_DEBUG_THROW_IF(src_offset == DOUBLE_ARRAY_INVALID_OFFSET);
//  GRN_DAT_DEBUG_THROW_IF(!nodes_[src_offset].is_origin());

  for (uint16_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
//    GRN_DAT_DEBUG_THROW_IF(ith_node(src_node_id).is_phantom());
//    GRN_DAT_DEBUG_THROW_IF(ith_node(src_node_id).label() != labels[i]);

    reserve_node(dest_node_id);
    DoubleArrayNode dest_node = nodes_[src_node_id];
    dest_node.set_is_origin(nodes_[dest_node_id].is_origin());
    nodes_[dest_node_id] = dest_node;
    siblings_[dest_node_id] = siblings_[src_node_id];
  }
  header_->set_num_zombies(header_->num_zombies() + num_labels);

//  GRN_DAT_DEBUG_THROW_IF(nodes_[dest_offset].is_origin());
  nodes_[dest_offset].set_is_origin(true);
  nodes_[node_id].set_offset(dest_offset);
}

uint64_t DoubleArrayImpl::find_offset(const uint16_t *labels,
                                      uint16_t num_labels) {
//  GRN_DAT_DEBUG_THROW_IF(labels == nullptr);
//  GRN_DAT_DEBUG_THROW_IF(num_labels == 0);
//  GRN_DAT_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  // Chunks are tested in descending order of level. Basically, lower level
  // chunks contain more phantom nodes.
  uint32_t level = 1;
  while (num_labels >= (1U << level)) {
    ++level;
  }
  level = (level < DOUBLE_ARRAY_MAX_CHUNK_LEVEL) ?
      (DOUBLE_ARRAY_MAX_CHUNK_LEVEL - level) : 0;

  uint64_t chunk_count = 0;
  do {
    uint64_t leader = header_->ith_leader(level);
    if (leader == DOUBLE_ARRAY_INVALID_LEADER) {
      // This level group is skipped because it is empty.
      continue;
    }

    uint64_t chunk_id = leader;
    do {
      const DoubleArrayChunk &chunk = chunks_[chunk_id];
//      GRN_DAT_DEBUG_THROW_IF(chunk.level() != level);

      const uint64_t first =
          (chunk_id * DOUBLE_ARRAY_CHUNK_SIZE) | chunk.first_phantom();
      uint64_t node_id = first;
      do {
//        GRN_DAT_DEBUG_THROW_IF(!nodes_[node_id=]).is_phantom());
        const uint64_t offset = node_id ^ labels[0];
        if (!nodes_[offset].is_origin()) {
          uint16_t i = 1;
          for ( ; i < num_labels; ++i) {
            if (!nodes_[offset ^ labels[i]].is_phantom()) {
              break;
            }
          }
          if (i >= num_labels) {
            return offset;
          }
        }
        node_id = (chunk_id * DOUBLE_ARRAY_CHUNK_SIZE) | nodes_[node_id].next();
      } while (node_id != first);

      const uint64_t prev = chunk_id;
      const uint64_t next = chunk.next();
      chunk_id = next;
      chunks_[prev].set_failure_count(chunks_[prev].failure_count() + 1);

      // The level of a chunk is updated when this function fails many times,
      // actually DOUBLE_ARRAY_MAX_FAILURE_COUNT times, in that chunk.
      if (chunks_[prev].failure_count() == DOUBLE_ARRAY_MAX_FAILURE_COUNT) {
        update_chunk_level(prev, level + 1);
        if (next == leader) {
          break;
        } else {
          // Note that the leader might be updated in the level update.
          leader = header_->ith_leader(level);
          continue;
        }
      }
    } while ((++chunk_count < DOUBLE_ARRAY_MAX_CHUNK_COUNT) &&
             (chunk_id != leader));
  } while ((chunk_count < DOUBLE_ARRAY_MAX_CHUNK_COUNT) && (level-- != 0));

  return header_->num_nodes() ^ labels[0];
}

void DoubleArrayImpl::reserve_node(uint64_t node_id) {
  if (node_id >= header_->num_nodes()) {
    reserve_chunk(node_id / DOUBLE_ARRAY_CHUNK_SIZE);
  }

  DoubleArrayNode &node = nodes_[node_id];
//  GRN_DAT_DEBUG_THROW_IF(!node.is_phantom());

  const uint64_t chunk_id = node_id / DOUBLE_ARRAY_CHUNK_SIZE;
  DoubleArrayChunk &chunk = chunks_[chunk_id];
//  GRN_DAT_DEBUG_THROW_IF(chunk.num_phantoms() == 0);

  const uint64_t next = (chunk_id * DOUBLE_ARRAY_CHUNK_SIZE) | node.next();
  const uint64_t prev = (chunk_id * DOUBLE_ARRAY_CHUNK_SIZE) | node.prev();
//  GRN_DAT_DEBUG_THROW_IF(next >= header_->num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(prev >= header_->num_nodes());

  if ((node_id & DOUBLE_ARRAY_CHUNK_MASK) == chunk.first_phantom()) {
    // The first phantom node is removed from the chunk and the second phantom
    // node comes first.
    chunk.set_first_phantom(next & DOUBLE_ARRAY_CHUNK_MASK);
  }

  nodes_[next].set_prev(prev & DOUBLE_ARRAY_CHUNK_MASK);
  nodes_[prev].set_next(next & DOUBLE_ARRAY_CHUNK_MASK);

  if (chunk.level() != DOUBLE_ARRAY_MAX_CHUNK_LEVEL) {
    const uint64_t threshold =
        uint64_t(1) << ((DOUBLE_ARRAY_MAX_CHUNK_LEVEL - chunk.level() - 1) * 2);
    if (chunk.num_phantoms() == threshold) {
      update_chunk_level(chunk_id, chunk.level() + 1);
    }
  }
  chunk.set_num_phantoms(chunk.num_phantoms() - 1);

  node.set_is_phantom(false);

//  GRN_DAT_DEBUG_THROW_IF(node.offset() != DOUBLE_ARRAY_INVALID_OFFSET);
//  GRN_DAT_DEBUG_THROW_IF(node.label() != DOUBLE_ARRAY_INVALID_LABEL);

  header_->set_num_phantoms(header_->num_phantoms() - 1);
}

void DoubleArrayImpl::reserve_chunk(uint64_t chunk_id) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id != num_chunks());
  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, chunk_id >= max_num_chunks());

  header_->set_num_chunks(chunk_id + 1);
  chunks_[chunk_id].set_failure_count(0);
  chunks_[chunk_id].set_first_phantom(0);
  chunks_[chunk_id].set_num_phantoms(DOUBLE_ARRAY_CHUNK_SIZE);

  const uint64_t begin = chunk_id * DOUBLE_ARRAY_CHUNK_SIZE;
  const uint64_t end = begin + DOUBLE_ARRAY_CHUNK_SIZE;
//  GRN_DAT_DEBUG_THROW_IF(end != num_nodes());

  DoubleArrayNode node;
  node.set_is_phantom(true);

  for (uint64_t i = begin; i < end; ++i) {
    node.set_prev((i - 1) & DOUBLE_ARRAY_CHUNK_MASK);
    node.set_next((i + 1) & DOUBLE_ARRAY_CHUNK_MASK);
    nodes_[i] = node;
    siblings_[i] = '\0';
  }

  // The level of the new chunk is 0.
  set_chunk_level(chunk_id, 0);
  header_->set_num_phantoms(header_->num_phantoms() + DOUBLE_ARRAY_CHUNK_SIZE);
}

void DoubleArrayImpl::update_chunk_level(uint64_t chunk_id, uint32_t level) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(level > DOUBLE_ARRAY_MAX_CHUNK_LEVEL);

  unset_chunk_level(chunk_id);
  set_chunk_level(chunk_id, level);
}

void DoubleArrayImpl::set_chunk_level(uint64_t chunk_id, uint32_t level) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(level > DOUBLE_ARRAY_MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->ith_leader(level);
  if (leader == DOUBLE_ARRAY_INVALID_LEADER) {
    // The chunk becomes the only one member of the level group.
    chunks_[chunk_id].set_next(chunk_id);
    chunks_[chunk_id].set_prev(chunk_id);
    header_->set_ith_leader(level, chunk_id);
  } else {
    // The chunk is appended to the level group, in practice.
    const uint64_t next = leader;
    const uint64_t prev = chunks_[leader].prev();
//    GRN_DAT_DEBUG_THROW_IF(next >= num_chunks());
//    GRN_DAT_DEBUG_THROW_IF(prev >= num_chunks());
    chunks_[chunk_id].set_next(next);
    chunks_[chunk_id].set_prev(prev);
    chunks_[next].set_prev(chunk_id);
    chunks_[prev].set_next(chunk_id);
  }
  chunks_[chunk_id].set_level(level);
  chunks_[chunk_id].set_failure_count(0);
}

void DoubleArrayImpl::unset_chunk_level(uint64_t chunk_id) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunk());

  const uint32_t level = chunks_[chunk_id].level();
//  GRN_DAT_DEBUG_THROW_IF(level > DOUBLE_ARRAY_MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->ith_leader(level);
//  GRN_DAT_DEBUG_THROW_IF(leader == DOUBLE_ARRAY_INVALID_LEADER);

  const uint64_t next = chunks_[chunk_id].next();
  const uint64_t prev = chunks_[chunk_id].prev();
//  GRN_DAT_DEBUG_THROW_IF(next >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(prev >= num_chunks());

  if (next == chunk_id) {
    // The level group becomes empty.
    header_->set_ith_leader(level, DOUBLE_ARRAY_INVALID_LEADER);
  } else {
    chunks_[next].set_prev(prev);
    chunks_[prev].set_next(next);
    if (chunk_id == leader) {
      // The second chunk becomes the leader of the level group.
      header_->set_ith_leader(level, next);
    }
  }
}

}  // namespace alpha
}  // namespace grnxx
