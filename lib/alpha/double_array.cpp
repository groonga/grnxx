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
    chunks_block_id_(io::BLOCK_INVALID_ID),
    entries_block_id_(io::BLOCK_INVALID_ID),
    keys_block_id_(io::BLOCK_INVALID_ID),
    root_node_id_(0),
    num_chunks_(0),
    num_phantoms_(0),
    leaders_(),
    inter_process_mutex_() {
  for (uint32_t i = 0; i < DOUBLE_ARRAY_MAX_CHUNK_LEVEL; ++i) {
    leaders_[i] = DOUBLE_ARRAY_INVALID_LEADER;
  }
}

DoubleArrayKey::DoubleArrayKey(uint64_t id, const char *address,
                               uint64_t length)
  : id_low_(static_cast<uint32_t>(id)),
    id_high_(static_cast<uint8_t>(id >> 32)),
    buf_{ '\0', '\0', '\0' } {
  std::memcpy(buf_, address, length);
}

DoubleArrayImpl::~DoubleArrayImpl() {
  if (!initialized_) try {
    if (header_->nodes_block_id() != io::BLOCK_INVALID_ID) {
      nodes_.unlink(pool_, header_->nodes_block_id());
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
                             uint64_t *key_offset) {
  uint64_t node_id = root_node_id();
  uint64_t query_pos = 0;
  if (!search_leaf(ptr, length, node_id, query_pos)) {
    return false;
  }

  const DoubleArrayNode node = nodes_[node_id];
  if (!node.is_leaf()) {
    return false;
  }

  if (node.key_length() == length) {
    if (get_key(node.key_offset()).equals_to(ptr, length, query_pos)) {
      if (key_offset) {
        *key_offset = node.key_offset();
      }
      return true;
    }
  }
  return false;
}

DoubleArrayImpl::DoubleArrayImpl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    nodes_(),
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
  chunks_.open(pool_, header_->chunks_block_id());
  entries_.open(pool_, header_->entries_block_id());
  keys_.open(pool_, header_->keys_block_id());
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

  // FIXME: Remove the magic numbers (TERMINAL_LABEL).
  const uint64_t next = node.offset() ^ 0x100;
  if (nodes_[next].label() != 0x100) {
    return false;
  }
  node_id = next;
  return nodes_[next].is_leaf();
}

void DoubleArrayImpl::reserve_node(uint64_t node_id) {
  if (node_id >= header_->num_nodes()) {
//    reserve_block(node_id / DOUBLE_ARRAY_BLOCK_SIZE);
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
//      update_chunk_level(chunk_id, chunk.level() + 1);
    }
  }
  chunk.set_num_phantoms(chunk.num_phantoms() - 1);

  node.set_is_phantom(false);

//  GRN_DAT_DEBUG_THROW_IF(node.offset() != INVALID_OFFSET);
//  GRN_DAT_DEBUG_THROW_IF(node.label() != INVALID_LABEL);

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
  node.set_offset(DOUBLE_ARRAY_INVALID_OFFSET);
  node.set_is_phantom(true);

  for (uint64_t i = begin; i < end; ++i) {
    node.set_prev((i - 1) & DOUBLE_ARRAY_CHUNK_MASK);
    node.set_next((i + 1) & DOUBLE_ARRAY_CHUNK_MASK);
    nodes_[i] = node;
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
