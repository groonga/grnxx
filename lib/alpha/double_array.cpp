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
    inter_process_mutex_() {}

DoubleArrayKey::DoubleArrayKey(uint64_t id, const char *address,
                               uint64_t length)
  : id_low_(static_cast<uint32_t>(id)),
    id_high_(static_cast<uint8_t>(id >> 32)),
    buf_() {
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
      if (key_offset != NULL) {
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

  // TODO

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

}  // namespace alpha
}  // namespace grnxx
