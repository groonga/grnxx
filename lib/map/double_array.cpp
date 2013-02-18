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
#include "double_array.hpp"

#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace map {

DoubleArrayHeader::DoubleArrayHeader()
  : map_header(),
    front_block_id(io::BLOCK_INVALID_ID),
    back_block_id(io::BLOCK_INVALID_ID) {
  map_header.type = MAP_DOUBLE_ARRAY;
}

DoubleArray::~DoubleArray() {}

DoubleArray *DoubleArray::create(const MapOptions &options, io::Pool pool) {
  std::unique_ptr<DoubleArray> da(new (std::nothrow) DoubleArray);
  if (!da) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    GRNXX_THROW();
  }
  da->create_double_array(options, pool);
  return da.release();
}

DoubleArray *DoubleArray::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<DoubleArray> da(new (std::nothrow) DoubleArray);
  if (!da) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    GRNXX_THROW();
  }
  da->open_double_array(pool, block_id);
  return da.release();
}

uint32_t DoubleArray::block_id() const {
  return block_info_->id();
}

bool DoubleArray::search(int64_t key_id, Slice *key) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  return front_->search(key_id, key);
}

bool DoubleArray::search(const Slice &key, int64_t *key_id) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  return front_->search(key, key_id);
}

bool DoubleArray::insert(const Slice &key, int64_t *key_id) {
  open_trie_if_needed();
  if (!front_) {
    // The first trie is created in the first insertion.
    da::TrieOptions options;
    std::unique_ptr<da::Trie> trie(da::Trie::create(options, pool_));
    trie.swap(front_);
    front_block_id_ = header_->front_block_id = front_->block_id();
  }
  try {
    return front_->insert(key, key_id);
  } catch (const da::TrieException &) {
    defrag_trie();
    return front_->insert(key, key_id);
  }
}

bool DoubleArray::remove(int64_t key_id) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  return front_->remove(key_id);
}

bool DoubleArray::remove(const Slice &key) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  return front_->remove(key);
}

bool DoubleArray::update(int64_t key_id, const Slice &dest_key) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  try {
    return front_->update(key_id, dest_key);
  } catch (const da::TrieException &) {
    defrag_trie();
    return front_->update(key_id, dest_key);
  }
}

bool DoubleArray::update(const Slice &src_key, const Slice &dest_key,
                         int64_t *key_id) {
  open_trie_if_needed();
  if (!front_) {
    return false;
  }
  try {
    return front_->update(src_key, dest_key, key_id);
  } catch (const da::TrieException &) {
    defrag_trie();
    return front_->update(src_key, dest_key, key_id);
  }
}

DoubleArray::DoubleArray()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    front_(nullptr),
    back_(nullptr),
    front_block_id_(io::BLOCK_INVALID_ID),
    inter_thread_mutex_() {}

void DoubleArray::create_double_array(const MapOptions &, io::Pool pool) {
  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(DoubleArrayHeader));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);
  *header_ = DoubleArrayHeader();
}

void DoubleArray::open_double_array(io::Pool pool, uint32_t block_id) {
  pool_ = pool;

  block_info_ = pool_.get_block_info(block_id);

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);

  // TODO: Check the format.
}

bool DoubleArray::open_trie_if_needed() {
  if (front_block_id_ == header_->front_block_id) {
    return false;
  }
  Lock lock(&inter_thread_mutex_);
  if (front_block_id_ == header_->front_block_id) {
    return false;
  }

  std::unique_ptr<da::Trie> trie(
      da::Trie::open(pool_, header_->front_block_id));
  trie.swap(front_);
  trie.swap(back_);
  front_block_id_ = front_->block_id();

  // TODO: Remove old tries.

  return true;
}

void DoubleArray::defrag_trie() {
  da::TrieOptions options;
  std::unique_ptr<da::Trie> trie(front_->defrag(options));
  trie.swap(front_);
  trie.swap(back_);
  front_block_id_ = header_->front_block_id = front_->block_id();
  header_->back_block_id = back_->block_id();

  // TODO: Remove old tries.
}

}  // namespace map
}  // namespace grnxx
