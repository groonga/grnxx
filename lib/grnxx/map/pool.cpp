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
#include "grnxx/map/pool.hpp"

#include <cstring>
#include <exception>
#include <limits>

#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr uint64_t INVALID_UNIT_ID = std::numeric_limits<uint64_t>::max();

}  // namespace

template <typename T>
PoolHeader<T>::PoolHeader()
    : max_key_id(-1),
      num_keys(0),
      size(0),
      latest_available_unit_id(INVALID_UNIT_ID),
      page_storage_node_id(STORAGE_INVALID_NODE_ID),
      mutex() {}

template <typename T>
Pool<T>::Pool()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      pages_(),
      table_(nullptr),
      size_(0),
      queue_(),
      clock_() {}

template <typename T>
Pool<T>::~Pool() {}

template <typename T>
Pool<T> *Pool<T>::create(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<Pool> pool(new (std::nothrow) Pool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::Pool failed";
    throw MemoryError();
  }
  pool->create_pool(storage, storage_node_id);
  return pool.release();
}

template <typename T>
Pool<T> *Pool<T>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<Pool> pool(new (std::nothrow) Pool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::Pool failed";
    throw MemoryError();
  }
  pool->open_pool(storage, storage_node_id);
  return pool.release();
}

template <typename T>
void Pool<T>::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<Pool> pool(Pool::open(storage, storage_node_id));
  storage->unlink_node(storage_node_id);
}

template <typename T>
void Pool<T>::unset(int64_t key_id) {
  refresh_if_possible();
  void * const page = get_page(key_id / PAGE_SIZE);
  const uint64_t local_key_id = key_id % PAGE_SIZE;
  const uint64_t unit_id = local_key_id / UNIT_SIZE;
  const uint64_t validity_bit = 1ULL << (local_key_id % UNIT_SIZE);
  Unit * const unit = static_cast<Unit *>(page) - unit_id - 1;
  if (~unit->validity_bits & validity_bit) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  if (unit->validity_bits == ~uint64_t(0)) {
    unit->next_available_unit_id = header_->latest_available_unit_id;
    header_->latest_available_unit_id = unit_id;
  }
  unit->validity_bits &= ~validity_bit;
  --header_->num_keys;
}

template <typename T>
void Pool<T>::reset(int64_t key_id, KeyArg dest_key) {
  refresh_if_possible();
  void * const page = get_page(key_id / PAGE_SIZE);
  const uint64_t local_key_id = key_id % PAGE_SIZE;
  const Unit * const unit =
      static_cast<const Unit *>(page) - (local_key_id / UNIT_SIZE) - 1;
  if (~unit->validity_bits & (1ULL << (local_key_id % UNIT_SIZE))) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  static_cast<T *>(page)[local_key_id] = dest_key;
}

template <typename T>
int64_t Pool<T>::add(KeyArg key) {
  refresh_if_possible();
  if (header_->latest_available_unit_id == INVALID_UNIT_ID) {
    // Use a new unit.
    const int64_t next_key_id = header_->max_key_id + 1;
    if (next_key_id > MAX_KEY_ID) {
      GRNXX_ERROR() << "pool is full: next_key_id = " << next_key_id
                    << ", max_key_id = " << MAX_KEY_ID;
      throw LogicError();
    }
    reserve_key_id(next_key_id);
    void * const page = get_page(next_key_id / PAGE_SIZE);
    const uint64_t local_key_id = next_key_id % PAGE_SIZE;
    Unit * const unit =
        static_cast<Unit *>(page) - (local_key_id / UNIT_SIZE) - 1;
    unit->validity_bits = 1;
    unit->next_available_unit_id = INVALID_UNIT_ID;
    header_->latest_available_unit_id = next_key_id / UNIT_SIZE;
    static_cast<T *>(page)[local_key_id] = key;
    header_->max_key_id = next_key_id;
    ++header_->num_keys;
    return next_key_id;
  } else {
    // Use an existing unit.
    const uint64_t unit_id = header_->latest_available_unit_id;
    void * const page = get_page(unit_id * UNIT_SIZE / PAGE_SIZE);
    Unit * const unit =
        static_cast<Unit *>(page) - (unit_id % (PAGE_SIZE / UNIT_SIZE)) - 1;
    const uint8_t validity_bit_id = bit_scan_forward(~unit->validity_bits);
    const int64_t next_key_id = (unit_id * UNIT_SIZE) + validity_bit_id;
    if (next_key_id > MAX_KEY_ID) {
      GRNXX_ERROR() << "pool is full: next_key_id = " << next_key_id
                    << ", max_key_id = " << MAX_KEY_ID;
      throw LogicError();
    }
    unit->validity_bits |= 1ULL << validity_bit_id;
    if (unit->validity_bits == ~uint64_t(0)) {
      header_->latest_available_unit_id = unit->next_available_unit_id;
    }
    static_cast<T *>(page)[next_key_id % PAGE_SIZE] = key;
    if (next_key_id > header_->max_key_id) {
      header_->max_key_id = next_key_id;
    }
    ++header_->num_keys;
    return next_key_id;
  }
}

template <typename T>
void Pool<T>::defrag() {
  refresh_if_possible();
  // Nothing to do.
}

template <typename T>
void Pool<T>::sweep(Duration lifetime) {
  const Time threshold = clock_.now() - lifetime;
  while (!queue_.empty()) {
    QueueEntry &queue_entry = queue_.front();
    if (queue_entry.time <= threshold) {
      queue_.pop();
    }
  }
}

template <typename T>
void Pool<T>::create_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode header_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = header_node.id();
  try {
    header_ = static_cast<Header *>(header_node.body());
    *header_ = Header();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

template <typename T>
void Pool<T>::open_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode header_node = storage->open_node(storage_node_id);
  storage_node_id_ = header_node.id();
  header_ = static_cast<Header *>(header_node.body());
}

template <typename T>
void *Pool<T>::open_page(uint64_t page_id) {
  if (page_id >= (header_->size / PAGE_SIZE)) {
    GRNXX_ERROR() << "invalid argument: page_id = " << page_id
                  << ", table_size = " << (header_->size / PAGE_SIZE);
    throw LogicError();
  }
  if (!pages_[page_id]) {
    Lock lock(&header_->mutex);
    if (!pages_[page_id]) {
      // Open an existing full-size page.
      // Note that a small-size page is opened in refresh_page().
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        GRNXX_ERROR() << "not found: page_id = " << page_id;
        throw LogicError();
      }
      StorageNode page_node = storage_->open_node(table_[page_id]);
      pages_[page_id] =
          static_cast<Unit *>(page_node.body()) + (PAGE_SIZE / UNIT_SIZE);
    }
  }
  return pages_[page_id];
}

template <typename T>
void Pool<T>::reserve_key_id(int64_t key_id) {
  if (static_cast<uint64_t>(key_id) >= header_->size) {
    expand();
  }
  const uint64_t page_id = key_id / PAGE_SIZE;
  if (!pages_[page_id]) {
    // Note that "pages_[0]" is not nullptr if there is a small-size page
    // because it is opened in refresh_page().
    if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock lock(&header_->mutex);
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        // Create a full-size page.
        // Note that a small-size page is created in expand_page().
        const uint64_t page_node_size =
            (sizeof(Unit) * (PAGE_SIZE / UNIT_SIZE)) + (sizeof(T) * PAGE_SIZE);
        StorageNode page_node =
            storage_->create_node(storage_node_id_, page_node_size);
        table_[page_id] = page_node.id();
      }
    }
  }
}

template <typename T>
void Pool<T>::expand() {
  Lock lock(&header_->mutex);
  if (size_ < PAGE_SIZE) {
    // Create a small-size page or the first full-size page.
    expand_page();
    refresh_page();
  } else {
    // Create a table.
    expand_table();
    refresh_table();
  }
}

template <typename T>
void Pool<T>::expand_page() {
  const uint64_t new_size = (size_ == 0) ? MIN_PAGE_SIZE : (size_ * 2);
  const uint64_t page_node_size =
      (sizeof(Unit) * (new_size / UNIT_SIZE)) + (sizeof(T) * new_size);
  StorageNode page_node =
      storage_->create_node(storage_node_id_, page_node_size);
  if (size_ != 0) {
    // Copy data from the current page and unlink it.
    std::memcpy(static_cast<Unit *>(page_node.body()) + (size_ / UNIT_SIZE),
                static_cast<Unit *>(pages_[0]) - (size_ / UNIT_SIZE),
                page_node_size / 2);
    try {
      storage_->unlink_node(header_->page_storage_node_id);
    } catch (...) {
      storage_->unlink_node(page_node.id());
      throw;
    }
  }
  header_->page_storage_node_id = page_node.id();
  header_->size = new_size;
}

template <typename T>
void Pool<T>::expand_table() {
  const uint64_t old_table_size =
      (size_ <= PAGE_SIZE) ? 0 : (size_ / PAGE_SIZE);
  const uint64_t new_table_size =
      (old_table_size == 0) ? MIN_TABLE_SIZE : (old_table_size * 2);
  const uint64_t new_size = new_table_size * PAGE_SIZE;
  StorageNode table_node = storage_->create_node(
      storage_node_id_, sizeof(uint32_t) * new_table_size);
  uint32_t * const new_table = static_cast<uint32_t *>(table_node.body());
  uint64_t i;
  if (old_table_size == 0) {
    new_table[0] = header_->page_storage_node_id;
    i = 1;
  } else {
    for (i = 0; i < old_table_size; ++i) {
      new_table[i] = table_[i];
    }
  }
  for ( ; i < new_table_size; ++i) {
    new_table[i] = STORAGE_INVALID_NODE_ID;
  }
  header_->table_storage_node_id = table_node.id();
  header_->size = new_size;
}

template <typename T>
void Pool<T>::refresh() {
  Lock lock(&header_->mutex);
  if (size_ != header_->size) {
    if (header_->size <= PAGE_SIZE) {
      // Reopen a page because it is old.
      refresh_page();
    } else {
      // Reopen a table because it is old.
      refresh_table();
    }
    size_ = header_->size;
  }
}

template <typename T>
void Pool<T>::refresh_page() {
  StorageNode page_node =
      storage_->open_node(header_->page_storage_node_id);
  if (!pages_) {
    std::unique_ptr<void *[]> new_pages(new (std::nothrow) void *[1]);
    if (!new_pages) {
      GRNXX_ERROR() << "new void *[] failed: size = " << 1;
      throw MemoryError();
    }
    new_pages[0] =
        static_cast<Unit *>(page_node.body()) + (header_->size / UNIT_SIZE);
    pages_.swap(new_pages);
  } else {
    pages_[0] =
        static_cast<Unit *>(page_node.body()) + (header_->size / UNIT_SIZE);
  }
}

template <typename T>
void Pool<T>::refresh_table() {
  StorageNode table_node =
      storage_->open_node(header_->table_storage_node_id);
  uint32_t * const new_table = static_cast<uint32_t *>(table_node.body());
  const uint64_t new_table_size = header_->size / PAGE_SIZE;
  std::unique_ptr<void *[]> new_pages(
      new (std::nothrow) void *[new_table_size]);
  if (!new_pages) {
    GRNXX_ERROR() << "new void *[] failed: size = " << new_table_size;
    throw MemoryError();
  }
  // Initialize a new cache table.
  const uint64_t old_table_size = size_ / PAGE_SIZE;
  uint64_t i = 0;
  for ( ; i < old_table_size; ++i) {
    new_pages[i] = pages_[i];
  }
  for ( ; i < new_table_size; ++i) {
    new_pages[i] = nullptr;
  }
  pages_.swap(new_pages);
  // Keep an old cache table because another thread may read it.
  if (new_pages) {
    try {
      queue_.push(QueueEntry{ std::move(new_pages), clock_.now() });
    } catch (const std::exception &exception) {
      GRNXX_ERROR() << "std::queue::push() failed";
      throw StandardError(exception);
    }
  }
  table_ = new_table;
}

template class Pool<int8_t>;
template class Pool<int16_t>;
template class Pool<int32_t>;
template class Pool<int64_t>;
template class Pool<uint8_t>;
template class Pool<uint16_t>;
template class Pool<uint32_t>;
template class Pool<uint64_t>;
template class Pool<double>;
template class Pool<GeoPoint>;

namespace {

constexpr uint32_t MAX_PAGE_ID     = std::numeric_limits<uint32_t>::max() - 1;
constexpr uint32_t INVALID_PAGE_ID = MAX_PAGE_ID + 1;

}  // namespace

PoolHeader<Bytes>::PoolHeader()
    : size(0),
      next_offset(0),
      page_storage_node_id(STORAGE_INVALID_NODE_ID),
      index_pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      mutex() {}

PoolTableEntry::PoolTableEntry()
    : page_storage_node_id(STORAGE_INVALID_NODE_ID),
      size_in_use(0) {}

Pool<Bytes>::Pool()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      index_pool_(),
      pages_(),
      table_(nullptr),
      size_(0),
      queue_(),
      clock_() {}

Pool<Bytes>::~Pool() {}

Pool<Bytes> *Pool<Bytes>::create(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<Pool> pool(new (std::nothrow) Pool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::Pool failed";
    throw MemoryError();
  }
  pool->create_pool(storage, storage_node_id);
  return pool.release();
}

Pool<Bytes> *Pool<Bytes>::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<Pool> pool(new (std::nothrow) Pool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::Pool failed";
    throw MemoryError();
  }
  pool->open_pool(storage, storage_node_id);
  return pool.release();
}

void Pool<Bytes>::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<Pool> pool(Pool::open(storage, storage_node_id));
  storage->unlink_node(storage_node_id);
}

void Pool<Bytes>::unset(int64_t key_id) {
  uint64_t bytes_id;
  if (!index_pool_->get(key_id, &bytes_id)) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  refresh_if_possible();
  index_pool_->unset(key_id);
  unset_bytes(bytes_id);
}

void Pool<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  uint64_t src_bytes_id;
  if (!index_pool_->get(key_id, &src_bytes_id)) {
    GRNXX_ERROR() << "not found: key_id = " << key_id;
    throw LogicError();
  }
  refresh_if_possible();
  const uint64_t dest_bytes_id = add_bytes(dest_key);
  index_pool_->reset(key_id, dest_bytes_id);
  unset_bytes(src_bytes_id);
}

int64_t Pool<Bytes>::add(KeyArg key) {
  refresh_if_possible();
  const uint64_t bytes_id = add_bytes(key);
  try {
    const int64_t key_id = index_pool_->add(bytes_id);
    return key_id;
  } catch (...) {
    unset_bytes(bytes_id);
    throw;
  }
}

void Pool<Bytes>::defrag() {
  index_pool_->defrag();
  refresh_if_possible();
  if (header_->size <= PAGE_SIZE) {
    // Nothing to do.
    return;
  }
  // Keys in the active page should not be moved.
  const uint64_t offset_threshold =
      header_->next_offset - (header_->next_offset % PAGE_SIZE);
  // Keys in low-usage-rate pages should be moved.
  const uint32_t size_in_use_threshold =
      static_cast<uint32_t>(PAGE_SIZE * USAGE_RATE_THRESHOLD);
  const int64_t max_key_id = index_pool_->max_key_id();
  uint32_t prev_page_id = INVALID_PAGE_ID;
  uint8_t *page = nullptr;
  for (int64_t key_id = MIN_KEY_ID; key_id <= max_key_id; ++key_id) {
    // FIXME: "index_pool_->get/reset()" can be the bottleneck.
    uint64_t bytes_id;
    if (!index_pool_->get(key_id, &bytes_id)) {
      continue;
    }
    const uint64_t offset = get_offset(bytes_id);
    if (offset >= offset_threshold) {
      continue;
    }
    const uint32_t page_id = static_cast<uint32_t>(offset / PAGE_SIZE);
    if (page_id != prev_page_id) {
      if (table_[page_id].size_in_use >= size_in_use_threshold) {
        page = nullptr;
      } else {
        page = get_page(page_id);
      }
      prev_page_id = page_id;
    }
    if (!page) {
      continue;
    }
    const uint32_t bytes_size = get_size(bytes_id);
    const Bytes bytes(page + (offset % PAGE_SIZE), bytes_size);
    const uint64_t new_bytes_id = add_bytes(bytes);
    index_pool_->reset(key_id, new_bytes_id);
    table_[page_id].size_in_use -= bytes_size;
    if (table_[page_id].size_in_use == 0) {
      // Unlink a page if this operation makes it empty.
      storage_->unlink_node(table_[page_id].page_storage_node_id);
    }
  }
}

void Pool<Bytes>::sweep(Duration lifetime) {
  const Time threshold = clock_.now() - lifetime;
  while (!queue_.empty()) {
    QueueEntry &queue_entry = queue_.front();
    if (queue_entry.time <= threshold) {
      queue_.pop();
    }
  }
}

void Pool<Bytes>::create_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  const uint64_t header_node_size = sizeof(Header) + sizeof(TableEntry);
  StorageNode header_node =
      storage->create_node(storage_node_id, header_node_size);
  storage_node_id_ = header_node.id();
  try {
    header_ = static_cast<Header *>(header_node.body());
    *header_ = Header();
    index_pool_.reset(IndexPool::create(storage_, storage_node_id_));
    header_->index_pool_storage_node_id = index_pool_->storage_node_id();
    table_ = reinterpret_cast<TableEntry *>(header_ + 1);
    *table_ = TableEntry();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Pool<Bytes>::open_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Header *>(storage_node.body());
  index_pool_.reset(IndexPool::open(storage_,
                                    header_->index_pool_storage_node_id));
  table_ = reinterpret_cast<TableEntry *>(header_ + 1);
}

void Pool<Bytes>::unset_bytes(uint64_t bytes_id) {
  if (bytes_id == EMPTY_BYTES_ID) {
    // Nothing to do.
    return;
  }
  const uint64_t bytes_offset = get_offset(bytes_id);
  const uint32_t bytes_size = get_size(bytes_id);
  if ((bytes_offset + bytes_size) > header_->next_offset) {
    GRNXX_ERROR() << "invalid argument: bytes_offset = " << bytes_offset
                  << ", bytes_size = " << bytes_size
                  << ", next_offset = " << header_->next_offset;
    throw LogicError();
  }
  const uint32_t page_id = static_cast<uint32_t>(bytes_offset / PAGE_SIZE);
  TableEntry * const table_entry = &table_[page_id];
  if (bytes_size > table_entry->size_in_use) {
    GRNXX_ERROR() << "invalid argument: bytes_size = " << bytes_size
                  << ", size_in_use = " << table_entry->size_in_use;
    throw LogicError();
  }
  table_entry->size_in_use -= bytes_size;
  if ((table_entry->size_in_use == 0) &&
      (page_id != (header_->next_offset / PAGE_SIZE))) {
    // Unlink a page if this operation makes it empty.
    storage_->unlink_node(table_entry->page_storage_node_id);
  }
}

uint64_t Pool<Bytes>::add_bytes(KeyArg bytes) {
  const uint32_t bytes_size = static_cast<uint32_t>(bytes.size());
  if (bytes_size > MAX_KEY_SIZE) {
    GRNXX_ERROR() << "invalid argument: key_size = " << bytes_size
                  << ", max_key_size = " << MAX_KEY_SIZE;
    throw LogicError();
  }
  if (bytes_size == 0) {
    return EMPTY_BYTES_ID;
  }
  const uint64_t bytes_offset = reserve_space(bytes_size);
  const uint32_t page_id = static_cast<uint32_t>(bytes_offset / PAGE_SIZE);
  uint8_t * const page = get_page(page_id);
  std::memcpy(page + (bytes_offset % PAGE_SIZE), bytes.data(), bytes_size);
  TableEntry * const table_entry = &table_[page_id];
  table_entry->size_in_use += bytes_size;
  return get_bytes_id(bytes_offset, bytes_size);
}

uint8_t *Pool<Bytes>::open_page(uint32_t page_id) {
  if (page_id >= (header_->size / PAGE_SIZE)) {
    GRNXX_ERROR() << "invalid argument: page_id = " << page_id
                  << ", table_size = " << (header_->size / PAGE_SIZE);
    throw LogicError();
  }
  if (!pages_[page_id]) {
    Lock lock(&header_->mutex);
    if (!pages_[page_id]) {
      // Open an existing full-size page.
      // Note that a small-size page is opened in refresh_page().
      if (table_[page_id].page_storage_node_id == STORAGE_INVALID_NODE_ID) {
        GRNXX_ERROR() << "not found: page_id = " << page_id;
        throw LogicError();
      }
      StorageNode page_node =
          storage_->open_node(table_[page_id].page_storage_node_id);
      pages_[page_id] = static_cast<uint8_t *>(page_node.body());
    }
  }
  return pages_[page_id];
}

uint64_t Pool<Bytes>::reserve_space(uint32_t size) {
  uint64_t offset = header_->next_offset;
  const uint32_t page_size = static_cast<uint32_t>(
      (header_->size < PAGE_SIZE) ? header_->size : PAGE_SIZE);
  const uint32_t page_size_left = static_cast<uint32_t>(
      ((offset % PAGE_SIZE) == 0) ? 0 : (page_size - (offset % PAGE_SIZE)));
  if (size <= page_size_left) {
    header_->next_offset += size;
    return offset;
  }
  if ((header_->next_offset + size) > header_->size) {
    expand(size);
  }
  if (page_size == PAGE_SIZE) {
    offset += page_size_left;
  }
  const uint32_t page_id = static_cast<uint32_t>(offset / PAGE_SIZE);
  if (page_id > 0) {
    if ((table_[page_id - 1].size_in_use == 0) && (page_size_left != 0)) {
      // Unlink an empty page if it is fixed.
      storage_->unlink_node(table_[page_id - 1].page_storage_node_id);
    }
  }
  if (!pages_[page_id]) {
    // Note that "pages_[0]" is not nullptr if there is a small-size page
    // because it is opened in refresh_page().
    if (table_[page_id].page_storage_node_id == STORAGE_INVALID_NODE_ID) {
      Lock lock(&header_->mutex);
      if (table_[page_id].page_storage_node_id == STORAGE_INVALID_NODE_ID) {
        // Create a full-size page.
        // Note that a small-size page is created in expand_page().
        StorageNode page_node =
            storage_->create_node(storage_node_id_, PAGE_SIZE);
        table_[page_id].page_storage_node_id = page_node.id();
      }
    }
  }
  header_->next_offset = offset + size;
  return offset;
}

void Pool<Bytes>::expand(uint32_t additional_size) {
  Lock lock(&header_->mutex);
  if (size_ < PAGE_SIZE) {
    // Create a small-size page or the first full-size page.
    expand_page(additional_size);
    refresh_page();
  } else {
    // Create a table.
    expand_table(additional_size);
    refresh_table();
  }
}

void Pool<Bytes>::expand_page(uint32_t additional_size) {
  const uint64_t min_size = size_ + additional_size;
  uint64_t new_size = (size_ == 0) ? MIN_PAGE_SIZE : (size_ * 2);
  while (new_size < min_size) {
    new_size *= 2;
  }
  StorageNode page_node = storage_->create_node(storage_node_id_, new_size);
  if (size_ != 0) {
    // Copy data from the current page and unlink it.
    std::memcpy(page_node.body(), pages_[0], size_);
    try {
      storage_->unlink_node(header_->page_storage_node_id);
    } catch (...) {
      storage_->unlink_node(page_node.id());
      throw;
    }
  }
  table_[0].page_storage_node_id = page_node.id();
  header_->page_storage_node_id = page_node.id();
  header_->size = new_size;
}

void Pool<Bytes>::expand_table(uint32_t) {
  const uint64_t old_table_size = size_ / PAGE_SIZE;
  const uint64_t new_table_size = (old_table_size < MIN_TABLE_SIZE) ?
       MIN_TABLE_SIZE : (old_table_size * 2);
  const uint64_t new_size = new_table_size * PAGE_SIZE;
  StorageNode table_node = storage_->create_node(
      storage_node_id_, sizeof(TableEntry) * new_table_size);
  TableEntry * const new_table = static_cast<TableEntry *>(table_node.body());
  uint64_t i = 0;
  for ( ; i < old_table_size; ++i) {
    new_table[i] = table_[i];
  }
  for ( ; i < new_table_size; ++i) {
    new_table[i] = TableEntry();
  }
  header_->table_storage_node_id = table_node.id();
  header_->size = new_size;
}

void Pool<Bytes>::refresh() {
  if (size_ != header_->size) {
    Lock lock(&header_->mutex);
    if (header_->size <= PAGE_SIZE) {
      // Reopen a page because it is old.
      refresh_page();
    } else {
      // Reopen a table because it is old.
      refresh_table();
    }
    size_ = header_->size;
  }
}

void Pool<Bytes>::refresh_page() {
  StorageNode page_node =
      storage_->open_node(header_->page_storage_node_id);
  if (!pages_) {
    std::unique_ptr<uint8_t *[]> new_pages(new (std::nothrow) uint8_t *[1]);
    if (!new_pages) {
      GRNXX_ERROR() << "new uint8_t *[] failed: size = " << 1;
      throw MemoryError();
    }
    new_pages[0] = static_cast<uint8_t *>(page_node.body());
    pages_.swap(new_pages);
  } else {
    pages_[0] = static_cast<uint8_t *>(page_node.body());
  }
}

void Pool<Bytes>::refresh_table() {
  StorageNode table_node =
      storage_->open_node(header_->table_storage_node_id);
  TableEntry * const new_table =
      static_cast<TableEntry *>(table_node.body());
  const uint64_t new_table_size = header_->size / PAGE_SIZE;
  std::unique_ptr<uint8_t *[]> new_pages(
      new (std::nothrow) uint8_t *[new_table_size]);
  if (!new_pages) {
    GRNXX_ERROR() << "new uint8_t *[] failed: size = " << new_table_size;
    throw MemoryError();
  }
  // Initialize a new cache table.
  const uint64_t old_table_size = size_ / PAGE_SIZE;
  uint64_t i = 0;
  for ( ; i < old_table_size; ++i) {
    new_pages[i] = pages_[i];
  }
  for ( ; i < new_table_size; ++i) {
    new_pages[i] = nullptr;
  }
  pages_.swap(new_pages);
  // Keep an old cache table because another thread may read it.
  if (new_pages) {
    try {
      queue_.push(QueueEntry{ std::move(new_pages), clock_.now() });
    } catch (const std::exception &exception) {
      GRNXX_ERROR() << "std::queue::push() failed";
      throw StandardError(exception);
    }
  }
  table_ = new_table;
}

}  // namespace map
}  // namespace grnxx
