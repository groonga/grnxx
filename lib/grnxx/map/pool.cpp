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

constexpr uint64_t INVALID_UNIT_ID = ~0ULL;

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
      queue_() {}

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
  void * const page = get_page(key_id);
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
  void * const page = get_page(key_id);
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
  if (header_->latest_available_unit_id == INVALID_UNIT_ID) {
    // Use a new unit.
    const int64_t next_key_id = header_->max_key_id + 1;
    if (next_key_id > MAX_KEY_ID) {
      GRNXX_ERROR() << "pool is full: next_key_id = " << next_key_id
                    << ", max_key_id = " << MAX_KEY_ID;
      throw LogicError();
    }
    void * const page = reserve_page(next_key_id);
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
    void * const page = get_page(unit_id * UNIT_SIZE);
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
  // Nothing to do.
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
void *Pool<T>::open_page(int64_t key_id) {
  if (static_cast<uint64_t>(key_id) >= header_->size) {
    GRNXX_ERROR() << "invalid argument: key_id = " << key_id
                  << ", size = " << header_->size;
    throw LogicError();
  }
  Lock lock(&header_->mutex);
  refresh_pool();
  const uint64_t page_id = key_id / PAGE_SIZE;
  if (!pages_[page_id]) {
    // Open an existing full-size page.
    // Note that a small-size page is opened in refresh_pool().
    if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
      GRNXX_ERROR() << "not found: page_id = " << page_id;
      throw LogicError();
    }
    StorageNode page_node = storage_->open_node(table_[page_id]);
    pages_[page_id] =
        static_cast<Unit *>(page_node.body()) + (PAGE_SIZE / UNIT_SIZE);
  }
  return pages_[page_id];
}

template <typename T>
void *Pool<T>::reserve_page(int64_t key_id) {
  if (static_cast<uint64_t>(key_id) >= header_->size) {
    expand_pool();
  }
  const uint64_t page_id = key_id / PAGE_SIZE;
  if (!pages_[page_id]) {
    Lock lock(&header_->mutex);
    if (!pages_[page_id]) {
      void *page;
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        // Create a full-size page.
        // Note that a small-size page is created in expand_pool().
        const uint64_t page_node_size =
            (sizeof(Unit) * (PAGE_SIZE / UNIT_SIZE)) + (sizeof(T) * PAGE_SIZE);
        StorageNode page_node =
            storage_->create_node(storage_node_id_, page_node_size);
        table_[page_id] = page_node.id();
        page = page_node.body();
      } else {
        // Open an existing full-size page.
        // Note that a small-size page is opened in refresh_pool().
        StorageNode page_node = storage_->open_node(table_[page_id]);
        page = page_node.body();
      }
      pages_[page_id] = static_cast<Unit *>(page) + (PAGE_SIZE / UNIT_SIZE);
    }
  }
  return pages_[page_id];
}

template <typename T>
void Pool<T>::expand_pool() {
  Lock lock(&header_->mutex);
  refresh_pool();
  uint64_t new_size = (size_ == 0) ? MIN_PAGE_SIZE : (size_ * 2);
  if (new_size <= PAGE_SIZE) {
    // Create a small-size page.
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
  } else {
    // Create a table.
    const uint64_t old_table_size =
        (size_ <= PAGE_SIZE) ? 0 : (size_ / PAGE_SIZE);
    const uint64_t new_table_size =
        (old_table_size == 0) ? MIN_TABLE_SIZE : (old_table_size * 2);
    new_size = new_table_size * PAGE_SIZE;
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
  }
  header_->size = new_size;
  refresh_pool();
}

template <typename T>
void Pool<T>::refresh_pool() {
  if (size_ == header_->size) {
    // Nothing to do.
    return;
  }
  if (header_->size <= PAGE_SIZE) {
    // Reopen a page because it is old.
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
  } else {
    // Reopen a table because it is old.
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
        // TODO: Time must be added.
        queue_.push(std::move(new_pages));
      } catch (const std::exception &exception) {
        GRNXX_ERROR() << "std::queue::push() failed";
        throw StandardError(exception);
      }
    }
    table_ = new_table;
  }
  size_ = header_->size;
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

}  // namespace map
}  // namespace grnxx
