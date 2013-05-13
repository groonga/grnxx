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
#include "grnxx/array/array_2d.hpp"

#include <cstring>
#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/mutex.hpp"

namespace grnxx {

struct Array2DHeader {
  uint64_t value_size;
  uint64_t page_size;
  uint64_t table_size;
  uint32_t has_default_value;
  uint32_t table_storage_node_id;
  Mutex mutex;

  Array2DHeader(uint64_t value_size, uint64_t page_size,
                uint64_t table_size, bool has_default_value);
};

Array2DHeader::Array2DHeader(uint64_t value_size, uint64_t page_size,
                             uint64_t table_size, bool has_default_value)
    : value_size(value_size),
      page_size(page_size),
      table_size(table_size),
      has_default_value(has_default_value ? 1 : 0),
      table_storage_node_id(STORAGE_INVALID_NODE_ID),
      mutex(MUTEX_UNLOCKED) {}

Array2D::Array2D()
    : storage_(nullptr),
      storage_node_(),
      header_(nullptr),
      default_value_(nullptr),
      fill_page_(nullptr),
      table_(nullptr),
      table_cache_(),
      mutex_(MUTEX_UNLOCKED) {}

Array2D::~Array2D() {}

Array2D *Array2D::create(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         uint64_t table_size,
                         const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array2D> array(new (std::nothrow) Array2D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array2D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size
                  << ", table_size = " << table_size
                  << ", has_default_value = " << (default_value != nullptr);
    return nullptr;
  }
  if (!array->create_array(storage, storage_node_id,
                           value_size, page_size, table_size,
                           default_value, fill_page)) {
    return nullptr;
  }
  return array.release();
}

Array2D *Array2D::open(Storage *storage, uint32_t storage_node_id,
                       uint64_t value_size, uint64_t page_size,
                       uint64_t table_size, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array2D> array(new (std::nothrow) Array2D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array2D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size
                  << ", table_size = " << table_size;
    return nullptr;
  }
  if (!array->open_array(storage, storage_node_id,
                         value_size, page_size, table_size,
                         fill_page)) {
    return nullptr;
  }
  return array.release();
}

bool Array2D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size) {
  Array2D array;
  if (!array.open(storage, storage_node_id,
                  value_size, page_size, table_size, nullptr)) {
    return false;
  }
  return storage->unlink_node(storage_node_id);
}

bool Array2D::create_array(Storage *storage, uint32_t storage_node_id,
                           uint64_t value_size, uint64_t page_size,
                           uint64_t table_size,
                           const void *default_value, FillPage fill_page) {
  storage_ = storage;
  uint64_t storage_node_size = sizeof(Array2DHeader);
  if (default_value) {
    storage_node_size += value_size;
  }
  storage_node_ = storage->create_node(storage_node_id, storage_node_size);
  if (!storage_node_.is_valid()) {
    return false;
  }
  header_ = static_cast<Array2DHeader *>(storage_node_.body());
  *header_ = Array2DHeader(value_size, page_size, table_size, default_value);
  if (default_value) {
    default_value_ = header_ + 1;
    std::memcpy(default_value_, default_value, value_size);
    fill_page_ = fill_page;
  }
  StorageNode table_node =
      storage->create_node(storage_node_.id(), sizeof(uint32_t) * table_size);
  if (!table_node.is_valid()) {
    storage->unlink_node(storage_node_.id());
    return false;
  }
  header_->table_storage_node_id = table_node.id();
  table_ = static_cast<uint32_t *>(table_node.body());
  for (uint64_t i = 0; i < table_size; ++i) {
    table_[i] = STORAGE_INVALID_NODE_ID;
  }
  table_cache_.reset(new (std::nothrow) void *[table_size]);
  if (!table_cache_) {
    GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
    storage->unlink_node(storage_node_.id());
    return false;
  }
  for (uint64_t i = 0; i < table_size; ++i) {
    table_cache_[i] = nullptr;
  }
  return true;
}

bool Array2D::open_array(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         uint64_t table_size, FillPage fill_page) {
  storage_ = storage;
  storage_node_ = storage->open_node(storage_node_id);
  if (!storage_node_.is_valid()) {
    return false;
  }
  header_ = static_cast<Array2DHeader *>(storage_node_.body());
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "parameter conflict: value_size = " << value_size
                  << ", stored_value_size = " << header_->value_size;
    return false;
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "parameter conflict: page_size = " << page_size
                  << ", stored_page_size = " << header_->page_size;
    return false;
  }
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "parameter conflict: table_size = " << table_size
                  << ", stored_table_size = " << header_->table_size;
    return false;
  }
  default_value_ = header_ + 1;
  fill_page_ = fill_page;
  StorageNode table_node = storage->open_node(header_->table_storage_node_id);
  if (!table_node.is_valid()) {
    return false;
  }
  table_ = static_cast<uint32_t *>(table_node.body());
  table_cache_.reset(new (std::nothrow) void *[table_size]);
  if (!table_cache_) {
    GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
    return false;
  }
  for (uint64_t i = 0; i < table_size; ++i) {
    table_cache_[i] = nullptr;
  }
  return true;
}

void Array2D::initialize_page(uint64_t page_id) {
  if (!initialize_page_nothrow(page_id)) {
    GRNXX_ERROR() << "failed to initialize page: page_id = " << page_id;
    GRNXX_THROW();
  }
}

bool Array2D::initialize_page_nothrow(uint64_t page_id) {
  Lock inter_thread_lock(&mutex_);
  if (!table_cache_[page_id]) {
    StorageNode page_node;
    if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->mutex);
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        page_node =
            storage_->create_node(header_->table_storage_node_id,
                                  header_->value_size * header_->page_size);
        if (!page_node.is_valid()) {
          return false;
        }
        if (default_value_) {
          fill_page_(page_node.body(), default_value_);
        }
        table_[page_id] = page_node.id();
        table_cache_[page_id] = page_node.body();
        return true;
      }
    }
    page_node = storage_->open_node(table_[page_id]);
    if (!page_node.is_valid()) {
      return false;
    }
    table_cache_[page_id] = page_node.body();
  }
  return true;
}

}  // namespace grnxx
