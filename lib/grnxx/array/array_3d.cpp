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
#include "grnxx/array/array_3d.hpp"

#include <cstring>
#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {

struct Array3DHeader {
  uint64_t value_size;
  uint64_t page_size;
  uint64_t table_size;
  uint64_t secondary_table_size;
  uint32_t has_default_value;
  uint32_t secondary_table_storage_node_id;
  Mutex page_mutex;
  Mutex table_mutex;
  Mutex secondary_table_mutex;

  Array3DHeader(uint64_t value_size, uint64_t page_size,
                uint64_t table_size, uint64_t secondary_table_size,
                bool has_default_value);
};

Array3DHeader::Array3DHeader(uint64_t value_size, uint64_t page_size,
                             uint64_t table_size, uint64_t secondary_table_size,
                             bool has_default_value)
    : value_size(value_size),
      page_size(page_size),
      table_size(table_size),
      secondary_table_size(secondary_table_size),
      has_default_value(has_default_value ? 1 : 0),
      secondary_table_storage_node_id(STORAGE_INVALID_NODE_ID),
      page_mutex(MUTEX_UNLOCKED),
      table_mutex(MUTEX_UNLOCKED),
      secondary_table_mutex(MUTEX_UNLOCKED) {}

Array3D::Array3D()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      default_value_(nullptr),
      fill_page_(nullptr),
      secondary_table_(nullptr),
      table_caches_(),
      page_mutex_(MUTEX_UNLOCKED),
      table_mutex_(MUTEX_UNLOCKED),
      secondary_table_mutex_(MUTEX_UNLOCKED) {}

Array3D::~Array3D() {}

Array3D *Array3D::create(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         uint64_t table_size, uint64_t secondary_table_size,
                         const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array3D> array(new (std::nothrow) Array3D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array3D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size
                  << ", table_size = " << table_size
                  << ", secondary_table_size = " << secondary_table_size
                  << ", has_default_value = " << (default_value != nullptr);
    return nullptr;
  }
  if (!array->create_array(storage, storage_node_id, value_size, page_size,
                           table_size, secondary_table_size,
                           default_value, fill_page)) {
    return nullptr;
  }
  return array.release();
}

Array3D *Array3D::open(Storage *storage, uint32_t storage_node_id,
                       uint64_t value_size, uint64_t page_size,
                       uint64_t table_size, uint64_t secondary_table_size,
                       FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    return nullptr;
  }
  std::unique_ptr<Array3D> array(new (std::nothrow) Array3D);
  if (!array) {
    GRNXX_ERROR() << "new grnxx::Array3D failed: "
                  << "storage_node_id = " << storage_node_id
                  << ", value_size = " << value_size
                  << ", page_size = " << page_size
                  << ", table_size = " << table_size
                  << ", secondary_table_size = " << secondary_table_size;
    return nullptr;
  }
  if (!array->open_array(storage, storage_node_id, value_size, page_size,
                         table_size, secondary_table_size, fill_page)) {
    return nullptr;
  }
  return array.release();
}

bool Array3D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t secondary_table_size) {
  Array3D array;
  if (!array.open(storage, storage_node_id, value_size, page_size,
                  table_size, secondary_table_size, nullptr)) {
    return false;
  }
  return storage->unlink_node(storage_node_id);
}

bool Array3D::create_array(Storage *storage, uint32_t storage_node_id,
                           uint64_t value_size, uint64_t page_size,
                           uint64_t table_size, uint64_t secondary_table_size,
                           const void *default_value, FillPage fill_page) {
  storage_ = storage;
  uint64_t storage_node_size = sizeof(Array3DHeader);
  if (default_value) {
    storage_node_size += value_size;
  }
  StorageNode storage_node =
      storage->create_node(storage_node_id, storage_node_size);
  if (!storage_node) {
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Array3DHeader *>(storage_node.body());
  *header_ = Array3DHeader(value_size, page_size, table_size,
                           secondary_table_size, default_value);
  if (default_value) {
    default_value_ = header_ + 1;
    std::memcpy(default_value_, default_value, value_size);
    fill_page_ = fill_page;
  }
  table_caches_.reset(
      new (std::nothrow) std::unique_ptr<void *[]>[secondary_table_size]);
  if (!table_caches_) {
    GRNXX_ERROR() << "new std::unique_ptr<void *[]>[] failed: size = "
                  << secondary_table_size;
    storage->unlink_node(storage_node_id_);
    return false;
  }
  return true;
}

bool Array3D::open_array(Storage *storage, uint32_t storage_node_id,
                         uint64_t value_size, uint64_t page_size,
                         uint64_t table_size, uint64_t secondary_table_size,
                         FillPage fill_page) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (!storage_node) {
    return false;
  }
  if (storage_node.size() < sizeof(Array3DHeader)) {
    GRNXX_ERROR() << "invalid format: node_size = " << storage_node.size()
                  << ", header_size = " << sizeof(Array3DHeader);
    return false;
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<Array3DHeader *>(storage_node.body());
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
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "parameter conflict: "
                  << "secondary_table_size = " << secondary_table_size
                  << ", stored_secondary_table_size = "
                  << header_->secondary_table_size;
    return false;
  }
  default_value_ = header_ + 1;
  fill_page_ = fill_page;
  table_caches_.reset(
      new (std::nothrow) std::unique_ptr<void *[]>[secondary_table_size]);
  if (!table_caches_) {
    GRNXX_ERROR() << "new std::unique_ptr<void *[]>[] failed: size = "
                  << secondary_table_size;
    return false;
  }
  return true;
}

void Array3D::initialize_page(uint64_t table_id, uint64_t page_id) {
  if (!initialize_page_nothrow(table_id, page_id)) {
    GRNXX_ERROR() << "failed to initialize page: table_id = " << table_id
                  << ", page_id = " << page_id;
    GRNXX_THROW();
  }
}

bool Array3D::initialize_page_nothrow(uint64_t table_id, uint64_t page_id) {
  if (!table_caches_[table_id]) {
    if (!initialize_table(table_id)) {
      return false;
    }
  }
  Lock inter_thread_lock(&page_mutex_);
  if (!table_caches_[table_id][page_id]) {
    StorageNode table_node = storage_->open_node(secondary_table_[table_id]);
    if (!table_node) {
      return false;
    }
    uint32_t * const table = static_cast<uint32_t *>(table_node.body());
    if (table[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->page_mutex);
      if (table[page_id] == STORAGE_INVALID_NODE_ID) {
        StorageNode page_node =
            storage_->create_node(secondary_table_[table_id],
                                  header_->value_size * header_->page_size);
        if (!page_node) {
          return false;
        }
        if (default_value_) {
          fill_page_(page_node.body(), default_value_);
        }
        table[page_id] = page_node.id();
        table_caches_[table_id][page_id] = page_node.body();
        return true;
      }
    }
    StorageNode page_node = storage_->open_node(table[page_id]);
    if (!page_node) {
      return false;
    }
    table_caches_[table_id][page_id] = page_node.body();
  }
  return true;
}

bool Array3D::initialize_table(uint64_t table_id) {
  Lock inter_thread_lock(&table_mutex_);
  if (!table_caches_[table_id]) {
    if (!secondary_table_) {
      if (!initialize_secondary_table()) {
        return false;
      }
    }
    if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->table_mutex);
      if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
        StorageNode table_node =
            storage_->create_node(header_->secondary_table_storage_node_id,
                                  sizeof(uint32_t) * header_->table_size);
        if (!table_node) {
          return false;
        }
        uint32_t * const table = static_cast<uint32_t *>(table_node.body());
        for (uint64_t i = 0; i < header_->table_size; ++i) {
          table[i] = STORAGE_INVALID_NODE_ID;
        }
        secondary_table_[table_id] = table_node.id();
      }
    }
    void ** const table_cache = new void *[header_->table_size];
    if (!table_cache) {
      GRNXX_ERROR() << "new void *[] failed: size = " << header_->table_size;
      return false;
    }
    for (uint64_t i = 0; i < header_->table_size; ++i) {
      table_cache[i] = nullptr;
    }
    table_caches_[table_id].reset(table_cache);
  }
  return true;
}

bool Array3D::initialize_secondary_table() {
  Lock inter_thread_lock(&secondary_table_mutex_);
  if (!secondary_table_) {
    if (header_->secondary_table_storage_node_id == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->secondary_table_mutex);
      if (header_->secondary_table_storage_node_id == STORAGE_INVALID_NODE_ID) {
        const uint64_t secondary_table_size =
            sizeof(uint32_t) * header_->secondary_table_size;
        StorageNode secondary_table_node =
            storage_->create_node(storage_node_id_, secondary_table_size);
        if (!secondary_table_node) {
          return false;
        }
        uint32_t * const secondary_table =
            static_cast<uint32_t *>(secondary_table_node.body());
        for (uint64_t i = 0; i < header_->secondary_table_size; ++i) {
          secondary_table[i] = STORAGE_INVALID_NODE_ID;
        }
        header_->secondary_table_storage_node_id = secondary_table_node.id();
        secondary_table_ = secondary_table;
        return true;
      }
    }
    StorageNode secondary_table_node =
        storage_->open_node(header_->secondary_table_storage_node_id);
    if (!secondary_table_node) {
      return false;
    }
    secondary_table_ = static_cast<uint32_t *>(secondary_table_node.body());
  }
  return true;
}

}  // namespace grnxx
