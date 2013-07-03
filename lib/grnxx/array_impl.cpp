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
#include "grnxx/array_impl.hpp"

#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {

struct ArrayHeader {
  uint64_t value_size;
  uint64_t page_size;
  uint64_t table_size;
  uint64_t secondary_table_size;
  uint32_t has_default_value;
  uint32_t page_storage_node_id;
  uint32_t table_storage_node_id;
  uint32_t secondary_table_storage_node_id;
  uint32_t reserved;
  Mutex page_mutex;
  Mutex table_mutex;
  Mutex secondary_table_mutex;

  ArrayHeader();
};

ArrayHeader::ArrayHeader()
    : value_size(1),
      page_size(1),
      table_size(1),
      secondary_table_size(1),
      has_default_value(false),
      page_storage_node_id(STORAGE_INVALID_NODE_ID),
      table_storage_node_id(STORAGE_INVALID_NODE_ID),
      secondary_table_storage_node_id(STORAGE_INVALID_NODE_ID),
      reserved(0),
      page_mutex(),
      table_mutex(),
      secondary_table_mutex() {}

Array1D::Array1D()
    : storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      page_(nullptr) {}

Array1D::~Array1D() {}

void Array1D::create(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(ArrayHeader));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<ArrayHeader *>(storage_node.body());
    *header_ = ArrayHeader();
    header_->value_size = value_size;
    header_->page_size = page_size;
    StorageNode page_node =
        storage->create_node(storage_node_id_, value_size * page_size);
    header_->page_storage_node_id = page_node.id();
    page_ = page_node.body();
    if (default_value) {
      header_->has_default_value = true;
      fill_page(page_, default_value);
    }
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array1D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "invalid format: node_size = " << storage_node.size()
                  << ", header_size = " << sizeof(ArrayHeader);
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayHeader *>(storage_node.body());
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "parameter conflict: value_size = " << value_size
                  << ", stored_value_size = " << header_->value_size;
    throw LogicError();
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "parameter conflict: page_size = " << page_size
                  << ", stored_page_size = " << header_->page_size;
    throw LogicError();
  }
  StorageNode page_node = storage->open_node(header_->page_storage_node_id);
  page_ = page_node.body();
}

bool Array1D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size) {
  Array1D array;
  array.open(storage, storage_node_id, value_size, page_size);
  return storage->unlink_node(storage_node_id);
}

Array2D::Array2D()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      default_value_(nullptr),
      fill_page_(nullptr),
      table_(nullptr),
      table_cache_(),
      mutex_() {}

Array2D::~Array2D() {}

void Array2D::create(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size,
                     const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  uint64_t storage_node_size = sizeof(ArrayHeader);
  if (default_value) {
    storage_node_size += value_size;
  }
  StorageNode storage_node =
      storage->create_node(storage_node_id, storage_node_size);
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<ArrayHeader *>(storage_node.body());
    *header_ = ArrayHeader();
    header_->value_size = value_size;
    header_->page_size = page_size;
    header_->table_size = table_size;
    if (default_value) {
      header_->has_default_value = true;
      default_value_ = header_ + 1;
      std::memcpy(default_value_, default_value, value_size);
      fill_page_ = fill_page;
    }
    StorageNode table_node =
        storage->create_node(storage_node_id_, sizeof(uint32_t) * table_size);
    header_->table_storage_node_id = table_node.id();
    table_ = static_cast<uint32_t *>(table_node.body());
    for (uint64_t i = 0; i < table_size; ++i) {
      table_[i] = STORAGE_INVALID_NODE_ID;
    }
    table_cache_.reset(new (std::nothrow) void *[table_size]);
    if (!table_cache_) {
      GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
      throw MemoryError();
    }
    for (uint64_t i = 0; i < table_size; ++i) {
      table_cache_[i] = nullptr;
    }
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array2D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size,
                   uint64_t table_size, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "invalid format: node_size = " << storage_node.size()
                  << ", header_size = " << sizeof(ArrayHeader);
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayHeader *>(storage_node.body());
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "parameter conflict: value_size = " << value_size
                  << ", stored_value_size = " << header_->value_size;
    throw LogicError();
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "parameter conflict: page_size = " << page_size
                  << ", stored_page_size = " << header_->page_size;
    throw LogicError();
  }
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "parameter conflict: table_size = " << table_size
                  << ", stored_table_size = " << header_->table_size;
    throw LogicError();
  }
  default_value_ = header_ + 1;
  fill_page_ = fill_page;
  StorageNode table_node = storage->open_node(header_->table_storage_node_id);
  table_ = static_cast<uint32_t *>(table_node.body());
  table_cache_.reset(new (std::nothrow) void *[table_size]);
  if (!table_cache_) {
    GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
    throw MemoryError();
  }
  for (uint64_t i = 0; i < table_size; ++i) {
    table_cache_[i] = nullptr;
  }
}

bool Array2D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size) {
  Array2D array;
  array.open(storage, storage_node_id,
             value_size, page_size, table_size, nullptr);
  return storage->unlink_node(storage_node_id);
}

void Array2D::initialize_page(uint64_t page_id) {
  Lock inter_thread_lock(&mutex_);
  if (!table_cache_[page_id]) {
    StorageNode page_node;
    if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->table_mutex);
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        page_node =
            storage_->create_node(header_->table_storage_node_id,
                                  header_->value_size * header_->page_size);
        if (default_value_) {
          fill_page_(page_node.body(), default_value_);
        }
        table_[page_id] = page_node.id();
        table_cache_[page_id] = page_node.body();
        return;
      }
    }
    page_node = storage_->open_node(table_[page_id]);
    table_cache_[page_id] = page_node.body();
  }
}

Array3D::Array3D()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      default_value_(nullptr),
      fill_page_(nullptr),
      secondary_table_(nullptr),
      table_caches_(),
      page_mutex_(),
      table_mutex_(),
      secondary_table_mutex_() {}

Array3D::~Array3D() {}

void Array3D::create(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t secondary_table_size,
                     const void *default_value, FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  uint64_t storage_node_size = sizeof(ArrayHeader);
  if (default_value) {
    storage_node_size += value_size;
  }
  StorageNode storage_node =
      storage->create_node(storage_node_id, storage_node_size);
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<ArrayHeader *>(storage_node.body());
    *header_ = ArrayHeader();
    header_->value_size = value_size;
    header_->page_size = page_size;
    header_->table_size = table_size;
    header_->secondary_table_size = secondary_table_size;
    if (default_value) {
      header_->has_default_value = true;
      default_value_ = header_ + 1;
      std::memcpy(default_value_, default_value, value_size);
      fill_page_ = fill_page;
    }
    table_caches_.reset(
        new (std::nothrow) std::unique_ptr<void *[]>[secondary_table_size]);
    if (!table_caches_) {
      GRNXX_ERROR() << "new std::unique_ptr<void *[]>[] failed: size = "
                    << secondary_table_size;
      throw MemoryError();
    }
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array3D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size,
                   uint64_t table_size, uint64_t secondary_table_size,
                   FillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "invalid format: node_size = " << storage_node.size()
                  << ", header_size = " << sizeof(ArrayHeader);
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayHeader *>(storage_node.body());
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "parameter conflict: value_size = " << value_size
                  << ", stored_value_size = " << header_->value_size;
    throw LogicError();
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "parameter conflict: page_size = " << page_size
                  << ", stored_page_size = " << header_->page_size;
    throw LogicError();
  }
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "parameter conflict: table_size = " << table_size
                  << ", stored_table_size = " << header_->table_size;
    throw LogicError();
  }
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "parameter conflict: "
                  << "secondary_table_size = " << secondary_table_size
                  << ", stored_secondary_table_size = "
                  << header_->secondary_table_size;
    throw LogicError();
  }
  default_value_ = header_ + 1;
  fill_page_ = fill_page;
  table_caches_.reset(
      new (std::nothrow) std::unique_ptr<void *[]>[secondary_table_size]);
  if (!table_caches_) {
    GRNXX_ERROR() << "new std::unique_ptr<void *[]>[] failed: size = "
                  << secondary_table_size;
    throw MemoryError();
  }
}

bool Array3D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t secondary_table_size) {
  Array3D array;
  array.open(storage, storage_node_id, value_size, page_size,
             table_size, secondary_table_size, nullptr);
  return storage->unlink_node(storage_node_id);
}

void Array3D::initialize_page(uint64_t table_id, uint64_t page_id) {
  if (!table_caches_[table_id]) {
    initialize_table(table_id);
  }
  Lock inter_thread_lock(&page_mutex_);
  if (!table_caches_[table_id][page_id]) {
    StorageNode table_node = storage_->open_node(secondary_table_[table_id]);
    uint32_t * const table = static_cast<uint32_t *>(table_node.body());
    if (table[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->page_mutex);
      if (table[page_id] == STORAGE_INVALID_NODE_ID) {
        StorageNode page_node =
            storage_->create_node(secondary_table_[table_id],
                                  header_->value_size * header_->page_size);
        if (default_value_) {
          fill_page_(page_node.body(), default_value_);
        }
        table[page_id] = page_node.id();
        table_caches_[table_id][page_id] = page_node.body();
        return;
      }
    }
    StorageNode page_node = storage_->open_node(table[page_id]);
    table_caches_[table_id][page_id] = page_node.body();
  }
}

void Array3D::initialize_table(uint64_t table_id) {
  Lock inter_thread_lock(&table_mutex_);
  if (!table_caches_[table_id]) {
    if (!secondary_table_) {
      initialize_secondary_table();
    }
    if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->table_mutex);
      if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
        StorageNode table_node =
            storage_->create_node(header_->secondary_table_storage_node_id,
                                  sizeof(uint32_t) * header_->table_size);
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
      throw MemoryError();
    }
    for (uint64_t i = 0; i < header_->table_size; ++i) {
      table_cache[i] = nullptr;
    }
    table_caches_[table_id].reset(table_cache);
  }
}

void Array3D::initialize_secondary_table() {
  Lock inter_thread_lock(&secondary_table_mutex_);
  if (!secondary_table_) {
    if (header_->secondary_table_storage_node_id == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->secondary_table_mutex);
      if (header_->secondary_table_storage_node_id == STORAGE_INVALID_NODE_ID) {
        const uint64_t secondary_table_size =
            sizeof(uint32_t) * header_->secondary_table_size;
        StorageNode secondary_table_node =
            storage_->create_node(storage_node_id_, secondary_table_size);
        uint32_t * const secondary_table =
            static_cast<uint32_t *>(secondary_table_node.body());
        for (uint64_t i = 0; i < header_->secondary_table_size; ++i) {
          secondary_table[i] = STORAGE_INVALID_NODE_ID;
        }
        header_->secondary_table_storage_node_id = secondary_table_node.id();
        secondary_table_ = secondary_table;
        return;
      }
    }
    StorageNode secondary_table_node =
        storage_->open_node(header_->secondary_table_storage_node_id);
    secondary_table_ = static_cast<uint32_t *>(secondary_table_node.body());
  }
}

}  // namespace grnxx
