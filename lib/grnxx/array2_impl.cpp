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
#include "grnxx/array2_impl.hpp"

#include <new>

#include "grnxx/bytes.hpp"
#include "grnxx/common_header.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace alpha {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::Array";

struct DummyTable {
  void **pages;
  uint32_t reference_count;
  Mutex mutex;

  DummyTable() : pages(nullptr), reference_count(0), mutex() {}
};

class DummyTableManager {
 public:
  // Get a singleton.
  static DummyTableManager &get();

  // Get a dummy table.
  void **get_dummy_table(uint64_t table_size);
  // Free a dummy table.
  void free_dummy_table(uint64_t table_size);

 private:
  DummyTable dummy_tables_[64];

  DummyTableManager() : dummy_tables_() {}

  const DummyTableManager(const DummyTableManager &) = delete;
  DummyTableManager &operator=(const DummyTableManager &) = delete;
};

DummyTableManager &DummyTableManager::get() {
  static DummyTableManager singleton;
  return singleton;
}

void **DummyTableManager::get_dummy_table(uint64_t table_size) {
  const uint64_t table_id = bit_scan_reverse(table_size);
  DummyTable &dummy_table = dummy_tables_[table_id];
  Lock lock(&dummy_table.mutex);
  if (dummy_table.reference_count == 0) {
    if (dummy_table.pages) {
      GRNXX_ERROR() << "already exists: table_size = " << table_size;
      throw LogicError();
    }
    // Create a dummy table.
    dummy_table.pages = new (std::nothrow) void *[table_size];
    if (!dummy_table.pages) {
      GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
      throw MemoryError();
    }
    for (uint64_t i = 0; i < table_size; ++i) {
      dummy_table.pages[i] = Array3D::invalid_page();
    }
  } else if (!dummy_table.pages) {
    GRNXX_ERROR() << "invalid pages: table_size = " << table_size;
    throw LogicError();
  }
  ++dummy_table.reference_count;
  return dummy_table.pages;
}

void DummyTableManager::free_dummy_table(uint64_t table_size) {
  const uint64_t table_id = bit_scan_reverse(table_size);
  DummyTable &dummy_table = dummy_tables_[table_id];
  Lock lock(&dummy_table.mutex);
  if (!dummy_table.pages || (dummy_table.reference_count == 0)) {
    GRNXX_ERROR() << "already freed: table_size = " << table_size;
    throw LogicError();
  }
  if (dummy_table.reference_count == 1) {
    // Free a dummy table.
    delete [] dummy_table.pages;
    dummy_table.pages = nullptr;
  }
  --dummy_table.reference_count;
}

}  // namespace

struct ArrayHeader {
  CommonHeader common_header;
  uint64_t value_size;
  uint64_t page_size;
  uint64_t table_size;
  uint64_t secondary_table_size;
  uint64_t size;
  uint32_t has_default_value;
  union {
    uint32_t page_storage_node_id;
    uint32_t table_storage_node_id;
    uint32_t secondary_table_storage_node_id;
  };
  Mutex page_mutex;
  Mutex table_mutex;

  // Initialize the members except "common_header".
  ArrayHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

ArrayHeader::ArrayHeader()
    : common_header(FORMAT_STRING),
      value_size(0),
      page_size(0),
      table_size(0),
      secondary_table_size(0),
      size(0),
      has_default_value(0),
      page_storage_node_id(STORAGE_INVALID_NODE_ID),
      page_mutex(),
      table_mutex() {}

ArrayHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

Array1D::Array1D()
    : page_(nullptr),
      size_(0),
      storage_node_id_(STORAGE_INVALID_NODE_ID) {}

Array1D::~Array1D() {}

void Array1D::create(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t,
                   uint64_t, uint64_t size,
                   const void *default_value, ArrayFillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(ArrayHeader));
  storage_node_id_ = storage_node.id();
  try {
    ArrayHeader * const header =
        static_cast<ArrayHeader *>(storage_node.body());
    *header = ArrayHeader();
    header->value_size = value_size;
    header->page_size = size;
    header->size = size;
    // Create a page.
    StorageNode page_node =
        storage->create_node(storage_node_id_, value_size * size);
    header->page_storage_node_id = page_node.id();
    page_ = page_node.body();
    if (default_value) {
      header->has_default_value = 1;
      fill_page(page_, size, default_value);
    }
    size_ = size;
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array1D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t,
                   uint64_t, ArrayFillPage) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "too small header: size = " << storage_node.size();
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  const ArrayHeader * const header =
      static_cast<ArrayHeader *>(storage_node.body());
  if (!*header) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header->common_header.format();
    throw LogicError();
  }
  if (header->value_size != value_size) {
    GRNXX_ERROR() << "wrong value_size: expected = " << value_size
                  << ", actual = " << header->value_size;
    throw LogicError();
  }
  StorageNode page_node = storage->open_node(header->page_storage_node_id);
  page_ = page_node.body();
  size_ = header->size;
}

bool Array1D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size) {
  Array1D array;
  array.open(storage, storage_node_id,
             value_size, page_size, table_size, nullptr);
  return storage->unlink_node(storage_node_id);
}

Array2D::Array2D()
    : pages_(),
      size_(0),
      storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      fill_page_(nullptr),
      table_(nullptr),
      mutex_() {}

Array2D::~Array2D() {}

void Array2D::create(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size,
                   uint64_t, uint64_t size,
                   const void *default_value, ArrayFillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  if ((size % page_size) != 0) {
    const uint64_t adjusted_size = size + page_size - (size % page_size);
    GRNXX_WARNING() << "size adjustment: before = " << size
                    << ", after = " << adjusted_size
                    << ", page_size = " << page_size;
    size = adjusted_size;
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
    header_->table_size = size / page_size;
    header_->size = size;
    if (default_value) {
      header_->has_default_value = 1;
      std::memcpy(header_ + 1, default_value, value_size);
      fill_page_ = fill_page;
    }
    // Create a table.
    StorageNode table_node = storage->create_node(
        storage_node_id_, sizeof(uint32_t) * header_->table_size);
    header_->table_storage_node_id = table_node.id();
    table_ = static_cast<uint32_t *>(table_node.body());
    for (uint64_t i = 0; i < header_->table_size; ++i) {
      table_[i] = STORAGE_INVALID_NODE_ID;
    }
    reserve_pages();
    size_ = size;
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array2D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size,
                   uint64_t , ArrayFillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "too small header: size = " << storage_node.size();
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayHeader *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "wrong value_size: expected = " << value_size
                  << ", actual = " << header_->value_size;
    throw LogicError();
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "wrong page_size: expected = " << page_size
                  << ", actual = " << header_->page_size;
    throw LogicError();
  }
  if (header_->has_default_value) {
    fill_page_ = fill_page;
  }
  StorageNode table_node = storage->open_node(header_->table_storage_node_id);
  table_ = static_cast<uint32_t *>(table_node.body());
  reserve_pages();
  size_ = header_->size;
}

bool Array2D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size) {
  Array2D array;
  array.open(storage, storage_node_id,
             value_size, page_size, table_size, nullptr);
  return storage->unlink_node(storage_node_id);
}

void Array2D::reserve_pages() {
  // Create a table cache.
  pages_.reset(new (std::nothrow) void *[header_->table_size]);
  if (!pages_) {
    GRNXX_ERROR() << "new void *[] failed: size = " << header_->table_size;
    throw MemoryError();
  }
  for (uint64_t i = 0; i < header_->table_size; ++i) {
    pages_[i] = invalid_page();
  }
}

void Array2D::reserve_page(uint64_t page_id) {
  Lock inter_thread_lock(&mutex_);
  if (pages_[page_id] == invalid_page()) {
    StorageNode page_node;
    if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->table_mutex);
      if (table_[page_id] == STORAGE_INVALID_NODE_ID) {
        // Create a page.
        page_node =
            storage_->create_node(header_->table_storage_node_id,
                                  header_->value_size * header_->page_size);
        if (header_->has_default_value) {
          fill_page_(page_node.body(), header_->page_size, header_ + 1);
        }
        table_[page_id] = page_node.id();
      }
    }
    if (!page_node) {
      page_node = storage_->open_node(table_[page_id]);
    }
    pages_[page_id] = static_cast<char *>(page_node.body())
        - (header_->value_size * header_->page_size * page_id);
  }
}

Array3D::Array3D()
    : tables_(),
      size_(0),
      storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      fill_page_(nullptr),
      secondary_table_(nullptr),
      dummy_table_(nullptr),
      page_mutex_(),
      table_mutex_() {}

Array3D::~Array3D() {
  if (tables_) {
    uint64_t offset = 0;
    for (uint64_t i = 0; i < header_->secondary_table_size; ++i) {
      if (tables_[i] != (dummy_table_ - offset)) {
        delete [] (tables_[i] + offset);
      }
      offset += header_->table_size;
    }
  }
  if (dummy_table_) {
    DummyTableManager::get().free_dummy_table(header_->table_size);
  }
}

void Array3D::create(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size, uint64_t size,
                     const void *default_value, ArrayFillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  if ((size % (page_size * table_size)) != 0) {
    const uint64_t adjusted_size =
        size + (page_size * table_size) - (size % (page_size * table_size));
    GRNXX_WARNING() << "size adjustment: before = " << size
                    << ", after = " << adjusted_size
                    << ", page_size = " << page_size
                    << ", table_size = " << table_size;
    size = adjusted_size;
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
    header_->secondary_table_size = size / (page_size * table_size);
    header_->size = size;
    if (default_value) {
      header_->has_default_value = 1;
      std::memcpy(header_ + 1, default_value, value_size);
      fill_page_ = fill_page;
    }
    // Create a secondary table.
    StorageNode secondary_table_node = storage->create_node(
        storage_node_id_, sizeof(uint32_t) * header_->secondary_table_size);
    header_->secondary_table_storage_node_id = secondary_table_node.id();
    secondary_table_ = static_cast<uint32_t *>(secondary_table_node.body());
    for (uint64_t i = 0; i < header_->secondary_table_size; ++i) {
      secondary_table_[i] = STORAGE_INVALID_NODE_ID;
    }
    reserve_tables();
    size_ = size;
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void Array3D::open(Storage *storage, uint32_t storage_node_id,
                   uint64_t value_size, uint64_t page_size,
                   uint64_t table_size, ArrayFillPage fill_page) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage = nullptr";
    throw LogicError();
  }
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  if (storage_node.size() < sizeof(ArrayHeader)) {
    GRNXX_ERROR() << "too small header: size = " << storage_node.size();
    throw LogicError();
  }
  storage_node_id_ = storage_node.id();
  header_ = static_cast<ArrayHeader *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "wrong value_size: expected = " << value_size
                  << ", actual = " << header_->value_size;
    throw LogicError();
  }
  if (header_->page_size != page_size) {
    GRNXX_ERROR() << "wrong page_size: expected = " << page_size
                  << ", actual = " << header_->page_size;
    throw LogicError();
  }
  if (header_->table_size != table_size) {
    GRNXX_ERROR() << "wrong table_size: expected = " << table_size
                  << ", actual = " << header_->table_size;
    throw LogicError();
  }
  if (header_->has_default_value) {
    fill_page_ = fill_page;
  }
  StorageNode secondary_table_node =
      storage->open_node(header_->secondary_table_storage_node_id);
  secondary_table_ = static_cast<uint32_t *>(secondary_table_node.body());
  reserve_tables();
  size_ = header_->size;
}

bool Array3D::unlink(Storage *storage, uint32_t storage_node_id,
                     uint64_t value_size, uint64_t page_size,
                     uint64_t table_size) {
  Array3D array;
  array.open(storage, storage_node_id,
             value_size, page_size, table_size, nullptr);
  return storage->unlink_node(storage_node_id);
}

void Array3D::reserve_tables() {
  dummy_table_ = DummyTableManager::get().get_dummy_table(header_->table_size);
  // Create a secondary table cache.
  tables_.reset(new (std::nothrow) void **[header_->secondary_table_size]);
  if (!tables_) {
    GRNXX_ERROR() << "new void **[] failed: size = "
                  << header_->secondary_table_size;
    throw MemoryError();
  }
  // Fill the secondary table cache with the dummy table cache.
  uint64_t offset = 0;
  for (uint64_t i = 0; i < header_->secondary_table_size; ++i) {
    tables_[i] = dummy_table_ - offset;
    offset += header_->table_size;
  }
}

void Array3D::reserve_page(uint64_t page_id) {
  const uint64_t table_id = page_id / header_->table_size;
  if (tables_[table_id] == (dummy_table_ - (header_->table_size * table_id))) {
    reserve_table(table_id);
  }
  Lock inter_thread_lock(&page_mutex_);
  if (tables_[table_id][page_id] == invalid_page()) {
    StorageNode page_node;
    StorageNode table_node = storage_->open_node(secondary_table_[table_id]);
    uint32_t * const table = static_cast<uint32_t *>(table_node.body())
        - (header_->table_size * table_id);
    if (table[page_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->page_mutex);
      if (table[page_id] == STORAGE_INVALID_NODE_ID) {
        // Create a page.
        page_node = storage_->create_node(
            secondary_table_[table_id],
            header_->value_size * header_->page_size);
        if (header_->has_default_value) {
          fill_page_(page_node.body(), header_->page_size, header_ + 1);
        }
        table[page_id] = page_node.id();
      }
    }
    if (!page_node) {
      page_node = storage_->open_node(table[page_id]);
    }
    tables_[table_id][page_id] = static_cast<char *>(page_node.body())
        - (header_->value_size * header_->page_size * page_id);
  }
}

void Array3D::reserve_table(uint64_t table_id) {
  Lock inter_thread_lock(&table_mutex_);
  if (tables_[table_id] == (dummy_table_ - (header_->table_size * table_id))) {
    if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
      Lock inter_process_lock(&header_->table_mutex);
      if (secondary_table_[table_id] == STORAGE_INVALID_NODE_ID) {
        // Create a table.
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
    // Create a table cache.
    void ** const pages = new (std::nothrow) void *[header_->table_size];
    if (!pages) {
      GRNXX_ERROR() << "new void *[] failed: size = " << header_->table_size;
      throw MemoryError();
    }
    for (uint64_t i = 0; i < header_->table_size; ++i) {
      pages[i] = invalid_page();
    }
    tables_[table_id] = pages - (header_->table_size * table_id);
  }
}

}  // namespace alpha
}  // namespace grnxx
