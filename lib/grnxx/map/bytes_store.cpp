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
#include "grnxx/map/bytes_store.hpp"

#include <cstring>
#include <memory>
#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/periodic_clock.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/string_builder.hpp"
#include "grnxx/time.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr uint32_t BYTES_STORE_MAX_SIZE     = 4096;

constexpr uint8_t  BYTES_STORE_OFFSET_SHIFT = 13;
constexpr uint64_t BYTES_STORE_SIZE_MASK    =
    (1ULL << BYTES_STORE_OFFSET_SHIFT) - 1;

static_assert(BYTES_STORE_MAX_SIZE <= BYTES_STORE_SIZE_MASK,
              "BYTES_STORE_MAX_SIZE > BYTES_STORE_SIZE_MASK");

constexpr uint64_t BYTES_STORE_SIZE                 = 1ULL << 48;
constexpr uint32_t BYTES_STORE_PAGE_SIZE            = 1U << 20;
constexpr uint32_t BYTES_STORE_TABLE_SIZE           = 1U << 14;

constexpr uint32_t BYTES_STORE_MAX_PAGE_ID          =
    (BYTES_STORE_SIZE / BYTES_STORE_PAGE_SIZE) - 1;
constexpr uint32_t BYTES_STORE_INVALID_PAGE_ID      =
    BYTES_STORE_MAX_PAGE_ID + 1;

enum BytesStorePageStatus : uint32_t {
  // The next byte sequence will be added to the page.
  BYTES_STORE_PAGE_ACTIVE = 0,
  // The page is in use.
  BYTES_STORE_PAGE_IN_USE = 1,
  // The page is empty but not ready-to-use.
  BYTES_STORE_PAGE_EMPTY  = 2,
  // The page is empty and ready-to-use.
  BYTES_STORE_PAGE_IDLE   = 3
};

StringBuilder &operator<<(StringBuilder &builder,
                          BytesStorePageStatus status) {
  switch (status) {
    case BYTES_STORE_PAGE_ACTIVE: {
      return builder << "BYTES_STORE_PAGE_ACTIVE";
    }
    case BYTES_STORE_PAGE_IN_USE: {
      return builder << "BYTES_STORE_PAGE_IN_USE";
    }
    case BYTES_STORE_PAGE_EMPTY: {
      return builder << "BYTES_STORE_PAGE_EMPTY";
    }
    case BYTES_STORE_PAGE_IDLE: {
      return builder << "BYTES_STORE_PAGE_IDLE";
    }
    default: {
      return builder << "n/a";
    }
  }
}

struct BytesStoreHeader {
  uint64_t next_offset;
  uint32_t max_page_id;
  uint32_t latest_empty_page_id;
  uint32_t latest_idle_page_id;
  uint32_t pages_storage_node_id;
  uint32_t page_headers_storage_node_id;
  uint32_t reserved;

  BytesStoreHeader();
};

BytesStoreHeader::BytesStoreHeader()
    : next_offset(0),
      max_page_id(0),
      latest_empty_page_id(BYTES_STORE_INVALID_PAGE_ID),
      latest_idle_page_id(BYTES_STORE_INVALID_PAGE_ID),
      pages_storage_node_id(STORAGE_INVALID_NODE_ID),
      page_headers_storage_node_id(STORAGE_INVALID_NODE_ID),
      reserved(0) {}

struct BytesStorePageHeader {
  // ACTIVE, IN_USE, EMPTY, and IDLE.
  BytesStorePageStatus status;
  union {
    // ACTIVE and IN_USE.
    uint32_t size_in_use;
    // EMPTY and IDLE.
    uint32_t next_page_id;
  };
  // ACTIVE, IN_USE, EMPTY, and IDLE.
  Time modified_time;

  BytesStorePageHeader();
};

BytesStorePageHeader::BytesStorePageHeader()
    : status(BYTES_STORE_PAGE_ACTIVE),
      size_in_use(0),
      modified_time(0) {}

class BytesStoreImpl : public BytesStore {
  using BytesArray = Array<uint8_t, BYTES_STORE_PAGE_SIZE,
                                    BYTES_STORE_TABLE_SIZE>;
  using PageHeaderArray = Array<BytesStorePageHeader, BYTES_STORE_TABLE_SIZE>;

 public:
  using Value = Bytes;
  using ValueArg = typename Traits<Bytes>::ArgumentType;

  BytesStoreImpl();
  virtual ~BytesStoreImpl();

  static BytesStoreImpl *create(Storage *storage, uint32_t storage_node_id);
  static BytesStoreImpl *open(Storage *storage, uint32_t storage_node_id);

  uint32_t storage_node_id() const;

  Value get(uint64_t bytes_id);
  void unset(uint64_t bytes_id);
  uint64_t add(ValueArg bytes);

  bool sweep(Duration lifetime);

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  BytesStoreHeader *header_;
  std::unique_ptr<BytesArray> pages_;
  std::unique_ptr<PageHeaderArray> page_headers_;
  PeriodicClock clock_;

  void create_store(Storage *storage, uint32_t storage_node_id);
  void open_store(Storage *storage, uint32_t storage_node_id);

  void reserve_active_page(uint32_t *page_id,
                           BytesStorePageHeader **page_header);
  void make_page_empty(uint32_t page_id, BytesStorePageHeader *page_header);
  void make_page_idle(uint32_t page_id, BytesStorePageHeader *page_header);

  static uint64_t get_bytes_id(uint64_t offset, uint32_t size) {
    return (offset << BYTES_STORE_OFFSET_SHIFT) | size;
  }
  static uint64_t get_offset(uint64_t bytes_id) {
    return bytes_id >> BYTES_STORE_OFFSET_SHIFT;
  }
  static uint32_t get_size(uint64_t bytes_id) {
    return static_cast<uint32_t>(bytes_id & BYTES_STORE_SIZE_MASK);
  }
  static uint32_t get_page_id(uint64_t offset) {
    return static_cast<uint32_t>(offset / BYTES_STORE_PAGE_SIZE);
  }
  static uint32_t get_offset_in_page(uint64_t offset) {
    return static_cast<uint32_t>(offset % BYTES_STORE_PAGE_SIZE);
  }
};

BytesStoreImpl::BytesStoreImpl()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      pages_(),
      page_headers_() {}

BytesStoreImpl::~BytesStoreImpl() {}

BytesStoreImpl *BytesStoreImpl::create(Storage *storage,
                                       uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<BytesStoreImpl> store(new (std::nothrow) BytesStoreImpl);
  if (!store) {
    GRNXX_ERROR() << "new grnxx::map::BytesStoreImpl failed";
    throw MemoryError();
  }
  store->create_store(storage, storage_node_id);
  return store.release();
}

BytesStoreImpl *BytesStoreImpl::open(Storage *storage,
                                     uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<BytesStoreImpl> store(new (std::nothrow) BytesStoreImpl);
  if (!store) {
    GRNXX_ERROR() << "new grnxx::map::BytesStoreImpl failed";
    throw MemoryError();
  }
  store->open_store(storage, storage_node_id);
  return store.release();
}

uint32_t BytesStoreImpl::storage_node_id() const {
  return storage_node_id_;
}

auto BytesStoreImpl::get(uint64_t bytes_id) -> Value {
  const uint64_t offset = get_offset(bytes_id);
  const uint32_t size = get_size(bytes_id);
  return Value(&pages_->get_value(offset), size);
}

void BytesStoreImpl::unset(uint64_t bytes_id) {
  const uint64_t offset = get_offset(bytes_id);
  const uint32_t size = get_size(bytes_id);
  const uint32_t page_id = get_page_id(offset);
  if ((size > BYTES_STORE_MAX_SIZE) || (page_id > header_->max_page_id)) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size
                  << ", page_id = " << page_id
                  << ", max_size = " << BYTES_STORE_MAX_SIZE
                  << ", max_page_id = " << header_->max_page_id;
    throw LogicError();
  }
  BytesStorePageHeader * const page_header =
      &page_headers_->get_value(page_id);
  if ((page_header->status != BYTES_STORE_PAGE_ACTIVE) &&
      (page_header->status != BYTES_STORE_PAGE_IN_USE)) {
    GRNXX_ERROR() << "invalid argument: page_id = " << page_id
                  << ", status = " << page_header->status;
    throw LogicError();
  }
  if (size > page_header->size_in_use) {
    GRNXX_ERROR() << "invalid argument: size = " << size
                  << ", size_in_use = " << page_header->size_in_use;
    throw LogicError();
  }
  if ((page_header->status == BYTES_STORE_PAGE_ACTIVE) ||
      (size < page_header->size_in_use)) {
    // This operation does not change the page status.
    page_header->size_in_use -= size;
  } else {
    // This operation makes the page EMPTY.
    make_page_empty(page_id, page_header);
  }
}

uint64_t BytesStoreImpl::add(ValueArg bytes) {
  if (bytes.size() > BYTES_STORE_MAX_SIZE) {
    GRNXX_ERROR() << "invalid argument: size = " << bytes.size();
    throw LogicError();
  }
  uint64_t offset = header_->next_offset;
  uint32_t size = static_cast<uint32_t>(bytes.size());
  uint32_t page_id = get_page_id(offset);
  BytesStorePageHeader *page_header = &page_headers_->get_value(page_id);
  uint32_t offset_in_page = get_offset_in_page(offset);
  const uint32_t size_left = BYTES_STORE_PAGE_SIZE - offset_in_page;
  if (size >= size_left) {
    uint32_t next_page_id;
    BytesStorePageHeader *next_page_header;
    reserve_active_page(&next_page_id, &next_page_header);
    if (size > size_left) {
      // Skip the remaining space of the previous ACTIVE page.
      if (page_header->size_in_use == 0) {
        // Change the page status from ACTIVE to EMPTY.
        make_page_empty(page_id, page_header);
      } else {
        // Change the page status from ACTIVE to IN_USE.
        page_header->status = BYTES_STORE_PAGE_IN_USE;
        page_header->modified_time = clock_.now();
      }
      // Use the new ACTIVE page.
      header_->next_offset = next_page_id * BYTES_STORE_PAGE_SIZE;
      offset = header_->next_offset;
      page_id = next_page_id;
      page_header = next_page_header;
    } else {
      // Use the previous ACTIVE page.
      page_header->status = BYTES_STORE_PAGE_IN_USE;
      page_header->modified_time = clock_.now();
      header_->next_offset = next_page_id * BYTES_STORE_PAGE_SIZE;
    }
  }
  uint8_t * const value = &pages_->get_value(offset);
  std::memcpy(value, bytes.data(), size);
  page_header->size_in_use += size;
  if (offset == header_->next_offset) {
    header_->next_offset += size;
  }
  return get_bytes_id(offset, size);
}

bool BytesStoreImpl::sweep(Duration lifetime) {
  if (header_->latest_empty_page_id == BYTES_STORE_INVALID_PAGE_ID) {
    // Nothing to do.
    return true;
  }
  BytesStorePageHeader * const latest_empty_page_header =
      &page_headers_->get_value(header_->latest_empty_page_id);
  const Time threshold = clock_.now() - lifetime;
  do {
    const uint32_t oldest_empty_page_id =
        latest_empty_page_header->next_page_id;
    BytesStorePageHeader * const oldest_empty_page_header =
        &page_headers_->get_value(oldest_empty_page_id);
    if (oldest_empty_page_header->status != BYTES_STORE_PAGE_EMPTY) {
      GRNXX_ERROR() << "status conflict: status = "
                    << oldest_empty_page_header->status;
      throw LogicError();
    }
    if (oldest_empty_page_header->modified_time > threshold) {
      // The remaining empty pages are not ready.
      return true;
    }
    const uint32_t next_oldest_empty_page_id =
        oldest_empty_page_header->next_page_id;
    make_page_idle(oldest_empty_page_id, oldest_empty_page_header);
    if (oldest_empty_page_header != latest_empty_page_header) {
      latest_empty_page_header->next_page_id = next_oldest_empty_page_id;
    } else {
      header_->latest_empty_page_id = BYTES_STORE_INVALID_PAGE_ID;
    }
  } while (header_->latest_empty_page_id != BYTES_STORE_INVALID_PAGE_ID);
  return true;
}

void BytesStoreImpl::create_store(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(BytesStoreHeader));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<BytesStoreHeader *>(storage_node.body());
    *header_ = BytesStoreHeader();
    pages_.reset(BytesArray::create(storage, storage_node_id_,
                                    BYTES_STORE_SIZE));
    page_headers_.reset(PageHeaderArray::create(storage, storage_node_id,
                                                BYTES_STORE_MAX_PAGE_ID + 1));
    header_->pages_storage_node_id = pages_->storage_node_id();
    header_->page_headers_storage_node_id = page_headers_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void BytesStoreImpl::open_store(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  storage_node_id_ = storage_node.id();
  header_ = static_cast<BytesStoreHeader *>(storage_node.body());
  pages_.reset(BytesArray::open(storage, header_->pages_storage_node_id));
  page_headers_.reset(
      PageHeaderArray::open(storage, header_->page_headers_storage_node_id));
}

void BytesStoreImpl::reserve_active_page(uint32_t *page_id,
                                         BytesStorePageHeader **page_header) {
  BytesStorePageHeader *latest_idle_page_header = nullptr;
  uint32_t next_page_id;
  if (header_->latest_idle_page_id != BYTES_STORE_INVALID_PAGE_ID) {
    // Use the oldest IDLE page.
    latest_idle_page_header =
        &page_headers_->get_value(header_->latest_idle_page_id);
    next_page_id = latest_idle_page_header->next_page_id;
  } else {
    // Create a new page.
    next_page_id = header_->max_page_id + 1;
    if (next_page_id > BYTES_STORE_MAX_PAGE_ID) {
      GRNXX_ERROR() << "too many pages: next_page_id = " << next_page_id
                    << ", max_page_id = " << BYTES_STORE_MAX_PAGE_ID;
      throw LogicError();
    }
  }
  BytesStorePageHeader * const next_page_header =
      &page_headers_->get_value(next_page_id);
  if (latest_idle_page_header) {
    if (next_page_id != header_->latest_idle_page_id) {
      latest_idle_page_header->next_page_id = next_page_header->next_page_id;
    } else {
      header_->latest_idle_page_id = BYTES_STORE_INVALID_PAGE_ID;
    }
  } else {
    ++header_->max_page_id;
  }
  *next_page_header = BytesStorePageHeader();
  next_page_header->modified_time = clock_.now();
  *page_id = next_page_id;
  *page_header = next_page_header;
}

void BytesStoreImpl::make_page_empty(uint32_t page_id,
                                     BytesStorePageHeader *page_header) {
  BytesStorePageHeader *latest_empty_page_header = nullptr;
  if (header_->latest_empty_page_id != BYTES_STORE_INVALID_PAGE_ID) {
    latest_empty_page_header =
        &page_headers_->get_value(header_->latest_empty_page_id);
  }
  page_header->status = BYTES_STORE_PAGE_EMPTY;
  if (latest_empty_page_header) {
    page_header->next_page_id = latest_empty_page_header->next_page_id;
    latest_empty_page_header->next_page_id = page_id;
  } else {
    page_header->next_page_id = page_id;
  }
  page_header->modified_time = clock_.now();
  header_->latest_empty_page_id = page_id;
}

void BytesStoreImpl::make_page_idle(uint32_t page_id,
                                    BytesStorePageHeader *page_header) {
  BytesStorePageHeader *latest_idle_page_header = nullptr;
  if (header_->latest_idle_page_id != BYTES_STORE_INVALID_PAGE_ID) {
    latest_idle_page_header =
        &page_headers_->get_value(header_->latest_idle_page_id);
  }
  page_header->status = BYTES_STORE_PAGE_IDLE;
  if (latest_idle_page_header) {
    page_header->next_page_id = latest_idle_page_header->next_page_id;
    latest_idle_page_header->next_page_id = page_id;
  } else {
    page_header->next_page_id = page_id;
  }
  page_header->modified_time = clock_.now();
  header_->latest_idle_page_id = page_id;
}

}  // namespace

BytesStore::BytesStore() {}
BytesStore::~BytesStore() {}

BytesStore *BytesStore::create(Storage *storage, uint32_t storage_node_id) {
  return BytesStoreImpl::create(storage, storage_node_id);
}

BytesStore *BytesStore::open(Storage *storage, uint32_t storage_node_id) {
  return BytesStoreImpl::open(storage, storage_node_id);
}

void BytesStore::unlink(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<BytesStore> store(
      BytesStore::open(storage, storage_node_id));
  storage->unlink_node(storage_node_id);
}

}  // namespace map
}  // namespace grnxx
