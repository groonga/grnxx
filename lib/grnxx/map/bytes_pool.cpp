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
#include "grnxx/map/bytes_pool.hpp"

#include <cstring>
#include <new>

#include "grnxx/exception.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/storage.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr uint64_t POOL_SIZE       = 1ULL << 48;

constexpr uint32_t MAX_PAGE_ID     = (POOL_SIZE / BytesPool::page_size()) - 1;
constexpr uint32_t INVALID_PAGE_ID = MAX_PAGE_ID + 1;

}  // namespace

struct BytesPoolHeader {
  uint64_t next_offset;
  uint32_t max_page_id;
  uint32_t latest_empty_page_id;
  uint32_t latest_idle_page_id;
  uint32_t pool_storage_node_id;
  uint32_t page_headers_storage_node_id;
  uint32_t reserved;

  BytesPoolHeader();
};

BytesPoolHeader::BytesPoolHeader()
    : next_offset(0),
      max_page_id(0),
      latest_empty_page_id(INVALID_PAGE_ID),
      latest_idle_page_id(INVALID_PAGE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID),
      page_headers_storage_node_id(STORAGE_INVALID_NODE_ID),
      reserved(0) {}

StringBuilder &operator<<(StringBuilder &builder, BytesPoolPageStatus status) {
  switch (status) {
    case BYTES_POOL_PAGE_ACTIVE: {
      return builder << "BYTES_POOL_PAGE_ACTIVE";
    }
    case BYTES_POOL_PAGE_IN_USE: {
      return builder << "BYTES_POOL_PAGE_IN_USE";
    }
    case BYTES_POOL_PAGE_EMPTY: {
      return builder << "BYTES_POOL_PAGE_EMPTY";
    }
    case BYTES_POOL_PAGE_IDLE: {
      return builder << "BYTES_POOL_PAGE_IDLE";
    }
    default: {
      return builder << "n/a";
    }
  }
}

BytesPoolPageHeader::BytesPoolPageHeader()
    : status(BYTES_POOL_PAGE_ACTIVE),
      size_in_use(0),
      modified_time(0) {}

BytesPool::BytesPool()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      pool_(),
      page_headers_() {}

BytesPool *BytesPool::create(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<BytesPool> pool(new (std::nothrow) BytesPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::BytesPool failed";
    throw MemoryError();
  }
  pool->create_pool(storage, storage_node_id);
  return pool.release();
}

BytesPool *BytesPool::open(Storage *storage, uint32_t storage_node_id) {
  if (!storage) {
    GRNXX_ERROR() << "invalid argument: storage == nullptr";
    throw LogicError();
  }
  std::unique_ptr<BytesPool> pool(new (std::nothrow) BytesPool);
  if (!pool) {
    GRNXX_ERROR() << "new grnxx::map::BytesPool failed";
    throw MemoryError();
  }
  pool->open_pool(storage, storage_node_id);
  return pool.release();
}

void BytesPool::unlink(Storage *storage, uint32_t storage_node_id) {
  std::unique_ptr<BytesPool> pool(BytesPool::open(storage, storage_node_id));
  storage->unlink_node(storage_node_id);
}

void BytesPool::unset(uint64_t value_id) {
  const uint64_t offset = get_offset(value_id);
  const uint32_t size = get_size(value_id);
  const uint32_t page_id = get_page_id(offset);
  if ((size > MAX_VALUE_SIZE) || (page_id > header_->max_page_id)) {
    GRNXX_ERROR() << "invalid argument: offset = " << offset
                  << ", size = " << size
                  << ", page_id = " << page_id
                  << ", max_size = " << MAX_VALUE_SIZE
                  << ", max_page_id = " << header_->max_page_id;
    throw LogicError();
  }
  BytesPoolPageHeader * const page_header =
      &page_headers_->get_value(page_id);
  if ((page_header->status != BYTES_POOL_PAGE_ACTIVE) &&
      (page_header->status != BYTES_POOL_PAGE_IN_USE)) {
    GRNXX_ERROR() << "wrong page: page_id = " << page_id
                  << ", status = " << page_header->status;
    throw LogicError();
  }
  if (size > page_header->size_in_use) {
    GRNXX_ERROR() << "wrong page: size = " << size
                  << ", size_in_use = " << page_header->size_in_use;
    throw LogicError();
  }
  if ((page_header->status == BYTES_POOL_PAGE_ACTIVE) ||
      (size < page_header->size_in_use)) {
    // This operation does not change the page status.
    page_header->size_in_use -= size;
  } else {
    // This operation makes the page EMPTY.
    make_page_empty(page_id, page_header);
  }
}

uint64_t BytesPool::add(ValueArg value) {
  if (value.size() > MAX_VALUE_SIZE) {
    GRNXX_ERROR() << "invalid argument: size = " << value.size()
                  << ", max_size = " << MAX_VALUE_SIZE;
    throw LogicError();
  }
  uint64_t offset = header_->next_offset;
  uint32_t size = static_cast<uint32_t>(value.size());
  uint32_t page_id = get_page_id(offset);
  BytesPoolPageHeader *page_header = &page_headers_->get_value(page_id);
  uint32_t offset_in_page = get_offset_in_page(offset);
  const uint32_t size_left = POOL_PAGE_SIZE - offset_in_page;
  if (size >= size_left) {
    uint32_t next_page_id;
    BytesPoolPageHeader *next_page_header = reserve_active_page(&next_page_id);
    if (size > size_left) {
      // Skip the remaining space of the previous ACTIVE page.
      if (page_header->size_in_use == 0) {
        // Change the page status from ACTIVE to EMPTY.
        make_page_empty(page_id, page_header);
      } else {
        // Change the page status from ACTIVE to IN_USE.
        page_header->status = BYTES_POOL_PAGE_IN_USE;
        page_header->modified_time = clock_.now();
      }
      // Use the new ACTIVE page.
      header_->next_offset = next_page_id * POOL_PAGE_SIZE;
      offset = header_->next_offset;
      page_id = next_page_id;
      page_header = next_page_header;
    } else {
      // Use the previous ACTIVE page.
      page_header->status = BYTES_POOL_PAGE_IN_USE;
      page_header->modified_time = clock_.now();
      header_->next_offset = next_page_id * POOL_PAGE_SIZE;
    }
  }
  uint8_t * const value_buf = &pool_->get_value(offset);
  std::memcpy(value_buf, value.data(), size);
  page_header->size_in_use += size;
  if (offset == header_->next_offset) {
    header_->next_offset += size;
  }
  return get_value_id(offset, size);
}

bool BytesPool::sweep(Duration lifetime) {
  if (header_->latest_empty_page_id == INVALID_PAGE_ID) {
    // Nothing to do.
    return true;
  }
  BytesPoolPageHeader * const latest_empty_page_header =
      &page_headers_->get_value(header_->latest_empty_page_id);
  const Time threshold = clock_.now() - lifetime;
  do {
    const uint32_t oldest_empty_page_id =
        latest_empty_page_header->next_page_id;
    BytesPoolPageHeader * const oldest_empty_page_header =
        &page_headers_->get_value(oldest_empty_page_id);
    if (oldest_empty_page_header->status != BYTES_POOL_PAGE_EMPTY) {
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
      header_->latest_empty_page_id = INVALID_PAGE_ID;
    }
  } while (header_->latest_empty_page_id != INVALID_PAGE_ID);
  return true;
}

BytesPool::~BytesPool() {}

void BytesPool::create_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(BytesPoolHeader));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<BytesPoolHeader *>(storage_node.body());
    *header_ = BytesPoolHeader();
    pool_.reset(Pool::create(storage, storage_node_id_, POOL_SIZE));
    page_headers_.reset(PageHeaderArray::create(storage, storage_node_id,
                                                MAX_PAGE_ID + 1));
    header_->pool_storage_node_id = pool_->storage_node_id();
    header_->page_headers_storage_node_id = page_headers_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void BytesPool::open_pool(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  StorageNode storage_node = storage->open_node(storage_node_id);
  storage_node_id_ = storage_node.id();
  header_ = static_cast<BytesPoolHeader *>(storage_node.body());
  pool_.reset(Pool::open(storage, header_->pool_storage_node_id));
  page_headers_.reset(
      PageHeaderArray::open(storage, header_->page_headers_storage_node_id));
}

BytesPoolPageHeader *BytesPool::reserve_active_page(uint32_t *page_id) {
  BytesPoolPageHeader *latest_idle_page_header = nullptr;
  uint32_t next_page_id;
  if (header_->latest_idle_page_id != INVALID_PAGE_ID) {
    // Use the oldest IDLE page.
    latest_idle_page_header =
        &page_headers_->get_value(header_->latest_idle_page_id);
    next_page_id = latest_idle_page_header->next_page_id;
  } else {
    // Create a new page.
    next_page_id = header_->max_page_id + 1;
    if (next_page_id > MAX_PAGE_ID) {
      GRNXX_ERROR() << "too many pages: next_page_id = " << next_page_id
                    << ", max_page_id = " << MAX_PAGE_ID;
      throw LogicError();
    }
  }
  BytesPoolPageHeader * const next_page_header =
      &page_headers_->get_value(next_page_id);
  if (latest_idle_page_header) {
    if (next_page_id != header_->latest_idle_page_id) {
      latest_idle_page_header->next_page_id = next_page_header->next_page_id;
    } else {
      header_->latest_idle_page_id = INVALID_PAGE_ID;
    }
  } else {
    ++header_->max_page_id;
  }
  *next_page_header = BytesPoolPageHeader();
  next_page_header->modified_time = clock_.now();
  *page_id = next_page_id;
  return next_page_header;
}

void BytesPool::make_page_empty(uint32_t page_id,
                                BytesPoolPageHeader *page_header) {
  BytesPoolPageHeader *latest_empty_page_header = nullptr;
  if (header_->latest_empty_page_id != INVALID_PAGE_ID) {
    latest_empty_page_header =
        &page_headers_->get_value(header_->latest_empty_page_id);
  }
  page_header->status = BYTES_POOL_PAGE_EMPTY;
  if (latest_empty_page_header) {
    page_header->next_page_id = latest_empty_page_header->next_page_id;
    latest_empty_page_header->next_page_id = page_id;
  } else {
    page_header->next_page_id = page_id;
  }
  page_header->modified_time = clock_.now();
  header_->latest_empty_page_id = page_id;
}

void BytesPool::make_page_idle(uint32_t page_id,
                               BytesPoolPageHeader *page_header) {
  BytesPoolPageHeader *latest_idle_page_header = nullptr;
  if (header_->latest_idle_page_id != INVALID_PAGE_ID) {
    latest_idle_page_header =
        &page_headers_->get_value(header_->latest_idle_page_id);
  }
  page_header->status = BYTES_POOL_PAGE_IDLE;
  if (latest_idle_page_header) {
    page_header->next_page_id = latest_idle_page_header->next_page_id;
    latest_idle_page_header->next_page_id = page_id;
  } else {
    page_header->next_page_id = page_id;
  }
  page_header->modified_time = clock_.now();
  header_->latest_idle_page_id = page_id;
}

}  // namespace map
}  // namespace grnxx
