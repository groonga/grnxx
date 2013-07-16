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
#include "grnxx/alpha/paged_array.hpp"

#include <new>

#include "grnxx/alpha/common_header.hpp"
#include "grnxx/bytes.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/mutex.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace alpha {

constexpr char PAGED_ARRAY_FORMAT[CommonHeader::FORMAT_SIZE] =
    "grnxx::alpha::PagedArray";

constexpr uint64_t PAGED_ARRAY_MIN_TABLE_SIZE = 16;

struct PagedArrayHeader {
  CommonHeader common_header;
  uint64_t value_size;
  uint64_t size;
  uint64_t page_size;
  uint64_t has_default_value;
  uint64_t table_size;
  uint32_t table_storage_node_id;
  Mutex mutex;

  PagedArrayHeader();
};

PagedArrayHeader::PagedArrayHeader()
    : common_header(PAGED_ARRAY_FORMAT),
      value_size(0),
      size(0),
      page_size(0),
      has_default_value(0),
      table_size(0),
      table_storage_node_id(STORAGE_INVALID_NODE_ID),
      mutex() {}

PagedArrayImpl::PagedArrayImpl()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      size_(0),
      page_size_(0),
      page_shift_(0),
      page_mask_(0),
      table_size_(0),
      pages_(),
      table_(nullptr),
      header_(nullptr),
      default_value_(nullptr),
      fill_page_(nullptr) {}

PagedArrayImpl::~PagedArrayImpl() {}

void PagedArrayImpl::create(Storage *storage, uint32_t storage_node_id,
                            uint64_t value_size, uint64_t size,
                            uint64_t page_size,
                            const void *default_value, FillPage fill_page) {
  if (page_size == 0) {
    GRNXX_ERROR() << "invalid argument: page_size = " << page_size;
    throw LogicError();
  }
  if ((page_size & (page_size - 1)) != 0) {
    const uint64_t revised_page_size = 2ULL << bit_scan_reverse(page_size);
    GRNXX_WARNING() << "page_size must be a power of two: "
                    << "page_size = " << page_size
                    << ", revised_page_size = " << revised_page_size;
    page_size = revised_page_size;
  }
  if ((size % page_size) != 0) {
    const uint64_t revised_size = size + page_size - (size % page_size);
    GRNXX_WARNING() << "size must be a multiple of page_size: size = " << size
                    << ", revised_size = " << revised_size
                    << ", page_size = " << page_size;
    size = revised_size;
  }
  PagedArrayImpl new_impl;
  new_impl.create_array(storage, storage_node_id, value_size, size, page_size,
                        default_value, fill_page);
  swap(new_impl);
}

void PagedArrayImpl::open(Storage *storage, uint32_t storage_node_id,
                          uint64_t value_size, FillPage fill_page) {
  PagedArrayImpl new_impl;
  new_impl.open_array(storage, storage_node_id, value_size, fill_page);
  swap(new_impl);
}

bool PagedArrayImpl::unlink(Storage *storage, uint32_t storage_node_id,
                            uint64_t value_size) {
  PagedArrayImpl impl;
  impl.open(storage, storage_node_id, value_size);
  return storage->unlink_node(storage_node_id);
}

void PagedArrayImpl::create_array(Storage *storage, uint32_t storage_node_id,
                                  uint64_t value_size, uint64_t size,
                                  uint64_t page_size,
                                  const void *default_value,
                                  FillPage fill_page) {
  storage_ = storage;
  uint64_t header_node_size = sizeof(PagedArrayHeader);
  if (default_value) {
    header_node_size += value_size;
  }
  StorageNode header_node =
      storage->create_node(storage_node_id, header_node_size);
  storage_node_id_ = header_node.id();
  try {
    header_ = static_cast<PagedArrayHeader *>(header_node.body());
    *header_ = PagedArrayHeader();
    header_->value_size = value_size;
    header_->size = size;
    header_->page_size = page_size;
    header_->has_default_value = default_value != nullptr;
    size_ = size;
    page_size_ = page_size;
    page_shift_ = bit_scan_reverse(page_size_);
    page_mask_ = page_size_ - 1;
    if (header_->has_default_value) {
      default_value_ = header_ + 1;
      fill_page_ = fill_page;
    }
  } catch (...) {
    storage->unlink_node(header_node.id());
    throw;
  }
}

void PagedArrayImpl::open_array(Storage *storage, uint32_t storage_node_id,
                                uint64_t value_size, FillPage fill_page) {
  storage_ = storage;
  storage_node_id_ = storage_node_id;
  StorageNode header_node = storage->open_node(storage_node_id);
  if (header_node.size() < sizeof(CommonHeader)) {
    GRNXX_ERROR() << "too small header: size = " << header_node.size();
    throw LogicError();
  }
  header_ = static_cast<PagedArrayHeader *>(header_node.body());
  if (header_->common_header.format() !=
      Bytes(PAGED_ARRAY_FORMAT, CommonHeader::FORMAT_SIZE)) {
    GRNXX_ERROR() << "invalid format: expected = " << PAGED_ARRAY_FORMAT
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  if (header_->value_size != value_size) {
    GRNXX_ERROR() << "invalid value size: expected = " << value_size
                  << ", actual = " << header_->value_size;
    throw LogicError();
  }
  size_ = header_->size;
  page_size_ = header_->page_size;
  page_shift_ = bit_scan_reverse(page_size_);
  page_mask_ = page_size_ - 1;
  if (header_->has_default_value) {
    default_value_ = header_ + 1;
    fill_page_ = fill_page;
  }
}

void PagedArrayImpl::resize_table(uint64_t table_size) {
  Lock lock(&header_->mutex);
  update_table();
  if (table_size <= table_size_) {
    // Nothing to do.
    return;
  }
  const uint64_t max_table_size = size_ / page_size_;
  if (table_size > max_table_size) {
    GRNXX_ERROR() << "too large size: table_size = " << table_size
                  << ", size = " << size_ << ", page_size = " << page_size_;
    throw LogicError();
  }
  if (table_size < PAGED_ARRAY_MIN_TABLE_SIZE) {
    table_size = PAGED_ARRAY_MIN_TABLE_SIZE;
  }
  if ((table_size & (table_size - 1)) != 0) {
    table_size = 2ULL << bit_scan_reverse(table_size);
  }
  if (table_size > max_table_size) {
    table_size = max_table_size;
  }
  // Create a new table cache.
  std::unique_ptr<void *[]> new_pages(new (std::nothrow) void *[table_size]);
  if (!new_pages) {
    GRNXX_ERROR() << "new void *[] failed: size = " << table_size;
    throw MemoryError();
  }
  for (uint64_t i = 0; i < table_size_; ++i) {
    new_pages[i] = pages_[i];
  }
  for (uint64_t i = table_size_; i < table_size; ++i) {
    new_pages[i] = invalid_page_address();
  }
  // Create a new table.
  StorageNode table_node =
      storage_->create_node(storage_node_id_, sizeof(uint32_t) * table_size);
  uint32_t * const new_table = static_cast<uint32_t *>(table_node.body());
  for (uint64_t i = 0; i < table_size_; ++i) {
    new_table[i] = table_[i];
  }
  for (uint64_t i = table_size_; i < table_size; ++i) {
    new_table[i] = STORAGE_INVALID_NODE_ID;
  }
  // Unlink the current table.
  try {
    if (header_->table_storage_node_id != STORAGE_INVALID_NODE_ID) {
      storage_->unlink_node(header_->table_storage_node_id);
    }
  } catch (...) {
    storage_->unlink_node(table_node.id());
    throw;
  }
  // Update pointers and the header.
  table_ = new_table;
  // TODO: Old table caches should be kept.
  pages_.swap(new_pages);
  header_->table_size = table_size_ = table_size;
  header_->table_storage_node_id = table_node.id();
}

void *PagedArrayImpl::reserve_page(uint64_t page_id) {
  Lock lock(&header_->mutex);
  if (pages_[page_id] != invalid_page_address()) {
    // Nothing to do.
    return pages_[page_id];
  }
  update_table();
  StorageNode page_node;
  if (table_[page_id] != STORAGE_INVALID_NODE_ID) {
    // Open an existing page.
    page_node = storage_->open_node(table_[page_id]);
  } else {
    // Create a new page.
    page_node = storage_->create_node(header_->table_storage_node_id,
                                      header_->value_size * page_size_);
    table_[page_id] = page_node.id();
  }
  pages_[page_id] = static_cast<char *>(page_node.body()) -
                    (header_->value_size * page_size_ * page_id);
  return pages_[page_id];
}

void PagedArrayImpl::update_table() {
  if (table_size_ == header_->table_size) {
    // Nothing to do.
    return;
  }
  StorageNode table_node = storage_->open_node(header_->table_storage_node_id);
  std::unique_ptr<void *[]> new_pages(
      new (std::nothrow) void *[header_->table_size]);
  if (!new_pages) {
    GRNXX_ERROR() << "new void *[] failed: size = " << header_->table_size;
    throw MemoryError();
  }
  for (uint64_t i = 0; i < table_size_; ++i) {
    new_pages[i] = pages_[i];
  }
  for (uint64_t i = table_size_; i < header_->table_size; ++i) {
    new_pages[i] = invalid_page_address();
  }
  table_ = static_cast<uint32_t *>(table_node.body());
  // TODO: Old table caches should be kept.
  pages_.swap(new_pages);
  table_size_ = header_->table_size;
}

void PagedArrayImpl::swap(PagedArrayImpl &rhs) {
  std::swap(storage_, rhs.storage_);
  std::swap(storage_node_id_, rhs.storage_node_id_);
  std::swap(size_, rhs.size_);
  std::swap(page_size_, rhs.page_size_);
  std::swap(page_shift_, rhs.page_shift_);
  std::swap(page_mask_, rhs.page_mask_);
  std::swap(table_size_, rhs.table_size_);
  std::swap(pages_, rhs.pages_);
  std::swap(table_, rhs.table_);
  std::swap(header_, rhs.header_);
  std::swap(default_value_, rhs.default_value_);
  std::swap(fill_page_, rhs.fill_page_);
}

//void Array<bool>::create(Storage *storage, uint32_t storage_node_id,
//                         uint64_t size) {
//  if ((size % UNIT_SIZE) != 0) {
//    const uint64_t revised_size = size + UNIT_SIZE - (size % UNIT_SIZE);
//    GRNXX_WARNING() << "size must be a multiple of UNIT_SIZE: size = " << size
//                    << ", revised_size = " << revised_size
//                    << ", UNIT_SIZE = " << UNIT_SIZE;
//    size = revised_size;
//  }
//  impl_.create(storage, storage_node_id, size / UNIT_SIZE);
//  size_ = size;
//}

//void Array<bool>::create(Storage *storage, uint32_t storage_node_id,
//                         uint64_t size, ValueArg default_value) {
//  if ((size % UNIT_SIZE) != 0) {
//    const uint64_t revised_size = size + UNIT_SIZE - (size % UNIT_SIZE);
//    GRNXX_WARNING() << "size must be a multiple of UNIT_SIZE: size = " << size
//                    << ", revised_size = " << revised_size
//                    << ", UNIT_SIZE = " << UNIT_SIZE;
//    size = revised_size;
//  }
//  impl_.create(storage, storage_node_id, size / UNIT_SIZE,
//               default_value ? ~Unit(0) : Unit(0));
//  size_ = size;
//}

}  // namespace alpha
}  // namespace grnxx
