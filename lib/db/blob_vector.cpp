/*
  Copyright (C) 2012  Brazil, Inc.

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
#include "blob_vector.hpp"

#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace db {

BlobVectorCreate BLOB_VECTOR_CREATE;
BlobVectorOpen BLOB_VECTOR_OPEN;

BlobVectorHeader::BlobVectorHeader(uint32_t table_block_id)
  : table_block_id_(table_block_id),
    value_store_block_id_(io::BLOCK_INVALID_ID),
    index_store_block_id_(io::BLOCK_INVALID_ID),
    next_page_id_(0),
    next_value_offset_(0),
    latest_frozen_page_id_(BLOB_VECTOR_INVALID_PAGE_ID),
    latest_large_value_block_id_(io::BLOCK_INVALID_ID),
    inter_process_mutex_(MUTEX_UNLOCKED) {}

StringBuilder &BlobVectorHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ table_block_id = " << table_block_id_
          << ", value_store_block_id = " << value_store_block_id_
          << ", index_store_block_id = " << index_store_block_id_
          << ", next_page_id = " << next_page_id_
          << ", next_value_offset = " << next_value_offset_
          << ", latest_large_value_block_id = " << latest_large_value_block_id_
          << ", inter_process_mutex = " << inter_process_mutex_;
  return builder << " }";
}

std::unique_ptr<BlobVectorImpl> BlobVectorImpl::create(io::Pool pool) {
  std::unique_ptr<BlobVectorImpl> vector(new (std::nothrow) BlobVectorImpl);
  if (!vector) {
    GRNXX_ERROR() << "new grnxx::io::VectorImpl failed";
    GRNXX_THROW();
  }
  vector->create_vector(pool);
  return vector;
}

std::unique_ptr<BlobVectorImpl> BlobVectorImpl::open(io::Pool pool,
                                                     uint32_t block_id) {
  std::unique_ptr<BlobVectorImpl> vector(new (std::nothrow) BlobVectorImpl);
  if (!vector) {
    GRNXX_ERROR() << "new grnxx::io::VectorImpl failed";
    GRNXX_THROW();
  }
  vector->open_vector(pool, block_id);
  return vector;
}

void BlobVectorImpl::set_value(uint64_t id, const Blob &value) {
  const BlobVectorCell new_cell = create_value(value);
  BlobVectorCell old_cell;
  try {
    do {
      old_cell = table_[id];
    } while (!atomic_compare_and_swap(old_cell, new_cell, &table_[id]));
  } catch (...) {
    // The new value is freed on failure.
    free_value(new_cell);
    throw;
  }
  // The old value is freed on success.
  free_value(old_cell);
}

void BlobVectorImpl::append(uint64_t id, const Blob &value) {
  if (!value || (value.length() == 0)) {
    return;
  }

  for ( ; ; ) {
    const BlobVectorCell old_cell = table_[id];
    const Blob old_value = get_value(old_cell);
    const BlobVectorCell new_cell = join_values(old_value, value);
    if (atomic_compare_and_swap(old_cell, new_cell, &table_[id])) {
      // The old value is freed on success.
      free_value(old_cell);
      break;
    } else {
      // The new value is freed on failure.
      free_value(new_cell);
    }
  }
}

void BlobVectorImpl::prepend(uint64_t id, const Blob &value) {
  if (!value || (value.length() == 0)) {
    return;
  }

  for ( ; ; ) {
    const BlobVectorCell old_cell = table_[id];
    const Blob old_value = get_value(old_cell);
    const BlobVectorCell new_cell = join_values(value, old_value);
    if (atomic_compare_and_swap(old_cell, new_cell, &table_[id])) {
      // The old value is freed on success.
      free_value(old_cell);
      break;
    } else {
      // The new value is freed on failure.
      free_value(new_cell);
    }
  }
}

void BlobVectorImpl::defrag() {
  // TODO: To be more efficient.
  table_.scan([this](uint64_t, BlobVectorCell *cell) -> bool {
    if (cell->type() != BLOB_VECTOR_MEDIUM) {
      return true;
    }

    const BlobVectorCell old_cell = *cell;
    const BlobVectorCell new_cell = create_value(get_value(old_cell));
    if (atomic_compare_and_swap(old_cell, new_cell, cell)) {
      free_value(old_cell);
    } else {
      free_value(new_cell);
    }
    return true;
  });
}

StringBuilder &BlobVectorImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ pool = " << pool_.path()
          << ", block_info = " << *block_info_
          << ", header = " << *header_
          << ", inter_thread_mutex = " << inter_thread_mutex_;
  return builder << " }";
}

void BlobVectorImpl::unlink(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<BlobVectorImpl> vector =
      BlobVectorImpl::open(pool, block_id);

  if (vector->header_->latest_large_value_block_id() != io::BLOCK_INVALID_ID) {
    uint32_t block_id = vector->header_->latest_large_value_block_id();
    do {
      auto value_header = static_cast<const BlobVectorValueHeader *>(
          pool.get_block_address(block_id));
      const uint32_t prev_block_id = value_header->prev_value_block_id();
      pool.free_block(block_id);
      block_id = prev_block_id;
    } while (block_id != vector->header_->latest_large_value_block_id());
  }
  BlobVectorTable::unlink(pool, vector->header_->table_block_id());
  pool.free_block(vector->block_info_->id());
}

BlobVectorImpl::BlobVectorImpl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    table_(),
    value_store_(),
    index_store_(),
    inter_thread_mutex_(MUTEX_UNLOCKED) {}

void BlobVectorImpl::create_vector(io::Pool pool) {
  pool_ = pool;
  block_info_ = pool.create_block(sizeof(BlobVectorHeader));

  try {
    table_.create(pool_, BlobVectorCell::null_value());
  } catch (...) {
    pool_.free_block(*block_info_);
    throw;
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<BlobVectorHeader *>(block_address);
  *header_ = BlobVectorHeader(table_.block_id());

  recycler_ = pool.mutable_recycler();
}

void BlobVectorImpl::open_vector(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  block_info_ = pool.get_block_info(block_id);
  if (block_info_->size() < sizeof(BlobVectorHeader)) {
    GRNXX_ERROR() << "invalid argument: block_info = " << *block_info_
                  << ", header_size = " << sizeof(BlobVectorHeader);
    GRNXX_THROW();
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<BlobVectorHeader *>(block_address);

  // TODO: Check the format!

  recycler_ = pool.mutable_recycler();

  // Open the core table.
  table_.open(pool, header_->table_block_id());
}

Blob BlobVectorImpl::get_value(BlobVectorCell cell) {
  switch (cell.type()) {
    case BLOB_VECTOR_NULL: {
      return Blob(nullptr);
    }
    case BLOB_VECTOR_SMALL: {
      return Blob(cell);
    }
    case BLOB_VECTOR_MEDIUM: {
      if (!value_store_) {
        Lock lock(mutable_inter_thread_mutex());
        if (!value_store_) {
          value_store_.open(pool_, header_->value_store_block_id());
        }
      }
      return Blob(&value_store_[cell.offset()], cell.medium_length());
    }
    case BLOB_VECTOR_LARGE: {
      const auto value_header = static_cast<const BlobVectorValueHeader *>(
          pool_.get_block_address(cell.block_id()));
      return Blob(value_header + 1, value_header->length());
    }
    default: {
      GRNXX_ERROR() << "invalid value type";
      GRNXX_THROW();
    }
  }
}

BlobVectorCell BlobVectorImpl::create_value(const Blob &value) {
  if (!value) {
    return BlobVectorCell::null_value();
  }

  BlobVectorCell cell;
  void *address;
  if (value.length() < BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH) {
    address = create_small_value(value.length(), &cell);
  } else if (value.length() < BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH) {
    address = create_medium_value(value.length(), &cell);
  } else {
    address = create_large_value(value.length(), &cell);
  }
  std::memcpy(address, value.address(), value.length());
  return cell;
}

BlobVectorCell BlobVectorImpl::join_values(const Blob &lhs, const Blob &rhs) {
  const uint64_t length = lhs.length() + rhs.length();
  BlobVectorCell cell;
  void *address;
  if (length < BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH) {
    address = create_small_value(length, &cell);
  } else if (length < BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH) {
    address = create_medium_value(length, &cell);
  } else {
    address = create_large_value(length, &cell);
  }
  std::memcpy(address, lhs.address(), lhs.length());
  std::memcpy(static_cast<char *>(address) + lhs.length(),
              rhs.address(), rhs.length());
  return cell;
}

void *BlobVectorImpl::create_small_value(uint64_t length,
                                         BlobVectorCell *cell) {
  *cell = BlobVectorCell::small_value(length);
  return cell->value();
}

void *BlobVectorImpl::create_medium_value(uint64_t length,
                                          BlobVectorCell *cell) {
  Lock lock(mutable_inter_thread_mutex());

  if (!value_store_) {
    if (header_->value_store_block_id() == io::BLOCK_INVALID_ID) {
      Lock lock(mutable_inter_process_mutex());
      if (header_->value_store_block_id() == io::BLOCK_INVALID_ID) {
        value_store_.create(pool_);
        header_->set_value_store_block_id(value_store_.block_id());
      }
    }
    if (!value_store_) {
      value_store_.open(pool_, header_->value_store_block_id());
    }
  }

  if (!index_store_) {
    if (header_->index_store_block_id() == io::BLOCK_INVALID_ID) {
      Lock lock(mutable_inter_process_mutex());
      if (header_->index_store_block_id() == io::BLOCK_INVALID_ID) {
        index_store_.create(pool_, BlobVectorPageInfo());
        header_->set_index_store_block_id(index_store_.block_id());
      }
    }
    if (!index_store_) {
      index_store_.open(pool_, header_->index_store_block_id());
    }
  }

  // Unfreeze the oldest frozen page for reuse.
  unfreeze_oldest_frozen_page();

  uint64_t offset = header_->next_value_offset();
  const uint64_t offset_in_page =
      ((offset - 1) & (BLOB_VECTOR_VALUE_STORE_PAGE_SIZE - 1)) + 1;
  const uint64_t size_left_in_page =
      BLOB_VECTOR_VALUE_STORE_PAGE_SIZE - offset_in_page;

  // Reserve a new page if there is not enough space in the current page.
  if (length > size_left_in_page) {
    if (offset != 0) {
      // Freeze the current page if it is empty.
      const uint32_t page_id = static_cast<uint32_t>(
          (offset - 1) >> BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS);
      if (index_store_[page_id].num_values() == 0) {
        freeze_page(page_id);
      }
    }

    const uint32_t page_id = header_->next_page_id();
    offset = static_cast<uint64_t>(
        page_id << BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS);
    if (index_store_[page_id].next_page_id() != BLOB_VECTOR_INVALID_PAGE_ID) {
      header_->set_next_page_id(index_store_[page_id].next_page_id());
    } else {
      header_->set_next_page_id(page_id + 1);
    }
    index_store_[page_id].set_num_values(0);
  }
  header_->set_next_value_offset(offset + length);

  const uint32_t page_id = static_cast<uint32_t>(
      offset >> BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS);
  index_store_[page_id].set_num_values(index_store_[page_id].num_values() + 1);

  *cell = BlobVectorCell::medium_value(offset, length);
  return &value_store_[offset];
}

void *BlobVectorImpl::create_large_value(uint64_t length,
                                         BlobVectorCell *cell) {
  const io::BlockInfo *block_info =
      pool_.create_block(sizeof(BlobVectorValueHeader) + length);
  auto value_header = static_cast<BlobVectorValueHeader *>(
      pool_.get_block_address(*block_info));
  value_header->set_length(length);
  register_large_value(block_info->id(), value_header);
  *cell = BlobVectorCell::large_value(block_info->id());
  return value_header + 1;
}

void BlobVectorImpl::free_value(BlobVectorCell cell) {
  switch (cell.type()) {
    case BLOB_VECTOR_NULL:
    case BLOB_VECTOR_SMALL: {
      break;
    }
    case BLOB_VECTOR_MEDIUM: {
      Lock lock(mutable_inter_thread_mutex());

      const uint32_t page_id = static_cast<uint32_t>(
          cell.offset() >> BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS);
      index_store_[page_id].set_num_values(
          index_store_[page_id].num_values() - 1);
      if (index_store_[page_id].num_values() == 0) {
        const uint32_t current_page_id =
            static_cast<uint32_t>(header_->next_value_offset()
                                  >> BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS);
        if (page_id != current_page_id) {
          freeze_page(page_id);
        }
      }
      break;
    }
    case BLOB_VECTOR_LARGE: {
      const io::BlockInfo * const block_info =
          pool_.get_block_info(cell.block_id());
      unregister_large_value(block_info->id(),
          static_cast<BlobVectorValueHeader *>(
              pool_.get_block_address(*block_info)));
      pool_.free_block(*block_info);
      break;
    }
  }
}

void BlobVectorImpl::register_large_value(uint32_t block_id,
    BlobVectorValueHeader *value_header) {
  Lock lock(mutable_inter_process_mutex());
  if (header_->latest_large_value_block_id() == io::BLOCK_INVALID_ID) {
    value_header->set_next_value_block_id(block_id);
    value_header->set_prev_value_block_id(block_id);
  } else {
    const uint32_t prev_id = header_->latest_large_value_block_id();
    auto prev_header = static_cast<BlobVectorValueHeader *>(
        pool_.get_block_address(prev_id));
    const uint32_t next_id = prev_header->next_value_block_id();
    auto next_header = static_cast<BlobVectorValueHeader *>(
        pool_.get_block_address(next_id));
    value_header->set_next_value_block_id(next_id);
    value_header->set_prev_value_block_id(prev_id);
    prev_header->set_next_value_block_id(block_id);
    next_header->set_prev_value_block_id(block_id);
  }
  header_->set_latest_large_value_block_id(block_id);
}

void BlobVectorImpl::unregister_large_value(uint32_t block_id,
    BlobVectorValueHeader *value_header) {
  Lock lock(mutable_inter_process_mutex());
  const uint32_t next_id = value_header->next_value_block_id();
  const uint32_t prev_id = value_header->prev_value_block_id();
  auto next_header = static_cast<BlobVectorValueHeader *>(
      pool_.get_block_address(next_id));
  auto prev_header = static_cast<BlobVectorValueHeader *>(
      pool_.get_block_address(prev_id));
  next_header->set_prev_value_block_id(prev_id);
  prev_header->set_next_value_block_id(next_id);
  if (block_id == header_->latest_large_value_block_id()) {
    header_->set_latest_large_value_block_id(prev_id);
  }
}

void BlobVectorImpl::freeze_page(uint32_t page_id) {
  BlobVectorPageInfo &page_info = index_store_[page_id];
  if (header_->latest_frozen_page_id() != BLOB_VECTOR_INVALID_PAGE_ID) {
    BlobVectorPageInfo &latest_frozen_page_info =
        index_store_[header_->latest_frozen_page_id()];
    page_info.set_next_page_id(latest_frozen_page_info.next_page_id());
    latest_frozen_page_info.set_next_page_id(page_id);
  } else {
    page_info.set_next_page_id(page_id);
  }
  page_info.set_stamp(recycler_->stamp());
  header_->set_latest_frozen_page_id(page_id);
}

void BlobVectorImpl::unfreeze_oldest_frozen_page() {
  if (header_->latest_frozen_page_id() != BLOB_VECTOR_INVALID_PAGE_ID) {
    BlobVectorPageInfo &latest_frozen_page_info =
        index_store_[header_->latest_frozen_page_id()];
    const uint32_t oldest_frozen_page_id =
        latest_frozen_page_info.next_page_id();
    BlobVectorPageInfo &oldest_frozen_page_info =
        index_store_[oldest_frozen_page_id];
    if (recycler_->check(oldest_frozen_page_info.stamp())) {
      latest_frozen_page_info.set_next_page_id(
          oldest_frozen_page_info.next_page_id());
      oldest_frozen_page_info.set_next_page_id(header_->next_page_id());
      header_->set_next_page_id(oldest_frozen_page_id);
      if (oldest_frozen_page_id == header_->latest_frozen_page_id()) {
        header_->set_latest_frozen_page_id(BLOB_VECTOR_INVALID_PAGE_ID);
      }
    }
  }
}

}  // namespace db
}  // namespace grnxx
