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
namespace alpha {

BlobVectorHeader::BlobVectorHeader(uint32_t cells_block_id)
  : cells_block_id_(cells_block_id),
    value_store_block_id_(io::BLOCK_INVALID_ID),
    next_medium_value_offset_(0),
    latest_large_value_block_id_(io::BLOCK_INVALID_ID),
    inter_process_mutex_() {}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorType type) {
  switch (type) {
    case BLOB_VECTOR_NULL: {
      return builder << "BLOB_VECTOR_NULL";
    }
    case BLOB_VECTOR_SMALL: {
      return builder << "BLOB_VECTOR_SMALL";
    }
    case BLOB_VECTOR_MEDIUM: {
      return builder << "BLOB_VECTOR_MEDIUM";
    }
    case BLOB_VECTOR_LARGE: {
      return builder << "BLOB_VECTOR_LARGE";
    }
    default: {
      return builder << "n/a";
    }
  }
}

StringBuilder &operator<<(StringBuilder &builder,
                          BlobVectorAttribute attribute) {
  switch (attribute) {
    case BLOB_VECTOR_APPENDABLE: {
      return builder << "BLOB_VECTOR_APPENDABLE";
    }
    case BLOB_VECTOR_PREPENDABLE: {
      return builder << "BLOB_VECTOR_PREPENDABLE";
    }
    default: {
      return builder << "n/a";
    }
  }
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

const void *BlobVectorImpl::get_value(uint64_t id, uint64_t *length) {
  if (id > BLOB_VECTOR_MAX_ID) {
    GRNXX_ERROR() << "invalid argument: id = " << id
                  << ", max_id = " << BLOB_VECTOR_MAX_ID;
    GRNXX_THROW();
  }

  const BlobVectorCell cell = cells_[id];
  switch (cell.type()) {
    case BLOB_VECTOR_NULL: {
      if (length) {
        *length = 0;
      }
      return nullptr;
    }
    case BLOB_VECTOR_SMALL: {
      if (length) {
        *length = cell.small().length();
      }
      // FIXME: The cell might be updated by other threads and processes.
      //        This interface cannot solve this problem.
      return cells_[id].small().value();
    }
    case BLOB_VECTOR_MEDIUM: {
      if (!value_store_) {
        Lock lock(mutable_inter_thread_mutex());
        if (!value_store_) {
          value_store_ = BlobVectorValueStore(
              VECTOR_OPEN, pool_, header_->value_store_block_id());
        }
      }
      if (length) {
        *length = cell.medium().length();
      }
      return &value_store_[cell.medium().offset()];
    }
    case BLOB_VECTOR_LARGE: {
      const BlobVectorLargeValueHeader *value_header =
          static_cast<const BlobVectorLargeValueHeader *>(
              pool_.get_block_address(cell.large().block_id()));
      if (length) {
        *length = value_header->length();
      }
      return value_header + 1;
    }
    default: {
      GRNXX_ERROR() << "invalid value type";
      GRNXX_THROW();
    }
  }
}

void BlobVectorImpl::set_value(uint64_t id, const void *ptr, uint64_t length,
                               uint64_t capacity,
                               BlobVectorAttribute attribute) {
  if (id > BLOB_VECTOR_MAX_ID) {
    GRNXX_ERROR() << "invalid argument: id = " << id
                  << ", max_id = " << BLOB_VECTOR_MAX_ID;
    GRNXX_THROW();
  }

  BlobVectorCell new_cell;
  if (!ptr) {
    new_cell = BlobVectorNullValue(attribute);
  } else {
    if (capacity < length) {
      capacity = length;
    }
    if (capacity < BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH) {
      new_cell = BlobVectorSmallValue(ptr, length, attribute);
    } else if (capacity < BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH) {
      new_cell = create_medium_value(id, ptr, length, capacity, attribute);
    } else {
      new_cell = create_large_value(id, ptr, length, capacity, attribute);
    }
  }

  // The old cell is replaced with the new cell.
  BlobVectorCell old_cell;
  try {
    do {
      old_cell = cells_[id];
    } while (!atomic_compare_and_swap(old_cell, new_cell, &cells_[id]));
  } catch (...) {
    free_value(new_cell);
    throw;
  }
  free_value(old_cell);
}

StringBuilder &BlobVectorImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  // TODO
  return builder;
}

void BlobVectorImpl::unlink(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<BlobVectorImpl> vector =
      BlobVectorImpl::open(pool, block_id);

  if (vector->header_->latest_large_value_block_id() != io::BLOCK_INVALID_ID) {
    uint32_t block_id = vector->header_->latest_large_value_block_id();
    do {
      auto value_header = static_cast<const BlobVectorLargeValueHeader *>(
          pool.get_block_address(block_id));
      const uint32_t prev_block_id = value_header->prev_value_block_id();
      pool.free_block(block_id);
      block_id = prev_block_id;
    } while (block_id != vector->header_->latest_large_value_block_id());
  }
  Vector<BlobVectorCell>::unlink(pool, vector->header_->cells_block_id());
  pool.free_block(vector->block_info_->id());
}

BlobVectorImpl::BlobVectorImpl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    cells_(),
    value_store_(),
    inter_thread_mutex_() {}

void BlobVectorImpl::create_vector(io::Pool pool) {
  pool_ = pool;
  block_info_ = pool.create_block(sizeof(BlobVectorHeader));

  try {
    cells_ = Vector<BlobVectorCell>(VECTOR_CREATE, pool_, BlobVectorCell());
  } catch (...) {
    pool_.free_block(*block_info_);
    throw;
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<BlobVectorHeader *>(block_address);
  *header_ = BlobVectorHeader(cells_.block_id());

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
  cells_ = Vector<BlobVectorCell>(VECTOR_OPEN, pool,
                                  header_->cells_block_id());
}

BlobVectorMediumValue BlobVectorImpl::create_medium_value(
    uint32_t id, const void *ptr, uint64_t length, uint64_t capacity,
    BlobVectorAttribute attribute) {
  if (!value_store_) {
    Lock lock(mutable_inter_thread_mutex());
    if (!value_store_) {
      value_store_ = BlobVectorValueStore(
          VECTOR_OPEN, pool_, header_->value_store_block_id());
    }
  }

  // TODO: Lock.

  capacity = (capacity + (BLOB_VECTOR_UNIT_SIZE - 1)) &
      ~(BLOB_VECTOR_UNIT_SIZE - 1);

  uint64_t offset = header_->next_medium_value_offset();
  const uint64_t offset_in_page =
      offset & (BLOB_VECTOR_VALUE_STORE_PAGE_SIZE - 1);
  const uint64_t size_left_in_page =
      BLOB_VECTOR_VALUE_STORE_PAGE_SIZE - offset_in_page;

  const uint64_t required_size =
      capacity + sizeof(BlobVectorMediumValueHeader);
  if (required_size > size_left_in_page) {
    offset += size_left_in_page;
  }
  header_->set_next_medium_value_offset(offset + required_size);

  auto value_header = reinterpret_cast<BlobVectorMediumValueHeader *>(
      &value_store_[offset]);
  value_header->set_value_id(id);
  value_header->set_capacity(capacity);
  std::memcpy(value_header + 1, ptr, length);

  return BlobVectorMediumValue(offset, length, attribute);
}

BlobVectorLargeValue BlobVectorImpl::create_large_value(
    uint32_t, const void *ptr, uint64_t length, uint64_t capacity,
    BlobVectorAttribute attribute) {
  const io::BlockInfo *block_info =
      pool_.create_block(sizeof(BlobVectorLargeValueHeader) + capacity);
  BlobVectorLargeValueHeader *value_header =
      static_cast<BlobVectorLargeValueHeader *>(
          pool_.get_block_address(*block_info));
  value_header->set_length(length);
  std::memcpy(value_header + 1, ptr, length);
  register_large_value(block_info->id(), value_header);
  return BlobVectorLargeValue(block_info->id(), attribute);
}

void BlobVectorImpl::free_value(BlobVectorCell cell) {
  switch (cell.type()) {
    case BLOB_VECTOR_NULL:
    case BLOB_VECTOR_SMALL: {
      break;
    }
    case BLOB_VECTOR_MEDIUM: {
      break;
    }
    case BLOB_VECTOR_LARGE: {
      const io::BlockInfo * const block_info =
          pool_.get_block_info(cell.large().block_id());
      unregister_large_value(block_info->id(),
          static_cast<BlobVectorLargeValueHeader *>(
              pool_.get_block_address(*block_info)));
      pool_.free_block(*block_info);
      break;
    }
  }
}

void BlobVectorImpl::register_large_value(uint32_t block_id,
    BlobVectorLargeValueHeader *value_header) {
  Lock lock(mutable_inter_process_mutex());
  if (header_->latest_large_value_block_id() == io::BLOCK_INVALID_ID) {
    value_header->set_next_value_block_id(block_id);
    value_header->set_prev_value_block_id(block_id);
  } else {
    const uint32_t prev_id = header_->latest_large_value_block_id();
    auto prev_header = static_cast<BlobVectorLargeValueHeader *>(
        pool_.get_block_address(prev_id));
    const uint32_t next_id = prev_header->next_value_block_id();
    auto next_header = static_cast<BlobVectorLargeValueHeader *>(
        pool_.get_block_address(next_id));
    value_header->set_next_value_block_id(next_id);
    value_header->set_prev_value_block_id(prev_id);
    prev_header->set_next_value_block_id(block_id);
    next_header->set_prev_value_block_id(block_id);
  }
  header_->set_latest_large_value_block_id(block_id);
}

void BlobVectorImpl::unregister_large_value(uint32_t block_id,
    BlobVectorLargeValueHeader *value_header) {
  Lock lock(mutable_inter_process_mutex());
  const uint32_t next_id = value_header->next_value_block_id();
  const uint32_t prev_id = value_header->prev_value_block_id();
  auto next_header = static_cast<BlobVectorLargeValueHeader *>(
      pool_.get_block_address(next_id));
  auto prev_header = static_cast<BlobVectorLargeValueHeader *>(
      pool_.get_block_address(prev_id));
  next_header->set_prev_value_block_id(prev_id);
  prev_header->set_next_value_block_id(next_id);
  if (block_id == header_->latest_large_value_block_id()) {
    header_->set_latest_large_value_block_id(prev_id);
  }
}

}  // namespace alpha
}  // namespace grnxx
