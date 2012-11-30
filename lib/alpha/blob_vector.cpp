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
      // TODO: Not implemented yet.
      return nullptr;
    }
    case BLOB_VECTOR_LARGE: {
      void * const block_address =
          pool_.get_block_address(cell.large().block_id());
      if (length) {
        *length = *static_cast<uint64_t *>(block_address);
      }
      return static_cast<uint64_t *>(block_address) + 1;
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
      new_cell = create_medium_value(ptr, length, capacity, attribute);
    } else {
      new_cell = create_large_value(ptr, length, capacity, attribute);
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

BlobVectorMediumValue BlobVectorImpl::create_medium_value(
    const void *ptr, uint64_t length, uint64_t capacity,
    BlobVectorAttribute attribute) {
  // TODO: Not implemented yet.
  capacity = (capacity + (BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE - 1)) &
      ~(BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE - 1);
  return BlobVectorMediumValue(0, length, attribute);
}

BlobVectorLargeValue BlobVectorImpl::create_large_value(
    const void *ptr, uint64_t length, uint64_t capacity,
    BlobVectorAttribute attribute) {
  const io::BlockInfo *block_info =
      pool_.create_block(sizeof(uint64_t) + capacity);
  void * const block_address = pool_.get_block_address(*block_info);
  *static_cast<uint64_t *>(block_address) = length;
  std::memcpy(static_cast<uint64_t *>(block_address) + 1, ptr, length);
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
      pool_.free_block(cell.large().block_id());
      break;
    }
  }
}

StringBuilder &BlobVectorImpl::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  // TODO
  return builder;
}

void BlobVectorImpl::unlink(io::Pool pool, uint32_t block_id) {
  // TODO
}

BlobVectorImpl::BlobVectorImpl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    cells_(),
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

}  // namespace alpha
}  // namespace grnxx
