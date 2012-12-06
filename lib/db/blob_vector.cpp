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

#include <ostream>

#include "../lock.hpp"

namespace grnxx {
namespace db {

void BlobVectorHeader::initialize(uint32_t cells_block_id,
                                  Duration frozen_duration) {
  std::memset(this, 0, sizeof(*this));

  cells_block_id_ = cells_block_id;
  frozen_duration_ = frozen_duration;

  for (uint32_t i = 0; i < BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM; ++i) {
    medium_value_store_block_ids_[i] = io::BLOCK_INVALID_ID;
  }

  large_value_store_block_id_ = io::BLOCK_INVALID_ID;
  rearmost_large_value_offset_ = BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET;
  latest_frozen_large_value_offset_ = BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET;
  for (uint32_t i = 0; i < BLOB_VECTOR_LARGE_VALUE_LISTS_NUM; ++i) {
    oldest_idle_large_value_offsets_[i] =
        BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET;
  }

  inter_process_mutex_.unlock();
  medium_value_store_mutex_.unlock();
  large_value_store_mutex_.unlock();
}

BlobVector::BlobVector()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    cells_(),
    medium_value_stores_(),
    large_value_store_(),
    inter_thread_mutex_(MUTEX_UNLOCKED) {}

BlobVector::~BlobVector() {}

BlobVector::BlobVector(BlobVector &&rhs)
  : pool_(std::move(rhs.pool_)),
    block_info_(std::move(rhs.block_info_)),
    header_(std::move(rhs.header_)),
    recycler_(std::move(rhs.recycler_)),
    cells_(std::move(rhs.cells_)),
    medium_value_stores_(std::move(rhs.medium_value_stores_)),
    large_value_store_(std::move(rhs.large_value_store_)),
    inter_thread_mutex_(std::move(rhs.inter_thread_mutex_)) {}

BlobVector &BlobVector::operator=(BlobVector &&rhs) {
  pool_ = std::move(rhs.pool_);
  block_info_ = std::move(rhs.block_info_);
  header_ = std::move(rhs.header_);
  recycler_ = std::move(rhs.recycler_);
  cells_ = std::move(rhs.cells_);
  medium_value_stores_ = std::move(rhs.medium_value_stores_);
  large_value_store_ = std::move(rhs.large_value_store_);
  inter_thread_mutex_ = std::move(rhs.inter_thread_mutex_);
  return *this;
}

void BlobVector::create(io::Pool pool) {
  if (!pool) {
    GRNXX_ERROR() << "invalid argument: pool = " << pool;
    GRNXX_THROW();
  }

  BlobVector new_vector;
  new_vector.create_vector(pool);
  *this = std::move(new_vector);
}

void BlobVector::open(io::Pool pool, uint32_t block_id) {
  if (!pool) {
    GRNXX_ERROR() << "invalid argument: pool = " << pool;
    GRNXX_THROW();
  }

  BlobVector new_vector;
  new_vector.open_vector(pool, block_id);
  *this = std::move(new_vector);
}

void BlobVector::close() {
  if (!is_open()) {
    GRNXX_ERROR() << "failed to close vector";
    GRNXX_THROW();
  }
  *this = BlobVector();
}

const void *BlobVector::get_value_address(uint64_t id, uint64_t *length) {
  const BlobVectorCell cell = cells_[id];
  switch (cell.value_type()) {
    case BLOB_VECTOR_SMALL_VALUE: {
      if (length) {
        *length = cell.small_value_cell().length();
      }
      // FIXME: the cell might be updated by other threads and processes.
      return cells_[id].small_value_cell().value();
    }
    case BLOB_VECTOR_MEDIUM_VALUE: {
      if (length) {
        *length = cell.medium_value_cell().length();
      }
      const uint8_t store_id = cell.medium_value_cell().store_id();
      const uint64_t offset = cell.medium_value_cell().offset();
      BlobVectorMediumValueStore &store = medium_value_stores_[store_id];
      if (!store.is_open()) {
        open_medium_value_store(store_id);
      }
      return &store[offset];
    }
    case BLOB_VECTOR_LARGE_VALUE: {
      if (length) {
        *length = cell.large_value_cell().length();
      }
      const uint64_t offset = cell.large_value_cell().offset();
      if (!large_value_store_.is_open()) {
        open_large_value_store();
      }
      return get_large_value_header(offset)->value();
    }
    case BLOB_VECTOR_HUGE_VALUE: {
      void *block_address =
          pool_.get_block_address(cell.huge_value_cell().block_id());
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

void BlobVector::set_value(uint64_t id, const void *ptr, uint64_t length) {
  if (!ptr && (length != 0)) {
    GRNXX_ERROR() << "invalid arguments: ptr = " << ptr
                  << ", length = " << length;
    GRNXX_THROW();
  }

  BlobVectorCell new_cell;
  if (length <= BLOB_VECTOR_SMALL_VALUE_LENGTH_MAX) {
    new_cell = create_small_value_cell(ptr, length);
  } else if (length <= BLOB_VECTOR_MEDIUM_VALUE_LENGTH_MAX) {
    new_cell = create_medium_value_cell(ptr, length);
  } else if (length <= BLOB_VECTOR_LARGE_VALUE_LENGTH_MAX) {
    new_cell = create_large_value_cell(ptr, length);
  } else {
    new_cell = create_huge_value_cell(ptr, length);
  }

  // The old cell is replaced with the new one.
  // Then, the resources allocated to the old cell are freed.
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

void BlobVector::swap(BlobVector &rhs) {
  using std::swap;
  swap(pool_, rhs.pool_);
  swap(block_info_, rhs.block_info_);
  swap(header_, rhs.header_);
  swap(recycler_, rhs.recycler_);
  swap(cells_, rhs.cells_);
  swap(medium_value_stores_, rhs.medium_value_stores_);
  swap(large_value_store_, rhs.large_value_store_);
  swap(inter_thread_mutex_, rhs.inter_thread_mutex_);
}

void BlobVector::unlink(io::Pool pool, uint32_t block_id) {
  if (!pool) {
    GRNXX_ERROR() << "invalid argument: pool = " << pool;
    GRNXX_THROW();
  }

  BlobVector vector;
  vector.open(pool, block_id);

  // TODO
}

void BlobVector::create_vector(io::Pool pool) {
  pool_ = pool;
  block_info_ = pool.create_block(sizeof(BlobVectorHeader));

  try {
    cells_.create(&pool_, BlobVectorCell());
  } catch (...) {
    pool_.free_block(*block_info_);
    throw;
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<BlobVectorHeader *>(block_address);
  header_->initialize(cells_.block_id(), pool.options().frozen_duration());

  recycler_ = pool.mutable_recycler();
}

void BlobVector::open_vector(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  block_info_ = pool.get_block_info(block_id);
  if (block_info_->size() < sizeof(BlobVectorHeader)) {
    GRNXX_ERROR() << "invalid argument: block_info = " << *block_info_
                  << ", header_size = " << sizeof(BlobVectorHeader);
    GRNXX_THROW();
  }

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<BlobVectorHeader *>(block_address);

  // TODO: check the header!

  recycler_ = pool.mutable_recycler();

  // Open the core table.
  cells_.open(&pool, header_->cells_block_id());
}

BlobVectorSmallValueCell BlobVector::create_small_value_cell(
    const void *ptr, uint64_t length) {
  return BlobVectorSmallValueCell(ptr, length);
}

BlobVectorMediumValueCell BlobVector::create_medium_value_cell(
    const void *ptr, uint64_t length) {
  const uint8_t store_id = get_store_id(length);
  BlobVectorMediumValueStore &store = medium_value_stores_[store_id];
  if (!store.is_open()) {
    open_medium_value_store(store_id);
  }

  uint64_t offset;
  {
    Lock lock(header_->mutable_medium_value_store_mutex());

    // TODO: Reuse.

    offset = header_->medium_value_store_next_offsets(store_id);
    if (offset > store.id_max()) {
      GRNXX_ERROR() << "store is full: offset = " << offset
                    << ", id_max = " << store.id_max();
      GRNXX_THROW();
    }
    header_->set_medium_value_store_next_offsets(store_id,
        offset + (1 << (store_id + BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS)));
  }

  std::memcpy(&store[offset], ptr, length);
  return BlobVectorMediumValueCell(store_id, offset, length);
}

BlobVectorLargeValueCell BlobVector::create_large_value_cell(
    const void *ptr, uint64_t length) {
  typedef BlobVectorLargeValueHeader ValueHeader;

  if (!large_value_store_.is_open()) {
    open_large_value_store();
  }

  const uint64_t capacity = ~(BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE - 1) &
      (length + (BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE - 1));
  const uint64_t required_size = sizeof(ValueHeader) + capacity;

  uint64_t offset;
  {
    Lock lock(header_->mutable_large_value_store_mutex());

    unfreeze_frozen_large_values();

    uint8_t list_id = get_list_id(capacity - 1);
    for (++list_id ; list_id < BLOB_VECTOR_LARGE_VALUE_LISTS_NUM; ++list_id) {
      if (header_->oldest_idle_large_value_offsets(list_id) !=
          BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
        offset = header_->oldest_idle_large_value_offsets(list_id);
        break;
      }
    }

    if (list_id < BLOB_VECTOR_LARGE_VALUE_LISTS_NUM) {
      auto header = get_large_value_header(offset);
      if (header->capacity() > capacity) {
        divide_idle_large_value(offset, capacity);
      } else {
        unregister_idle_large_value(offset);
        header->set_type(BLOB_VECTOR_ACTIVE_VALUE);
      }
    } else {
      BlobVectorLargeValueFlags flags;
      uint64_t prev_capacity = 0;

      const uint64_t prev_offset = header_->rearmost_large_value_offset();
      ValueHeader *prev_header = nullptr;
      if (prev_offset == BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
        offset = 0;
      } else {
        prev_header = get_large_value_header(prev_offset);
        offset = prev_offset + prev_header->capacity() + sizeof(ValueHeader);

        const uint64_t size_left = BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE
            - (offset & (BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE - 1));
        if (size_left < required_size) {
          auto header = get_large_value_header(offset);
          header->initialize(BLOB_VECTOR_IDLE_VALUE,
                             BLOB_VECTOR_LARGE_VALUE_HAS_PREV,
                             size_left - sizeof(ValueHeader),
                             prev_header->capacity());
          prev_header->set_flags(
              prev_header->flags() | BLOB_VECTOR_LARGE_VALUE_HAS_NEXT);
          header_->set_rearmost_large_value_offset(offset);
          register_idle_large_value(offset);
          offset += size_left;
        } else if (size_left < BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE) {
          flags |= BLOB_VECTOR_LARGE_VALUE_HAS_PREV;
          prev_capacity = prev_header->capacity();
        }
      }

      auto header = get_large_value_header(offset);
      header->initialize(BLOB_VECTOR_ACTIVE_VALUE,
                         flags, capacity, prev_capacity);
      if (flags & BLOB_VECTOR_LARGE_VALUE_HAS_PREV) {
        prev_header->set_flags(
            prev_header->flags() | BLOB_VECTOR_LARGE_VALUE_HAS_NEXT);
      }
      header_->set_rearmost_large_value_offset(offset);
    }
  }

  std::memcpy(get_large_value_header(offset)->value(), ptr, length);
  return BlobVectorLargeValueCell(offset, length);
}

BlobVectorHugeValueCell BlobVector::create_huge_value_cell(
    const void *ptr, uint64_t length) {
  const io::BlockInfo *block_info =
      pool_.create_block(sizeof(uint64_t) + length);
  void * const block_address = pool_.get_block_address(*block_info);
  *static_cast<uint64_t *>(block_address) = length;
  std::memcpy(static_cast<uint64_t *>(block_address) + 1, ptr, length);
  return BlobVectorHugeValueCell(block_info->id());
}

void BlobVector::free_value(BlobVectorCell cell) {
  switch (cell.value_type()) {
    case BLOB_VECTOR_SMALL_VALUE: {
      // Nothing to do.
      break;
    }
    case BLOB_VECTOR_MEDIUM_VALUE: {
      // TODO
      break;
    }
    case BLOB_VECTOR_LARGE_VALUE: {
      typedef BlobVectorLargeValueHeader ValueHeader;
      Lock lock(header_->mutable_large_value_store_mutex());
      if (!large_value_store_.is_open()) {
        open_large_value_store();
      }
      const uint64_t offset = cell.large_value_cell().offset();
      ValueHeader * const header = get_large_value_header(offset);
      header->set_frozen_stamp(recycler_->stamp());
      header->set_type(BLOB_VECTOR_FROZEN_VALUE);
      const uint64_t latest_offset =
          header_->latest_frozen_large_value_offset();
      if (latest_offset == BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
        header->set_next_offset(offset);
      } else {
        ValueHeader * const latest_header =
            get_large_value_header(header_->latest_frozen_large_value_offset());
        header->set_next_offset(latest_header->next_offset());
        latest_header->set_next_offset(offset);
      }
      header_->set_latest_frozen_large_value_offset(offset);
      break;
    }
    case BLOB_VECTOR_HUGE_VALUE: {
      pool_.free_block(cell.huge_value_cell().block_id());
      break;
    }
  }
}

void BlobVector::open_medium_value_store(uint8_t store_id) {
  Lock inter_thread_lock(&inter_thread_mutex_);
  BlobVectorMediumValueStore &store = medium_value_stores_[store_id];
  if (!store.is_open()) {
    if (header_->medium_value_store_block_ids(store_id) ==
        io::BLOCK_INVALID_ID) {
      Lock inter_process_lock(header_->mutable_inter_process_mutex());
      if (header_->medium_value_store_block_ids(store_id) ==
          io::BLOCK_INVALID_ID) {
        store.create(&pool_);
        header_->set_medium_value_store_block_ids(store_id, store.block_id());
      }
    }
    if (!store.is_open()) {
      store.open(&pool_, header_->medium_value_store_block_ids(store_id));
    }
  }
}

void BlobVector::open_large_value_store() {
  Lock inter_thread_lock(&inter_thread_mutex_);
  if (!large_value_store_.is_open()) {
    if (header_->large_value_store_block_id() == io::BLOCK_INVALID_ID) {
      Lock inter_process_lock(header_->mutable_inter_process_mutex());
      if (header_->large_value_store_block_id() == io::BLOCK_INVALID_ID) {
        large_value_store_.create(&pool_);
        header_->set_large_value_store_block_id(large_value_store_.block_id());
      }
    }
    if (!large_value_store_.is_open()) {
      large_value_store_.open(&pool_, header_->large_value_store_block_id());
    }
  }
}

void BlobVector::unfreeze_frozen_large_values() {
  if (header_->latest_frozen_large_value_offset() !=
      BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
    auto latest_frozen_value_header =
        get_large_value_header(header_->latest_frozen_large_value_offset());
    for (int i = 0; i < 5; ++i) {
      const uint64_t oldest_frozen_value_offset =
          latest_frozen_value_header->next_offset();
      auto oldest_frozen_value_header =
          get_large_value_header(oldest_frozen_value_offset);
      if (!recycler_->check(oldest_frozen_value_header->frozen_stamp())) {
        break;
      }
      latest_frozen_value_header->set_next_offset(
          oldest_frozen_value_header->next_offset());
      oldest_frozen_value_header->set_type(BLOB_VECTOR_IDLE_VALUE);
      register_idle_large_value(oldest_frozen_value_offset);
      merge_idle_large_values(oldest_frozen_value_offset);
      if (latest_frozen_value_header == oldest_frozen_value_header) {
        header_->set_latest_frozen_large_value_offset(
            BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET);
        break;
      }
    }
  }
}

void BlobVector::divide_idle_large_value(uint64_t offset, uint64_t capacity) {
  typedef BlobVectorLargeValueHeader ValueHeader;

  unregister_idle_large_value(offset);

  const uint64_t next_offset = offset + capacity + sizeof(ValueHeader);
  auto header = get_large_value_header(offset);
  auto next_header = get_large_value_header(next_offset);
  next_header->initialize(BLOB_VECTOR_IDLE_VALUE,
                          BLOB_VECTOR_LARGE_VALUE_HAS_PREV |
                          (header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT),
                          header->capacity() - capacity - sizeof(ValueHeader),
                          capacity);

  if (header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT) {
    const uint64_t next_next_offset =
        offset + header->capacity() + sizeof(ValueHeader);
    auto next_next_header = get_large_value_header(next_next_offset);
    next_next_header->set_prev_capacity(next_header->capacity());
  }

  header->set_type(BLOB_VECTOR_ACTIVE_VALUE);
  header->set_flags(header->flags() | BLOB_VECTOR_LARGE_VALUE_HAS_NEXT);
  header->set_capacity(capacity);

  register_idle_large_value(next_offset);

  if (offset == header_->rearmost_large_value_offset()) {
    header_->set_rearmost_large_value_offset(next_offset);
  }
}

void BlobVector::merge_idle_large_values(uint64_t offset) {
  typedef BlobVectorLargeValueHeader ValueHeader;

  auto header = get_large_value_header(offset);
  if (header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT) {
    const uint64_t next_offset =
        offset + header->capacity() + sizeof(ValueHeader);
    auto next_header = get_large_value_header(next_offset);
    if (next_header->type() == BLOB_VECTOR_IDLE_VALUE) {
      merge_idle_large_values(offset, next_offset);
    }
  }
  if (header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_PREV) {
    const uint64_t prev_offset =
        offset - header->prev_capacity() - sizeof(ValueHeader);
    auto prev_header = get_large_value_header(prev_offset);
    if (prev_header->type() == BLOB_VECTOR_IDLE_VALUE) {
      merge_idle_large_values(prev_offset, offset);
    }
  }
}

void BlobVector::merge_idle_large_values(uint64_t offset, uint64_t next_offset) {
  typedef BlobVectorLargeValueHeader ValueHeader;

  unregister_idle_large_value(offset);
  unregister_idle_large_value(next_offset);

  auto header = get_large_value_header(offset);
  auto next_header = get_large_value_header(next_offset);

  header->set_flags((header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_PREV) |
                    (next_header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT));
  header->set_capacity(
      header->capacity() + next_header->capacity() + sizeof(ValueHeader));

  if (next_header->flags() & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT) {
    const uint64_t next_next_offset =
        next_offset + next_header->capacity() + sizeof(ValueHeader);
    auto next_next_header = get_large_value_header(next_next_offset);
    next_next_header->set_prev_capacity(header->capacity());
  }

  register_idle_large_value(offset);

  if (next_offset == header_->rearmost_large_value_offset()) {
    header_->set_rearmost_large_value_offset(offset);
  }
}

void BlobVector::register_idle_large_value(uint64_t offset) {
  auto header = get_large_value_header(offset);
  if (header->capacity() < BLOB_VECTOR_LARGE_VALUE_LENGTH_MIN) {
    return;
  }
  const uint8_t list_id = get_list_id(header->capacity());
  const uint64_t oldest_idle_value_offset =
      header_->oldest_idle_large_value_offsets(list_id);
  if (oldest_idle_value_offset == BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
    header->set_next_offset(offset);
    header->set_prev_offset(offset);
    header_->set_oldest_idle_large_value_offsets(list_id, offset);
  } else {
    auto next_header = get_large_value_header(oldest_idle_value_offset);
    auto prev_header = get_large_value_header(next_header->prev_offset());
    header->set_next_offset(oldest_idle_value_offset);
    header->set_prev_offset(next_header->prev_offset());
    prev_header->set_next_offset(offset);
    next_header->set_prev_offset(offset);
  }
}

void BlobVector::unregister_idle_large_value(uint64_t offset) {
  auto header = get_large_value_header(offset);
  if (header->capacity() < BLOB_VECTOR_LARGE_VALUE_LENGTH_MIN) {
    return;
  }
  const uint8_t list_id = get_list_id(header->capacity());
  if (offset == header->next_offset()) {
    header_->set_oldest_idle_large_value_offsets(
        list_id, BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET);
  } else {
    auto next_header = get_large_value_header(header->next_offset());
    auto prev_header = get_large_value_header(header->prev_offset());
    next_header->set_prev_offset(header->prev_offset());
    prev_header->set_next_offset(header->next_offset());
    if (offset == header_->oldest_idle_large_value_offsets(list_id)) {
      header_->set_oldest_idle_large_value_offsets(
          list_id, header->next_offset());
    }
  }
}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorLargeValueType type) {
  switch (type) {
    case BLOB_VECTOR_ACTIVE_VALUE: {
      return builder << "BLOB_VECTOR_ACTIVE_VALUE";
    }
    case BLOB_VECTOR_FROZEN_VALUE: {
      return builder << "BLOB_VECTOR_FROZEN_VALUE";
    }
    case BLOB_VECTOR_IDLE_VALUE: {
      return builder << "BLOB_VECTOR_IDLE_VALUE";
    }
    default: {
      return builder << "n/a";
    }
 }
}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorLargeValueFlags flags) {
  if (!builder) {
    return builder;
  }

  if (flags) {
    bool is_first = true;
    if (flags & BLOB_VECTOR_LARGE_VALUE_HAS_NEXT) {
      builder << "BLOB_VECTOR_LARGE_VALUE_HAS_NEXT";
      is_first = false;
    }
    if (flags & BLOB_VECTOR_LARGE_VALUE_HAS_PREV) {
      if (!is_first) {
        builder << " | ";
      }
      builder << "BLOB_VECTOR_LARGE_VALUE_HAS_PREV";
    }
    return builder;
  } else {
    return builder << "0";
  }
}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorValueType type) {
  switch (type) {
    case BLOB_VECTOR_SMALL_VALUE: {
      return builder << "BLOB_VECTOR_SMALL_VALUE";
    }
    case BLOB_VECTOR_MEDIUM_VALUE: {
      return builder << "BLOB_VECTOR_MEDIUM_VALUE";
    }
    case BLOB_VECTOR_LARGE_VALUE: {
      return builder << "BLOB_VECTOR_LARGE_VALUE";
    }
    case BLOB_VECTOR_HUGE_VALUE: {
      return builder << "BLOB_VECTOR_HUGE_VALUE";
    }
    default: {
      return builder << "n/a";
    }
  }
}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorCellFlags flags) {
  BlobVectorCellFlags clone = flags;
  clone.flags &= BLOB_VECTOR_VALUE_TYPE_MASK;
  return builder << clone.value_type;
}

StringBuilder &BlobVectorHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ cells_block_id = " << cells_block_id_;

  builder << ", medium_value_store_block_ids = ";
  bool is_empty = true;
  for (uint32_t i = 0; i < BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM; ++i) {
    if (medium_value_store_block_ids_[i] != io::BLOCK_INVALID_ID) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << medium_value_store_block_ids_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", medium_value_store_next_offsets = ";
  is_empty = true;
  for (uint32_t i = 0; i < BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM; ++i) {
    if (medium_value_store_next_offsets_[i] != 0) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << medium_value_store_next_offsets_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", large_value_store_block_id = " << large_value_store_block_id_
          << ", rearmost_large_value_offset = " << rearmost_large_value_offset_
          << ", latest_frozen_large_value_offset = "
          << latest_frozen_large_value_offset_;

  builder << ", oldest_idle_large_value_offsets = ";
  is_empty = true;
  for (uint32_t i = 0; i < BLOB_VECTOR_LARGE_VALUE_LISTS_NUM; ++i) {
    if (oldest_idle_large_value_offsets_[i] !=
        BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET) {
      if (is_empty) {
        builder << "{ ";
        is_empty = false;
      } else {
        builder << ", ";
      }
      builder << '[' << i << "] = " << oldest_idle_large_value_offsets_[i];
    }
  }
  builder << (is_empty ? "{}" : " }");

  builder << ", inter_process_mutex = " << inter_process_mutex_
          << ", large_value_store_mutex = " << large_value_store_mutex_;
  return builder << " }";
}

StringBuilder &BlobVectorLargeValueHeader::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ type = " << type()
          << ", flags = " << flags()
          << ", capacity = " << capacity()
          << ", prev_capacity = " << prev_capacity();

  switch (type()) {
    case BLOB_VECTOR_ACTIVE_VALUE: {
      break;
    }
    case BLOB_VECTOR_FROZEN_VALUE: {
      builder << ", next_offset = " << next_offset()
              << ", frozen_stamp = " << frozen_stamp();
      break;
    }
    case BLOB_VECTOR_IDLE_VALUE: {
      builder << ", next_offset = " << next_offset()
              << ", prev_offset = " << prev_offset();
      break;
    }
  }
  return builder << " }";
}

StringBuilder &BlobVectorCell::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  builder << "{ value_type = " << value_type();
  switch (value_type()) {
    case BLOB_VECTOR_SMALL_VALUE: {
      builder << ", length = " << small_value_cell().length();
      break;
    }
    case BLOB_VECTOR_MEDIUM_VALUE: {
      builder << ", store_id = " << medium_value_cell().store_id()
              << ", capacity = " << medium_value_cell().capacity()
              << ", length = " << medium_value_cell().length()
              << ", offset = " << medium_value_cell().offset();
      break;
    }
    case BLOB_VECTOR_LARGE_VALUE: {
      builder << ", length = " << large_value_cell().length()
              << ", offset = " << large_value_cell().offset();
      break;
    }
    case BLOB_VECTOR_HUGE_VALUE: {
      builder << ", block_id = " << huge_value_cell().block_id();
      break;
    }
  }
  return builder << " }";
}

StringBuilder &BlobVector::write_to(StringBuilder &builder) const {
  if (!builder) {
    return builder;
  }

  if (!is_open()) {
    return builder << "n/a";
  }

  builder << "{ pool = " << pool_.path()
          << ", block_info = " << *block_info_
          << ", header = ";
  if (header_) {
    builder << *header_;
  } else {
    builder << "n/a";
  }
  return builder << ", inter_thread_mutex = " << inter_thread_mutex_ << " }";
}

}  // namespace db
}  // namespace grnxx
