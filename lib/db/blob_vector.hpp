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
#ifndef GRNXX_DB_BLOB_VECTOR_HPP
#define GRNXX_DB_BLOB_VECTOR_HPP

#include <array>

#include "vector.hpp"

namespace grnxx {
namespace db {

const uint64_t BLOB_VECTOR_SMALL_VALUE_LENGTH_MAX  = 7;

const uint64_t BLOB_VECTOR_MEDIUM_VALUE_LENGTH_MIN =
    BLOB_VECTOR_SMALL_VALUE_LENGTH_MAX + 1;
const uint64_t BLOB_VECTOR_MEDIUM_VALUE_LENGTH_MAX = 64;

const uint64_t BLOB_VECTOR_LARGE_VALUE_LENGTH_MIN  =
    BLOB_VECTOR_MEDIUM_VALUE_LENGTH_MAX + 1;
const uint64_t BLOB_VECTOR_LARGE_VALUE_LENGTH_MAX  = 65535;

const uint64_t BLOB_VECTOR_HUGE_VALUE_LENGTH_MIN  =
    BLOB_VECTOR_LARGE_VALUE_LENGTH_MAX + 1;

// 8, 16, 32, and 64 bytes (4 size types).
const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM   = 4;
const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS = 3;

const uint8_t  BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS  = 4;
const uint64_t BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE       =
    uint64_t(1) << BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
const uint8_t  BLOB_VECTOR_LARGE_VALUE_STORE_SIZE_BITS = 44;
const uint64_t BLOB_VECTOR_LARGE_VALUE_STORE_SIZE      =
    uint64_t(1) << BLOB_VECTOR_LARGE_VALUE_STORE_SIZE_BITS;
const uint64_t BLOB_VECTOR_LARGE_VALUE_INVALID_OFFSET  =
    BLOB_VECTOR_LARGE_VALUE_STORE_SIZE - BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE;
const uint8_t  BLOB_VECTOR_LARGE_VALUE_LISTS_NUM       = 16;

// The settings of stores for medium values.
const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_STORE_PAGE_SIZE_BITS            = 18;
const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_STORE_TABLE_SIZE_BITS           = 12;
const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS = 16;

const uint64_t BLOB_VECTOR_MEDIUM_VALUE_STORE_PAGE_SIZE            =
    uint64_t(1) << BLOB_VECTOR_MEDIUM_VALUE_STORE_PAGE_SIZE_BITS;
const uint64_t BLOB_VECTOR_MEDIUM_VALUE_STORE_TABLE_SIZE           =
    uint64_t(1) << BLOB_VECTOR_MEDIUM_VALUE_STORE_TABLE_SIZE_BITS;
const uint64_t BLOB_VECTOR_MEDIUM_VALUE_STORE_SECONDARY_TABLE_SIZE =
    uint64_t(1) << BLOB_VECTOR_MEDIUM_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS;

typedef Vector<char, BLOB_VECTOR_MEDIUM_VALUE_STORE_PAGE_SIZE,
                     BLOB_VECTOR_MEDIUM_VALUE_STORE_TABLE_SIZE,
                     BLOB_VECTOR_MEDIUM_VALUE_STORE_SECONDARY_TABLE_SIZE>
BlobVectorMediumValueStore;

// The settings of the store for large values.
const uint8_t  BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE_BITS            = 19;
const uint8_t  BLOB_VECTOR_LARGE_VALUE_STORE_TABLE_SIZE_BITS           = 12;
const uint8_t  BLOB_VECTOR_LARGE_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS = 16;

const uint64_t BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE            =
    uint64_t(1) << BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE_BITS;
const uint64_t BLOB_VECTOR_LARGE_VALUE_STORE_TABLE_SIZE           =
    uint64_t(1) << BLOB_VECTOR_LARGE_VALUE_STORE_TABLE_SIZE_BITS;
const uint64_t BLOB_VECTOR_LARGE_VALUE_STORE_SECONDARY_TABLE_SIZE =
    uint64_t(1) << BLOB_VECTOR_LARGE_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS;

typedef Vector<char, BLOB_VECTOR_LARGE_VALUE_STORE_PAGE_SIZE,
                     BLOB_VECTOR_LARGE_VALUE_STORE_TABLE_SIZE,
                     BLOB_VECTOR_LARGE_VALUE_STORE_SECONDARY_TABLE_SIZE>
BlobVectorLargeValueStore;

class BlobVectorHeader {
 public:
  void initialize(uint32_t cells_block_id, Duration frozen_duration);

  uint32_t cells_block_id() const {
    return cells_block_id_;
  }
  Duration frozen_duration() const {
    return frozen_duration_;
  }
  uint32_t medium_value_store_block_ids(uint8_t store_id) const {
    return medium_value_store_block_ids_[store_id];
  }
  uint64_t medium_value_store_next_offsets(uint8_t store_id) const {
    return medium_value_store_next_offsets_[store_id];
  }
  uint32_t large_value_store_block_id() const {
    return large_value_store_block_id_;
  }
  uint64_t rearmost_large_value_offset() const {
    return rearmost_large_value_offset_;
  }
  uint64_t latest_frozen_large_value_offset() const {
    return latest_frozen_large_value_offset_;
  }
  uint64_t oldest_idle_large_value_offsets(uint8_t list_id) const {
    return oldest_idle_large_value_offsets_[list_id];
  }

  void set_medium_value_store_block_ids(uint8_t store_id, uint32_t value) {
    medium_value_store_block_ids_[store_id] = value;
  }
  void set_medium_value_store_next_offsets(uint8_t store_id, uint64_t value) {
    medium_value_store_next_offsets_[store_id] = value;
  }
  void set_large_value_store_block_id(uint32_t value) {
    large_value_store_block_id_ = value;
  }
  void set_rearmost_large_value_offset(uint64_t value) {
    rearmost_large_value_offset_ = value;
  }
  void set_latest_frozen_large_value_offset(uint64_t value) {
    latest_frozen_large_value_offset_ = value;
  }
  void set_oldest_idle_large_value_offsets(uint8_t list_id, uint64_t value) {
    oldest_idle_large_value_offsets_[list_id] = value;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }
  Mutex *mutable_medium_value_store_mutex() {
    return &medium_value_store_mutex_;
  }
  Mutex *mutable_large_value_store_mutex() {
    return &large_value_store_mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint32_t cells_block_id_;
  Duration frozen_duration_;
  uint32_t medium_value_store_block_ids_[BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM];
  uint64_t medium_value_store_next_offsets_[BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM];
  uint32_t large_value_store_block_id_;
  uint64_t rearmost_large_value_offset_;
  uint64_t latest_frozen_large_value_offset_;
  uint64_t oldest_idle_large_value_offsets_[BLOB_VECTOR_LARGE_VALUE_LISTS_NUM];
  Mutex inter_process_mutex_;
  Mutex medium_value_store_mutex_;
  Mutex large_value_store_mutex_;
};

enum BlobVectorLargeValueType : uint8_t {
  BLOB_VECTOR_ACTIVE_VALUE  = 0x00,
  BLOB_VECTOR_FROZEN_VALUE  = 0x01,
  BLOB_VECTOR_IDLE_VALUE    = 0x02
};

class BlobVectorLargeValueFlagsIdentifier;
typedef FlagsImpl<BlobVectorLargeValueFlagsIdentifier, uint8_t>
    BlobVectorLargeValueFlags;

const BlobVectorLargeValueFlags BLOB_VECTOR_LARGE_VALUE_HAS_NEXT =
    BlobVectorLargeValueFlags::define(0x01);
const BlobVectorLargeValueFlags BLOB_VECTOR_LARGE_VALUE_HAS_PREV =
    BlobVectorLargeValueFlags::define(0x02);

class BlobVectorLargeValueHeader {
 public:
  void initialize(BlobVectorLargeValueType type,
                  BlobVectorLargeValueFlags flags,
                  uint64_t capacity,
                  uint64_t prev_capacity) {
    set_type(type);
    set_flags(flags);
    set_capacity(capacity);
    set_prev_capacity(prev_capacity);
    next_offset_high_ = 0;
    prev_offset_high_ = 0;
    next_offset_low_ = 0;
    prev_offset_low_ = 0;
  }

  void *value() {
    return this + 1;
  }

  BlobVectorLargeValueType type() const {
    return type_;
  }
  BlobVectorLargeValueFlags flags() const {
    return flags_;
  }
  uint64_t capacity() const {
    return static_cast<uint64_t>(capacity_)
         << BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
  }
  uint64_t prev_capacity() const {
    return static_cast<uint64_t>(prev_capacity_)
         << BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
  }
  uint64_t next_offset() const {
    return ((static_cast<uint64_t>(next_offset_high_) << 32) |
            next_offset_low_) << BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
  }
  uint64_t prev_offset() const {
    return ((static_cast<uint64_t>(prev_offset_high_) << 32) |
            prev_offset_low_) << BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
  }
  uint16_t frozen_stamp() const {
    return static_cast<uint16_t>(frozen_stamp_);
  }

  void set_type(BlobVectorLargeValueType value) {
    type_ = value;
  }
  void set_flags(BlobVectorLargeValueFlags value) {
    flags_ = value;
  }
  void set_capacity(uint64_t value) {
    capacity_ = static_cast<uint16_t>(
        value >> BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS);
  }
  void set_prev_capacity(uint64_t value) {
    prev_capacity_ = static_cast<uint16_t>(
        value >> BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS);
  }
  void set_next_offset(uint64_t value) {
    value >>= BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
    next_offset_high_ = static_cast<uint8_t>(value >> 32);
    next_offset_low_ = static_cast<uint32_t>(value);
  }
  void set_prev_offset(uint64_t value) {
    value >>= BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS;
    prev_offset_high_ = static_cast<uint8_t>(value >> 32);
    prev_offset_low_ = static_cast<uint32_t>(value);
  }
  void set_frozen_stamp(uint16_t value) {
    frozen_stamp_ = value;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  BlobVectorLargeValueType type_;
  BlobVectorLargeValueFlags flags_;
  uint16_t capacity_;
  uint16_t prev_capacity_;
  uint8_t next_offset_high_;
  uint8_t prev_offset_high_;
  uint32_t next_offset_low_;
  union {
    uint32_t frozen_stamp_;
    uint32_t prev_offset_low_;
  };
};

enum BlobVectorValueType : uint8_t {
  BLOB_VECTOR_SMALL_VALUE  = 0x00,
  BLOB_VECTOR_MEDIUM_VALUE = 0x01,
  BLOB_VECTOR_LARGE_VALUE  = 0x02,
  BLOB_VECTOR_HUGE_VALUE   = 0x03
};

const uint8_t BLOB_VECTOR_VALUE_TYPE_MASK = 0x03;

union BlobVectorCellFlags {
  uint8_t flags;
  BlobVectorValueType value_type;
};

class BlobVectorSmallValueCell {
 public:
  BlobVectorSmallValueCell(const void *ptr, uint64_t length)
    : flags_and_length_(BLOB_VECTOR_SMALL_VALUE |
                        (static_cast<uint8_t>(length) << 5)),
      value_() {
    std::memcpy(value_, ptr, length);
  }

  uint64_t length() const {
    return flags_and_length_ >> 5;
  }
  const void *value() const {
    return value_;
  }

 private:
  uint8_t flags_and_length_;
  uint8_t value_[7];
};

class BlobVectorMediumValueCell {
 public:
  BlobVectorMediumValueCell(uint8_t store_id, uint64_t offset, uint64_t length)
    : flags_(BLOB_VECTOR_MEDIUM_VALUE),
      store_id_(store_id),
      length_(static_cast<uint8_t>(length)),
      offset_high_(static_cast<uint8_t>(
          offset >> (32 + store_id + BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS))),
      offset_low_(static_cast<uint32_t>(
          offset >> (store_id + BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS))) {}

  uint8_t store_id() const {
    return store_id_;
  }
  uint64_t capacity() const {
    return uint64_t(8) << store_id_;
  }
  uint64_t length() const {
    return length_;
  }
  uint64_t offset() const {
    return ((static_cast<uint64_t>(offset_high_) << 32) | offset_low_)
        << (store_id_ + BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS);
  }

 private:
  uint8_t flags_;
  uint8_t store_id_;
  uint8_t length_;
  uint8_t offset_high_;
  uint32_t offset_low_;
};

class BlobVectorLargeValueCell {
 public:
  BlobVectorLargeValueCell(uint64_t offset, uint64_t length)
    : flags_(BLOB_VECTOR_LARGE_VALUE),
      offset_high_(static_cast<uint8_t>(offset >> 32)),
      length_(static_cast<uint16_t>(length)),
      offset_low_(static_cast<uint32_t>(offset)) {}

  uint64_t length() const {
    return length_;
  }
  uint64_t offset() const {
    return (static_cast<uint64_t>(offset_high_) << 32) | offset_low_;
  }

 private:
  uint8_t flags_;
  uint8_t offset_high_;
  uint16_t length_;
  uint32_t offset_low_;
};

class BlobVectorHugeValueCell {
 public:
  explicit BlobVectorHugeValueCell(uint32_t block_id)
    : flags_(BLOB_VECTOR_HUGE_VALUE), reserved_(), block_id_(block_id) {
    std::memset(reserved_, 0, sizeof(reserved_));
  }

  uint32_t block_id() const {
    return block_id_;
  }

 private:
  uint8_t flags_;
  uint8_t reserved_[3];
  uint32_t block_id_;
};

class BlobVectorCell {
 public:
  BlobVectorCell() : cell_(0) {}
  ~BlobVectorCell() {}

  BlobVectorCell(const BlobVectorCell &rhs) : cell_(rhs.cell_) {}
  BlobVectorCell &operator=(const BlobVectorCell &rhs) {
    cell_ = rhs.cell_;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorSmallValueCell &rhs) {
    small_value_cell_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorMediumValueCell &rhs) {
    medium_value_cell_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorLargeValueCell &rhs) {
    large_value_cell_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorHugeValueCell &rhs) {
    huge_value_cell_ = rhs;
    return *this;
  }

  BlobVectorValueType value_type() const {
    BlobVectorCellFlags clone = flags_;
    clone.flags &= BLOB_VECTOR_VALUE_TYPE_MASK;
    return clone.value_type;
  }

  const BlobVectorSmallValueCell &small_value_cell() const {
    return small_value_cell_;
  }
  const BlobVectorMediumValueCell &medium_value_cell() const {
    return medium_value_cell_;
  }
  const BlobVectorLargeValueCell &large_value_cell() const {
    return large_value_cell_;
  }
  const BlobVectorHugeValueCell &huge_value_cell() const {
    return huge_value_cell_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  union {
    uint64_t cell_;
    BlobVectorCellFlags flags_;
    BlobVectorSmallValueCell small_value_cell_;
    BlobVectorMediumValueCell medium_value_cell_;
    BlobVectorLargeValueCell large_value_cell_;
    BlobVectorHugeValueCell huge_value_cell_;
  };
};

class BlobVector {
 public:
  BlobVector();
  ~BlobVector();

  BlobVector(BlobVector &&rhs);
  BlobVector &operator=(BlobVector &&rhs);

  bool is_open() const {
    return static_cast<bool>(pool_);
  }

  void create(io::Pool pool);
  void open(io::Pool pool, uint32_t block_id);
  void close();

  const void *get_value_address(uint64_t id, uint64_t *length = nullptr);
  void set_value(uint64_t id, const void *ptr, uint64_t length);

  // TODO
//  void append(uint64_t id, const void *ptr, uint64_t length);
//  void prepend(uint64_t id, const void *ptr, uint64_t length);

  uint32_t block_id() const {
    return is_open() ? block_info_->id() : io::BLOCK_INVALID_ID;
  }
  uint64_t id_max() const {
    return cells_.id_max();
  }

  void swap(BlobVector &rhs);

  StringBuilder &write_to(StringBuilder &builder) const;

  static void unlink(io::Pool pool, uint32_t block_id);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  BlobVectorHeader *header_;
  Recycler *recycler_;
  Vector<BlobVectorCell> cells_;
  std::array<BlobVectorMediumValueStore,
             BLOB_VECTOR_MEDIUM_VALUE_STORES_NUM> medium_value_stores_;
  BlobVectorLargeValueStore large_value_store_;
  Mutex inter_thread_mutex_;

  void create_vector(io::Pool pool);
  void open_vector(io::Pool pool, uint32_t block_id);

  BlobVectorSmallValueCell create_small_value_cell(
      const void *ptr, uint64_t length);
  BlobVectorMediumValueCell create_medium_value_cell(
      const void *ptr, uint64_t length);
  BlobVectorLargeValueCell create_large_value_cell(
      const void *ptr, uint64_t length);
  BlobVectorHugeValueCell create_huge_value_cell(
      const void *ptr, uint64_t length);

  void free_value(BlobVectorCell cell);

  void open_medium_value_store(uint8_t store_id);
  void open_large_value_store();

  void unfreeze_frozen_large_values();

  void divide_idle_large_value(uint64_t offset, uint64_t capacity);
  void merge_idle_large_values(uint64_t offset);
  void merge_idle_large_values(uint64_t offset, uint64_t next_offset);

  void register_idle_large_value(uint64_t offset);
  void unregister_idle_large_value(uint64_t offset);

  uint8_t get_store_id(uint64_t capacity) {
    return bit_scan_reverse(capacity - 1)
        - (BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS - 1);
  }
  uint8_t get_list_id(uint64_t capacity) {
    return bit_scan_reverse(
        (capacity >> BLOB_VECTOR_LARGE_VALUE_UNIT_SIZE_BITS) | 1);
  }

  BlobVectorLargeValueHeader *get_large_value_header(uint64_t offset) {
    return reinterpret_cast<BlobVectorLargeValueHeader *>(
        &large_value_store_[offset]);
  }

  BlobVector(const BlobVector &);
  BlobVector &operator=(const BlobVector &);
};

inline void swap(BlobVector &lhs, BlobVector &rhs) {
  lhs.swap(rhs);
}

StringBuilder &operator<<(StringBuilder &builder, BlobVectorLargeValueType type);
StringBuilder &operator<<(StringBuilder &builder, BlobVectorLargeValueFlags flags);
StringBuilder &operator<<(StringBuilder &builder, BlobVectorValueType type);
StringBuilder &operator<<(StringBuilder &builder, BlobVectorCellFlags flags);

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorHeader &header) {
  return header.write_to(builder);
}
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorLargeValueHeader &header) {
  return header.write_to(builder);
}
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorCell &cell) {
  return cell.write_to(builder);
}
inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVector &vector) {
  return vector.write_to(builder);
}

}  // namespace db
}  // namespace grnxx

#endif  // GRNXX_DB_BLOB_VECTOR_HPP
