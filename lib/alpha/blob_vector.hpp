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
#ifndef GRNXX_ALPHA_BLOB_VECTOR_HPP
#define GRNXX_ALPHA_BLOB_VECTOR_HPP

#include "../db/vector.hpp"

namespace grnxx {
namespace alpha {

using namespace grnxx::db;

constexpr uint64_t BLOB_VECTOR_MAX_ID = uint64_t(1) << 40;

constexpr uint32_t BLOB_VECTOR_INVALID_PAGE_ID =
    std::numeric_limits<uint32_t>::max();

constexpr uint64_t BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH  = 7;

constexpr uint64_t BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH =
    BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH + 1;
constexpr uint64_t BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH = 65535;

constexpr uint64_t BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH  =
    BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH + 1;

constexpr uint8_t  BLOB_VECTOR_UNIT_SIZE_BITS  = 3;
constexpr uint64_t BLOB_VECTOR_UNIT_SIZE       =
    uint64_t(1) << BLOB_VECTOR_UNIT_SIZE_BITS;

constexpr uint8_t  BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS            = 19;
constexpr uint8_t  BLOB_VECTOR_VALUE_STORE_TABLE_SIZE_BITS           = 12;
constexpr uint8_t  BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS = 16;

constexpr uint64_t BLOB_VECTOR_VALUE_STORE_PAGE_SIZE            =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS;
constexpr uint64_t BLOB_VECTOR_VALUE_STORE_TABLE_SIZE           =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_TABLE_SIZE_BITS;
constexpr uint64_t BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS;

extern class BlobVectorCreate {} BLOB_VECTOR_CREATE;
extern class BlobVectorOpen {} BLOB_VECTOR_OPEN;

class BlobVectorHeader {
 public:
  explicit BlobVectorHeader(uint32_t table_block_id);

  uint32_t table_block_id() const {
    return table_block_id_;
  }
  uint32_t value_store_block_id() const {
    return value_store_block_id_;
  }
  uint32_t index_store_block_id() const {
    return index_store_block_id_;
  }
  uint32_t next_page_id() const {
    return next_page_id_;
  }
  uint64_t next_value_offset() const {
    return next_value_offset_;
  }
  uint32_t latest_frozen_page_id() const {
    return latest_frozen_page_id_;
  }
  uint32_t latest_large_value_block_id() const {
    return latest_large_value_block_id_;
  }

  void set_value_store_block_id(uint32_t value) {
    value_store_block_id_ = value;
  }
  void set_index_store_block_id(uint32_t value) {
    index_store_block_id_ = value;
  }
  void set_next_page_id(uint32_t value) {
    next_page_id_ = value;
  }
  void set_next_value_offset(uint64_t value) {
    next_value_offset_ = value;
  }
  void set_latest_frozen_page_id(uint32_t value) {
    latest_frozen_page_id_ = value;
  }
  void set_latest_large_value_block_id(uint32_t value) {
    latest_large_value_block_id_ = value;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint32_t table_block_id_;
  uint32_t value_store_block_id_;
  uint32_t index_store_block_id_;
  uint32_t next_page_id_;
  uint64_t next_value_offset_;
  uint32_t latest_frozen_page_id_;
  uint32_t latest_large_value_block_id_;
  Mutex inter_process_mutex_;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorHeader &header) {
  return header.write_to(builder);
}

enum BlobVectorValueType : uint8_t {
  BLOB_VECTOR_NULL   = 0x00,
  BLOB_VECTOR_SMALL  = 0x10,
  BLOB_VECTOR_MEDIUM = 0x20,
  BLOB_VECTOR_LARGE  = 0x30
};

constexpr uint8_t BLOB_VECTOR_TYPE_MASK = 0x30;

class BlobVectorPageInfo {
 public:
  BlobVectorPageInfo()
    : next_page_id_(BLOB_VECTOR_INVALID_PAGE_ID), stamp_(0), reserved_(0) {}

  uint32_t next_page_id() const {
    return next_page_id_;
  }
  uint32_t num_values() const {
    return num_values_;
  }
  uint16_t stamp() const {
    return reserved_;
  }

  void set_next_page_id(uint32_t value) {
    next_page_id_ = value;
  }
  void set_num_values(uint32_t value) {
    num_values_ = value;
  }
  void set_stamp(uint16_t value) {
    stamp_ = value;
  }

 private:
  union {
    uint32_t next_page_id_;
    uint32_t num_values_;
  };
  uint16_t stamp_;
  uint16_t reserved_;
};

class BlobVectorValueHeader {
 public:
  uint64_t length() const {
    return length_;
  }
  uint32_t next_value_block_id() const {
    return next_value_block_id_;
  }
  uint32_t prev_value_block_id() const {
    return prev_value_block_id_;
  }

  void set_length(uint64_t value) {
    length_ = value;
  }
  void set_next_value_block_id(uint32_t value) {
    next_value_block_id_ = value;
  }
  void set_prev_value_block_id(uint32_t value) {
    prev_value_block_id_ = value;
  }

 private:
  uint64_t length_;
  uint32_t next_value_block_id_;
  uint32_t prev_value_block_id_;
};

const uint8_t BLOB_VECTOR_CELL_FLAGS_MASK = 0xF0;

class BlobVectorCell {
 public:
  constexpr BlobVectorCell() : qword_(0) {}

  static constexpr BlobVectorCell null_value_cell() {
    return BlobVectorCell();
  }
  static BlobVectorCell small_value_cell(const void *ptr, uint64_t length) {
    BlobVectorCell cell;
    cell.bytes_[0] = BLOB_VECTOR_SMALL | static_cast<uint8_t>(length);
    std::memcpy(&cell.bytes_[1], ptr, length);
    return cell;
  }
  static BlobVectorCell medium_value_cell(uint64_t offset, uint64_t length) {
    BlobVectorCell cell;
    cell.bytes_[0] = BLOB_VECTOR_MEDIUM | static_cast<uint8_t>(offset >> 40);
    cell.bytes_[1] = static_cast<uint8_t>(offset >> 32);
    cell.words_[1] = static_cast<uint16_t>(length);
    cell.dwords_[1] = static_cast<uint32_t>(offset);
    return cell;
  }
  static BlobVectorCell large_value_cell(uint32_t block_id) {
    BlobVectorCell cell;
    cell.bytes_[0] = BLOB_VECTOR_LARGE;
    cell.dwords_[1] = block_id;
    return cell;
  }

  BlobVectorValueType type() const {
    Flags flags;
    flags.byte = flags_.byte & BLOB_VECTOR_TYPE_MASK;
    return flags.type;
  }

  // Accessors to small values.
  uint64_t small_length() const {
    return bytes_[0] & ~BLOB_VECTOR_CELL_FLAGS_MASK;
  }
  const void *value() const {
    return &bytes_[1];
  }

  // Accessors to medium values.
  uint64_t medium_length() const {
    return words_[1];
  }
  uint64_t offset() const {
    return (static_cast<uint64_t>(bytes_[0] &
                                  ~BLOB_VECTOR_CELL_FLAGS_MASK) << 40) |
           (static_cast<uint64_t>(bytes_[1]) << 32) | dwords_[1];
  }

  // Accessors to large values.
  uint32_t block_id() const {
    return dwords_[1];
  }

 private:
  union Flags {
    uint8_t byte;
    BlobVectorValueType type;
  };

  union {
    Flags flags_;
    uint8_t bytes_[8];
    uint16_t words_[4];
    uint32_t dwords_[2];
    uint64_t qword_;
  };
};

static_assert(sizeof(BlobVectorCell) == sizeof(uint64_t),
              "sizeof(BlobVectorCell) != sizeof(uint64_t)");

typedef Vector<BlobVectorCell> BlobVectorTable;

typedef Vector<char, BLOB_VECTOR_VALUE_STORE_PAGE_SIZE,
                     BLOB_VECTOR_VALUE_STORE_TABLE_SIZE,
                     BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE>
BlobVectorValueStore;

typedef Vector<BlobVectorPageInfo,
               BLOB_VECTOR_VALUE_STORE_TABLE_SIZE,
               BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE>
BlobVectorIndexStore;

class BlobVectorImpl {
 public:
  static std::unique_ptr<BlobVectorImpl> create(io::Pool pool);
  static std::unique_ptr<BlobVectorImpl> open(io::Pool pool,
                                              uint32_t block_id);

  const void *get_value(uint64_t id, uint64_t *length);
  void set_value(uint64_t id, const void *ptr, uint64_t length);

  // TODO

  uint32_t block_id() const {
    return block_info_->id();
  }

  StringBuilder &write_to(StringBuilder &builder) const;

  static void unlink(io::Pool pool, uint32_t block_id);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  BlobVectorHeader *header_;
  Recycler *recycler_;
  BlobVectorTable table_;
  BlobVectorValueStore value_store_;
  BlobVectorIndexStore index_store_;
  Mutex inter_thread_mutex_;

  BlobVectorImpl();

  void create_vector(io::Pool pool);
  void open_vector(io::Pool pool, uint32_t block_id);

  BlobVectorCell create_medium_value(const void *ptr, uint64_t length);
  BlobVectorCell create_large_value(const void *ptr, uint64_t length);

  void free_value(BlobVectorCell cell);

  void register_large_value(uint32_t block_id,
                            BlobVectorValueHeader *value_header);
  void unregister_large_value(uint32_t block_id,
                              BlobVectorValueHeader *value_header);

  void freeze_page(uint32_t page_id);
  void unfreeze_oldest_frozen_page();

  Mutex *mutable_inter_thread_mutex() {
    return &inter_thread_mutex_;
  }
  Mutex *mutable_inter_process_mutex() {
    return header_->mutable_inter_process_mutex();
  }
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorImpl &vector) {
  return vector.write_to(builder);
}

class BlobVector {
 public:
  BlobVector() = default;
  BlobVector(const BlobVectorCreate &, io::Pool pool);
  BlobVector(const BlobVectorOpen &, io::Pool pool, uint32_t block_id);

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  void create(io::Pool pool) {
    *this = BlobVector(BLOB_VECTOR_CREATE, pool);
  }
  void open(io::Pool pool, uint32_t block_id) {
    *this = BlobVector(BLOB_VECTOR_OPEN, pool, block_id);
  }
  void close() {
    *this = BlobVector();
  }

  // TODO
//  BlobVectorValueRef operator[](uint64_t id);

  const void *get_value(uint64_t id, uint64_t *length = nullptr) {
    return impl_->get_value(id, length);
  }
  void set_value(uint64_t id, const void *ptr, uint64_t length) {
    impl_->set_value(id, ptr, length);
  }

  uint32_t block_id() const {
    return impl_->block_id();
  }

  void swap(BlobVector &rhs) {
    impl_.swap(rhs.impl_);
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return impl_ ? impl_->write_to(builder) : (builder << "n/a");
  }

  static constexpr uint64_t max_id() {
    return BLOB_VECTOR_MAX_ID;
  }

  static void unlink(io::Pool pool, uint32_t block_id) {
    BlobVectorImpl::unlink(pool, block_id);
  }

 private:
  std::shared_ptr<BlobVectorImpl> impl_;
};

inline void swap(BlobVector &lhs, BlobVector &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVector &vector) {
  return vector.write_to(builder);
}

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_BLOB_VECTOR_HPP
