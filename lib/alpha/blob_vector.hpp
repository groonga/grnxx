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

#include "vector.hpp"

namespace grnxx {
namespace alpha {

const uint64_t BLOB_VECTOR_MAX_ID = uint64_t(1) << 40;

const uint64_t BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH  = 7;

const uint64_t BLOB_VECTOR_MEDIUM_VALUE_MIN_LENGTH =
    BLOB_VECTOR_SMALL_VALUE_MAX_LENGTH + 1;
const uint64_t BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH = 65535;

const uint64_t BLOB_VECTOR_LARGE_VALUE_MIN_LENGTH  =
    BLOB_VECTOR_MEDIUM_VALUE_MAX_LENGTH + 1;

const uint8_t  BLOB_VECTOR_UNIT_SIZE_BITS  = 3;
const uint64_t BLOB_VECTOR_UNIT_SIZE       =
    uint64_t(1) << BLOB_VECTOR_UNIT_SIZE_BITS;

const uint8_t  BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS            = 19;
const uint8_t  BLOB_VECTOR_VALUE_STORE_TABLE_SIZE_BITS           = 12;
const uint8_t  BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS = 16;

const uint64_t BLOB_VECTOR_VALUE_STORE_PAGE_SIZE            =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_PAGE_SIZE_BITS;
const uint64_t BLOB_VECTOR_VALUE_STORE_TABLE_SIZE           =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_TABLE_SIZE_BITS;
const uint64_t BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE =
    uint64_t(1) << BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE_BITS;

typedef Vector<char, BLOB_VECTOR_VALUE_STORE_PAGE_SIZE,
                     BLOB_VECTOR_VALUE_STORE_TABLE_SIZE,
                     BLOB_VECTOR_VALUE_STORE_SECONDARY_TABLE_SIZE>
BlobVectorValueStore;

extern class BlobVectorCreate {} BLOB_VECTOR_CREATE;
extern class BlobVectorOpen {} BLOB_VECTOR_OPEN;

class BlobVectorHeader {
 public:
  explicit BlobVectorHeader(uint32_t cells_block_id);

  uint32_t cells_block_id() const {
    return cells_block_id_;
  }
  uint32_t value_store_block_id() const {
    return value_store_block_id_;
  }
  uint64_t next_medium_value_offset() const {
    return next_medium_value_offset_;
  }
  uint32_t latest_large_value_block_id() const {
    return latest_large_value_block_id_;
  }

  void set_value_store_block_id(uint32_t value) {
    value_store_block_id_ = value;
  }
  void set_next_medium_value_offset(uint64_t value) {
    next_medium_value_offset_ = value;
  }
  void set_latest_large_value_block_id(uint32_t value) {
    latest_large_value_block_id_ = value;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  uint32_t cells_block_id_;
  uint32_t value_store_block_id_;
  uint64_t next_medium_value_offset_;
  uint32_t latest_large_value_block_id_;
  Mutex inter_process_mutex_;
};

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const BlobVectorHeader &header) {
  return header.write_to(builder);
}

enum BlobVectorType : uint8_t {
  BLOB_VECTOR_NULL   = 0x00,
  BLOB_VECTOR_SMALL  = 0x10,
  BLOB_VECTOR_MEDIUM = 0x20,
  BLOB_VECTOR_LARGE  = 0x30
};

const uint8_t BLOB_VECTOR_TYPE_MASK = 0x30;

StringBuilder &operator<<(StringBuilder &builder, BlobVectorType type);

class BlobVectorMediumValueHeader {
 public:
  uint64_t value_id() const {
    return dwords_[0] | (static_cast<uint64_t>(bytes_[4]) << 32);
  }
  uint64_t capacity() const {
    return static_cast<uint64_t>(words_[3]) << BLOB_VECTOR_UNIT_SIZE_BITS;
  }

  void set_value_id(uint64_t value) {
    dwords_[0] = static_cast<uint32_t>(value);
    bytes_[4] = static_cast<uint8_t>(value >> 32);
  }
  void set_capacity(uint64_t value) {
    words_[3] = static_cast<uint16_t>(value >> BLOB_VECTOR_UNIT_SIZE_BITS);
  }

 private:
  union {
    uint8_t bytes_[8];
    uint16_t words_[4];
    uint32_t dwords_[2];
  };
};

class BlobVectorLargeValueHeader {
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

// TODO: Not implemented yet.
enum BlobVectorAttribute : uint8_t {
  BLOB_VECTOR_APPENDABLE  = 0x00,
  BLOB_VECTOR_PREPENDABLE = 0x40
};

const uint8_t BLOB_VECTOR_ATTRIBUTE_MASK = 0x40;

StringBuilder &operator<<(StringBuilder &builder,
                          BlobVectorAttribute attribute);

const uint8_t BLOB_VECTOR_CELL_FLAGS_MASK = 0xF0;

class BlobVectorNullValue {
 public:
  explicit BlobVectorNullValue(BlobVectorAttribute attribute)
    : qword_(0) {
    bytes_[0] = BLOB_VECTOR_NULL | attribute;
  }

 private:
  union {
    uint8_t bytes_[8];
    uint64_t qword_;
  };
};

class BlobVectorSmallValue {
 public:
  BlobVectorSmallValue(const void *ptr, uint64_t length,
                       BlobVectorAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_SMALL | attribute |
                static_cast<uint8_t>(length);
    std::memcpy(&bytes_[1], ptr, length);
  }

  uint64_t length() const {
    return bytes_[0] & ~BLOB_VECTOR_CELL_FLAGS_MASK;
  }
  const void *value() const {
    return &bytes_[1];
  }

 private:
  union {
    uint8_t bytes_[8];
    uint64_t qword_;
  };
};

class BlobVectorMediumValue {
 public:
  BlobVectorMediumValue(uint64_t offset, uint64_t length,
                        BlobVectorAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_MEDIUM | attribute |
                static_cast<uint8_t>(offset >> 40);
    bytes_[1] = static_cast<uint8_t>(offset >> 32);
    words_[1] = static_cast<uint16_t>(length);
    dwords_[1] = static_cast<uint32_t>(offset);
  }

  uint64_t length() const {
    return words_[1];
  }
  uint64_t offset() const {
    return (static_cast<uint64_t>(bytes_[0] &
                                  ~BLOB_VECTOR_CELL_FLAGS_MASK) << 40) |
           (static_cast<uint64_t>(bytes_[1]) << 32) | dwords_[1];
  }

 private:
  union {
    uint8_t bytes_[8];
    uint16_t words_[4];
    uint32_t dwords_[2];
    uint64_t qword_;
  };
};

class BlobVectorLargeValue {
 public:
  BlobVectorLargeValue(uint32_t block_id,
                       BlobVectorAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_LARGE | attribute;
    dwords_[1] = block_id;
  }

  uint32_t block_id() const {
    return dwords_[1];
  }

 private:
  union {
    uint8_t bytes_[8];
    uint32_t dwords_[2];
    uint64_t qword_;
  };
};

class BlobVectorCell {
 public:
  BlobVectorCell() : qword_(0) {}

  BlobVectorCell &operator=(const BlobVectorNullValue &rhs) {
    null_value_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorSmallValue &rhs) {
    small_value_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorMediumValue &rhs) {
    medium_value_ = rhs;
    return *this;
  }
  BlobVectorCell &operator=(const BlobVectorLargeValue &rhs) {
    large_value_ = rhs;
    return *this;
  }

  BlobVectorType type() const {
    Flags flags;
    flags.byte = flags_.byte & BLOB_VECTOR_TYPE_MASK;
    return flags.type;
  }
  BlobVectorAttribute attribute() const {
    Flags flags;
    flags.byte = flags_.byte & BLOB_VECTOR_ATTRIBUTE_MASK;
    return flags.attribute;
  };

  const BlobVectorNullValue &null() const {
    return null_value_;
  }
  const BlobVectorSmallValue &small() const {
    return small_value_;
  }
  const BlobVectorMediumValue &medium() const {
    return medium_value_;
  }
  const BlobVectorLargeValue &large() const {
    return large_value_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  union Flags {
    uint8_t byte;
    BlobVectorType type;
    BlobVectorAttribute attribute;
  };

  union {
    Flags flags_;
    uint64_t qword_;
    BlobVectorNullValue null_value_;
    BlobVectorSmallValue small_value_;
    BlobVectorMediumValue medium_value_;
    BlobVectorLargeValue large_value_;
  };
};

static_assert(sizeof(BlobVectorCell) == sizeof(uint64_t),
              "sizeof(BlobVectorCell) != sizeof(uint64_t)");

class BlobVectorImpl {
 public:
  static std::unique_ptr<BlobVectorImpl> create(io::Pool pool);
  static std::unique_ptr<BlobVectorImpl> open(io::Pool pool,
                                              uint32_t block_id);

  const void *get_value(uint64_t id, uint64_t *length);

  void set_value(uint64_t id, const void *ptr, uint64_t length,
                 uint64_t capacity = 0,
                 BlobVectorAttribute attribute = BLOB_VECTOR_APPENDABLE);

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
  Vector<BlobVectorCell> cells_;
  BlobVectorValueStore value_store_;
  Mutex inter_thread_mutex_;

  BlobVectorImpl();

  void create_vector(io::Pool pool);
  void open_vector(io::Pool pool, uint32_t block_id);

  BlobVectorMediumValue create_medium_value(
      uint32_t id, const void *ptr, uint64_t length, uint64_t capacity,
      BlobVectorAttribute attribute);
  BlobVectorLargeValue create_large_value(
      uint32_t id, const void *ptr, uint64_t length, uint64_t capacity,
      BlobVectorAttribute attribute);

  void free_value(BlobVectorCell cell);

  void register_large_value(uint32_t block_id,
                            BlobVectorLargeValueHeader *value_header);
  void unregister_large_value(uint32_t block_id,
                              BlobVectorLargeValueHeader *value_header);

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
  BlobVector() : impl_() {}
  BlobVector(const BlobVectorCreate &, io::Pool pool);
  BlobVector(const BlobVectorOpen &, io::Pool pool, uint32_t block_id);
  ~BlobVector() {}

  BlobVector(const BlobVector &vector) : impl_(vector.impl_) {}
  BlobVector &operator=(const BlobVector &vector) {
    impl_ = vector.impl_;
    return *this;
  }

  BlobVector(BlobVector &&vector) : impl_(std::move(vector.impl_)) {}
  BlobVector &operator=(BlobVector &&vector) {
    impl_ = std::move(vector.impl_);
    return *this;
  }

  GRNXX_EXPLICIT_CONVERSION operator bool() const {
    return static_cast<bool>(impl_);
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

  static GRNXX_CONSTEXPR uint64_t max_id() {
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
