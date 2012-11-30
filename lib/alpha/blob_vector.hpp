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

const uint64_t BLOB_VECTOR_MAX_SMALL_VALUE_LENGTH  = 7;

const uint64_t BLOB_VECTOR_MIN_MEDIUM_VALUE_LENGTH =
    BLOB_VECTOR_MAX_SMALL_VALUE_LENGTH + 1;
const uint64_t BLOB_VECTOR_MAX_MEDIUM_VALUE_LENGTH = 65535;

const uint64_t BLOB_VECTOR_MIN_LARGE_VALUE_LENGTH  =
    BLOB_VECTOR_MAX_MEDIUM_VALUE_LENGTH + 1;

const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS  = 3;
const uint64_t BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE       =
    uint64_t(1) << BLOB_VECTOR_MEDIUM_VALUE_UNIT_SIZE_BITS;

const uint8_t  BLOB_VECTOR_MEDIUM_VALUE_STORE_PAGE_SIZE_BITS            = 19;
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

extern class BlobVectorCreate {} BLOB_VECTOR_CREATE;
extern class BlobVectorOpen {} BLOB_VECTOR_OPEN;

class BlobVectorHeader {
 public:
  explicit BlobVectorHeader(uint32_t cells_block_id);

  uint32_t cells_block_id() const {
    return cells_block_id_;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }

 private:
  uint32_t cells_block_id_;
  Mutex inter_process_mutex_;
};

enum BlobVectorValueType : uint8_t {
  BLOB_VECTOR_VALUE_NULL   = 0x00,
  BLOB_VECTOR_VALUE_SMALL  = 0x10,
  BLOB_VECTOR_VALUE_MEDIUM = 0x20,  // TODO: Not implemented yet.
  BLOB_VECTOR_VALUE_LARGE  = 0x30
};

const uint8_t BLOB_VECTOR_VALUE_TYPE_MASK = 0x30;

StringBuilder &operator<<(StringBuilder &builder,
                          BlobVectorValueType value_type);

// TODO: Not implemented yet.
enum BlobVectorValueAttribute : uint8_t {
  BLOB_VECTOR_VALUE_APPENDABLE  = 0x00,
  BLOB_VECTOR_VALUE_PREPENDABLE = 0x40
};

const uint8_t BLOB_VECTOR_VALUE_ATTRIBUTE_MASK = 0x40;

StringBuilder &operator<<(StringBuilder &builder,
                          BlobVectorValueAttribute value_attribute);

const uint8_t BLOB_VECTOR_CELL_FLAGS_MASK = 0xF0;

class BlobVectorNullValueCell {
 public:
  explicit BlobVectorNullValueCell(BlobVectorValueAttribute attribute)
    : qword_(0) {
    bytes_[0] = BLOB_VECTOR_VALUE_NULL | attribute;
  }

 private:
  union {
    uint8_t bytes_[8];
    uint64_t qword_;
  };
};

class BlobVectorSmallValueCell {
 public:
  BlobVectorSmallValueCell(const void *ptr, uint64_t length,
                           BlobVectorValueAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_VALUE_SMALL | attribute |
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

class BlobVectorMediumValueCell {
 public:
  BlobVectorMediumValueCell(uint64_t offset, uint64_t length,
                            BlobVectorValueAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_VALUE_MEDIUM | attribute |
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

class BlobVectorLargeValueCell {
 public:
  BlobVectorLargeValueCell(uint32_t block_id,
                           BlobVectorValueAttribute attribute) : qword_(0) {
    bytes_[0] = BLOB_VECTOR_VALUE_LARGE | attribute;
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

  BlobVectorCell &operator=(const BlobVectorNullValueCell &rhs) {
    null_value_cell_ = rhs;
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

  BlobVectorValueType value_type() const {
    Flags flags;
    flags.byte = flags_.byte & BLOB_VECTOR_VALUE_TYPE_MASK;
    return flags.value_type;
  }
  BlobVectorValueAttribute value_attribute() const {
    Flags flags;
    flags.byte = flags_.byte & BLOB_VECTOR_VALUE_ATTRIBUTE_MASK;
    return flags.value_attribute;
  };

  const BlobVectorNullValueCell &null() const {
    return null_value_cell_;
  }
  const BlobVectorSmallValueCell &small() const {
    return small_value_cell_;
  }
  const BlobVectorMediumValueCell &medium() const {
    return medium_value_cell_;
  }
  const BlobVectorLargeValueCell &large() const {
    return large_value_cell_;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  union Flags {
    uint8_t byte;
    BlobVectorValueType value_type;
    BlobVectorValueAttribute value_attribute;
  };

  union {
    Flags flags_;
    uint64_t qword_;
    BlobVectorNullValueCell null_value_cell_;
    BlobVectorSmallValueCell small_value_cell_;
    BlobVectorMediumValueCell medium_value_cell_;
    BlobVectorLargeValueCell large_value_cell_;
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
                 uint64_t capacity, BlobVectorValueAttribute value_attribute);

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
  Mutex inter_thread_mutex_;

  BlobVectorImpl();

  void create_vector(io::Pool pool);
  void open_vector(io::Pool pool, uint32_t block_id);
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
