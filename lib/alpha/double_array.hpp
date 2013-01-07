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
#ifndef GRNXX_ALPHA_DOUBLE_ARRAY_HPP
#define GRNXX_ALPHA_DOUBLE_ARRAY_HPP

#include "../db/vector.hpp"

namespace grnxx {
namespace alpha {

using namespace grnxx::db;

extern struct DoubleArrayCreate {} DOUBLE_ARRAY_CREATE;
extern struct DoubleArrayOpen {} DOUBLE_ARRAY_OPEN;

class DoubleArrayNode {
 public:
  // The ID of this node is used as an offset (true) or not (false).
  bool is_origin() const {
    // 1 bit.
    return qword_ & IS_ORIGIN_FLAG;
  }
  // This node is valid (false) or not (true).
  bool is_phantom() const {
    // 1 bit.
    return qword_ & IS_PHANTOM_FLAG;
  }
  // This node is associated with a key (true) or not (false).
  bool is_leaf() const {
    // 1 bit.
    return qword_ & IS_LEAF_FLAG;
  }
  // A child of this node is a leaf node (true) or not (false).
  bool is_terminal() const {
    // 1 bit.
    return qword_ & IS_TERMINAL_FLAG;
  }

  void set_is_origin(bool value) {
    if (value) {
      qword_ &= ~IS_ORIGIN_FLAG;
    } else {
      qword_ |= IS_ORIGIN_FLAG;
    }
  }
  void set_is_phantom(bool value) {
    if (value) {
      qword_ &= ~IS_PHANTOM_FLAG;
    } else {
      qword_ |= IS_PHANTOM_FLAG;
    }
  }
  void set_is_leaf(bool value) {
    if (value) {
      qword_ &= ~IS_LEAF_FLAG;
    } else {
      qword_ |= IS_LEAF_FLAG;
    }
  }
  void set_is_terminal(bool value) {
    if (value) {
      qword_ &= ~IS_TERMINAL_FLAG;
    } else {
      qword_ |= IS_TERMINAL_FLAG;
    }
  }

  // Phantom nodes are doubly linked in each block.
  // Each block consists of 512 nodes.
  uint16_t next() const {
    // 9 bits.
    return static_cast<uint16_t>((qword_ >> NEXT_SHIFT) & NEXT_MASK);
  }
  uint16_t prev() const {
    // 9 bits.
    return static_cast<uint16_t>((qword_ >> PREV_SHIFT) & PREV_MASK);
  }

  void set_next(uint16_t value) {
    qword_ = (qword_ & ~(NEXT_MASK << NEXT_SHIFT)) |
             (static_cast<uint64_t>(value) << NEXT_SHIFT);
  }
  void set_prev(uint16_t value) {
    qword_ = (qword_ & ~(PREV_MASK << PREV_SHIFT)) |
             (static_cast<uint64_t>(value) << PREV_SHIFT);
  }

  // A non-phantom node stores its label.
  // A phantom node returns an invalid label with IS_PHANTOM_FLAG.
  uint64_t label() const {
    // 8 bits.
    return qword_ & (IS_PHANTOM_FLAG | LABEL_MASK);
  }

  void set_label(uint8_t value) {
    qword_ = (qword_ & ~LABEL_MASK) | value;
  }

  // A leaf node stores the offset and the length of its associated key.
  uint64_t key_offset() const {
    // 40 bits.
    return (qword_ >> KEY_OFFSET_SHIFT) & KEY_OFFSET_MASK;
  }
  uint64_t key_length() const {
    // 12 bits.
    return (qword_ >> KEY_LENGTH_SHIFT) & KEY_LENGTH_MASK;
  }

  void set_key_offset(uint64_t value) {
    qword_ = (qword_ & ~(KEY_OFFSET_MASK << KEY_OFFSET_SHIFT)) |
             (value << KEY_OFFSET_SHIFT);
  }
  void set_key_length(uint64_t value) {
    qword_ = (qword_ & ~(KEY_LENGTH_MASK << KEY_LENGTH_SHIFT)) |
             (value << KEY_LENGTH_SHIFT);
  }

  // A non-phantom and non-leaf node stores the offset to its children,
  // the label of its next sibling, and the label of its first child.
  uint64_t offset() const {
    // 36 bits.
    return (qword_ >> 8) & ((uint64_t(1) << 36) - 1);
  }
  uint8_t child() const {
    // 8 bits.
    return static_cast<uint8_t>(qword_ >> 44);
  }
  uint8_t sibling() const {
    // 8 bits.
    return static_cast<uint8_t>(qword_ >> 52);
  }

  void set_offset(uint64_t value) {
    qword_ = (qword_ & ~(OFFSET_MASK << OFFSET_SHIFT)) |
             (value << OFFSET_SHIFT);
  }
  void set_child(uint8_t value) {
    qword_ = (qword_ & ~(CHILD_MASK << CHILD_SHIFT)) |
             (static_cast<uint64_t>(value) << CHILD_SHIFT);
  }
  void set_sibling(uint8_t value) {
    qword_ = (qword_ & ~(SIBLING_MASK << SIBLING_SHIFT)) |
             (static_cast<uint64_t>(value) << SIBLING_SHIFT);
  }

 private:
  uint64_t qword_;

  static constexpr uint64_t IS_ORIGIN_FLAG   = uint64_t(1) << 63;
  static constexpr uint64_t IS_PHANTOM_FLAG  = uint64_t(1) << 62;
  static constexpr uint64_t IS_LEAF_FLAG     = uint64_t(1) << 61;
  static constexpr uint64_t IS_TERMINAL_FLAG = uint64_t(1) << 60;

  static constexpr uint64_t NEXT_MASK  = 0x1FF;
  static constexpr uint8_t  NEXT_SHIFT = 0;
  static constexpr uint64_t PREV_MASK  = 0x1FF;
  static constexpr uint8_t  PREV_SHIFT = 9;

  static constexpr uint64_t LABEL_MASK = 0xFF;

  static constexpr uint64_t KEY_OFFSET_MASK  = (uint64_t(1) << 40) - 1;
  static constexpr uint8_t  KEY_OFFSET_SHIFT = 8;
  static constexpr uint64_t KEY_LENGTH_MASK  = (uint64_t(1) << 12) - 1;
  static constexpr uint8_t  KEY_LENGTH_SHIFT = 48;

  static constexpr uint64_t OFFSET_MASK   = (uint64_t(1) << 36) - 1;
  static constexpr uint8_t  OFFSET_SHIFT  = 8;
  static constexpr uint64_t CHILD_MASK    = (uint64_t(1) << 8) - 1;
  static constexpr uint8_t  CHILD_SHIFT   = 44;
  static constexpr uint64_t SIBLING_MASK  = (uint64_t(1) << 8) - 1;
  static constexpr uint8_t  SIBLING_SHIFT = 52;
};

class DoubleArrayEntry {
 public:
  DoubleArrayEntry() : qword_(0) {}

  // This entry is associated with a key (true) or not (false).
  bool is_valid() const {
    return qword_ & IS_VALID_FLAG;
  }

  // A valid entry stores the offset and the length of its associated key.
  uint64_t key_offset() const {
    return qword_ & OFFSET_MASK;
  }
  uint64_t key_length() const {
    return qword_ >> 48;
  }

  void set_key(uint64_t offset, uint64_t length) {
    qword_ = IS_VALID_FLAG | offset | (length << 48);
  }

  // An invalid entry stores the index of the next invalid entry.
  uint64_t next() const {
    return qword_;
  }

  void set_next(uint64_t next) {
    qword_ = next;
  }

 private:
  uint64_t qword_;

  // 11 (= 64 - (1 + 40 + 12)) bits are not used．
  static constexpr uint64_t OFFSET_MASK = uint64_t(1) << 40;
  static constexpr uint64_t IS_VALID_FLAG = uint64_t(1) << 47;
};

// TODO
class DoubleArrayKey {
 public:
  DoubleArrayKey(uint64_t id, const char *address, uint64_t length);

  uint64_t id() const {
    return id_low_ | (static_cast<uint64_t>(id_high_) << 32);
  }
  const void *ptr() const {
    return buf_;
  }

 private:
  uint32_t id_low_;
  uint8_t id_high_;
  uint8_t buf_[3];
};

// TODO
class DoubleArrayImpl {
 public:
  static std::unique_ptr<DoubleArrayImpl> create(io::Pool pool);
  static std::unique_ptr<DoubleArrayImpl> open(io::Pool pool,
                                               uint32_t block_id);

  uint32_t block_id() const {
    return block_info_->id();
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
};

// TODO
class DoubleArray {
 public:
  DoubleArray() = default;
  DoubleArray(const DoubleArrayCreate &, io::Pool pool)
    : impl_(DoubleArrayImpl::create(pool)) {}
  DoubleArray(const DoubleArrayOpen &, io::Pool pool, uint32_t block_id)
    : impl_(DoubleArrayImpl::open(pool, block_id)) {}

  explicit operator bool() const {
    return static_cast<bool>(impl_);
  }

  void create(io::Pool pool) {
    *this = DoubleArray(DOUBLE_ARRAY_CREATE, pool);
  }
  void open(io::Pool pool, uint32_t block_id) {
    *this = DoubleArray(DOUBLE_ARRAY_OPEN, pool, block_id);
  }
  void close() {
    *this = DoubleArray();
  }

  uint32_t block_id() const {
    return impl_->block_id();
  }

  void swap(DoubleArray &rhs) {
    impl_.swap(rhs.impl_);
  }

  StringBuilder &write_to(StringBuilder &builder) const {
    return impl_ ? impl_->write_to(builder) : (builder << "n/a");
  }

 private:
  std::shared_ptr<DoubleArrayImpl> impl_;
};

inline void swap(DoubleArray &lhs, DoubleArray &rhs) {
  lhs.swap(rhs);
}

inline StringBuilder &operator<<(StringBuilder &builder,
                                 const DoubleArray &da) {
  return da.write_to(builder);
}

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_DOUBLE_ARRAY_HPP
