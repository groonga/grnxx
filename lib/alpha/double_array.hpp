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

constexpr uint64_t DOUBLE_ARRAY_INVALID_ID     = 0xFFFFFFFFFFULL;
constexpr uint64_t DOUBLE_ARRAY_INVALID_OFFSET = 0;

constexpr uint64_t DOUBLE_ARRAY_CHUNK_SIZE      = 0x200;
constexpr uint64_t DOUBLE_ARRAY_CHUNK_MASK      = 0x1FF;

// Chunks are grouped by the level which indicates how easily update operations
// can find a good offset in that chunk. The chunk level rises when
// find_offset() fails in that chunk many times. DOUBLE_ARRAY_MAX_FAILURE_COUNT
// is the threshold. Also, in order to limit the time cost, find_offset() scans
// at most DOUBLE_ARRAY_MAX_CHUNk_COUNT chunks.
// Larger parameters bring more chances of finding good offsets but it leads to
// more node renumberings, which are costly operations, and thus results in
// a degradation of space/time efficiencies.
constexpr uint64_t DOUBLE_ARRAY_MAX_FAILURE_COUNT  = 4;
constexpr uint64_t DOUBLE_ARRAY_MAX_CHUNK_COUNT    = 16;
constexpr uint64_t DOUBLE_ARRAY_MAX_CHUNK_LEVEL    = 5;

// Chunks in the same level compose a doubly linked list. The entry chunk of
// a linked list is called a leader. DOUBLE_ARRAY_INVALID_LEADER means that
// the linked list is empty and there exists no leader.
constexpr uint64_t DOUBLE_ARRAY_INVALID_LEADER     = 0x7FFFFFFF;

class DoubleArrayString {
 public:
  DoubleArrayString() : ptr_(nullptr), length_(0) {}
  DoubleArrayString(const void *ptr, uint64_t length)
      : ptr_(static_cast<const uint8_t *>(ptr)), length_(length) {}

  const uint8_t &operator[](uint64_t i) const {
    return ptr_[i];
  }

  const void *ptr() const {
    return ptr_;
  }
  uint64_t length() const {
    return length_;
  }

  void assign(const void *ptr, uint64_t length) {
    ptr_ = static_cast<const uint8_t *>(ptr);
    length_ = length;
  }

  DoubleArrayString substr(uint64_t offset = 0) const {
    return DoubleArrayString(ptr_ + offset, length_ - offset);
  }
  DoubleArrayString substr(uint64_t offset, uint64_t length) const {
    return DoubleArrayString(ptr_ + offset, length);
  }

  // This function returns an integer as follows:
  // - a negative value if *this < rhs,
  // - zero if *this == rhs,
  // - a positive value if *this > rhs.
  // Note that the result is undefined if the offset is too large.
  int compare(const DoubleArrayString &rhs, uint64_t offset = 0) const {
    for (uint64_t i = offset; i < length(); ++i) {
      if (i >= rhs.length()) {
        return 1;
      } else if ((*this)[i] != rhs[i]) {
        return (*this)[i] - rhs[i];
      }
    }
    return (length() == rhs.length()) ? 0 : -1;
  }

 private:
  const uint8_t *ptr_;
  uint64_t length_;
};

inline bool operator==(const DoubleArrayString &lhs,
                       const DoubleArrayString &rhs) {
  if (lhs.length() != rhs.length()) {
    return false;
  } else if (lhs.ptr() == rhs.ptr()) {
    return true;
  }
  for (uint64_t i = 0; i < lhs.length(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}

inline bool operator!=(const DoubleArrayString &lhs,
                       const DoubleArrayString &rhs) {
  return !(lhs == rhs);
}

inline bool operator<(const DoubleArrayString &lhs,
                      const DoubleArrayString &rhs) {
  return lhs.compare(rhs) < 0;
}

inline bool operator>(const DoubleArrayString &lhs,
                      const DoubleArrayString &rhs) {
  return rhs < lhs;
}

inline bool operator<=(const DoubleArrayString &lhs,
                       const DoubleArrayString &rhs) {
  return !(lhs > rhs);
}

inline bool operator>=(const DoubleArrayString &lhs,
                       const DoubleArrayString &rhs) {
  return !(lhs < rhs);
}

class DoubleArrayHeader {
 public:
  DoubleArrayHeader();

  uint32_t nodes_block_id() const {
    return nodes_block_id_;
  }
  uint32_t chunks_block_id() const {
    return chunks_block_id_;
  }
  uint32_t entries_block_id() const {
    return entries_block_id_;
  }
  uint32_t keys_block_id() const {
    return keys_block_id_;
  }
  uint64_t root_node_id() const {
    return root_node_id_;
  }
  uint64_t num_chunks() const {
    return num_chunks_;
  }
  uint64_t num_nodes() const {
    return num_chunks_ * DOUBLE_ARRAY_CHUNK_SIZE;
  }
  uint64_t num_phantoms() const {
    return num_phantoms_;
  }
  uint64_t ith_leader(uint64_t i) const {
//    GRN_DAT_DEBUG_THROW_IF(i > DOUBLE_ARRAY_MAX_CHUNK_LEVEL);
    return leaders_[i];
  }

  void set_nodes_block_id(uint32_t value) {
    nodes_block_id_ = value;
  }
  void set_chunks_block_id(uint32_t value) {
    chunks_block_id_ = value;
  }
  void set_entries_block_id(uint32_t value) {
    entries_block_id_ = value;
  }
  void set_keys_block_id(uint32_t value) {
    keys_block_id_ = value;
  }
  void set_root_node_id(uint64_t value) {
    root_node_id_ = value;
  }
  void set_num_chunks(uint64_t value) {
    num_chunks_ = value;
  }
  void set_num_phantoms(uint64_t value) {
    num_phantoms_ = value;
  }
  void set_ith_leader(uint64_t i, uint64_t x) {
//    GRN_DAT_DEBUG_THROW_IF(i > MAX_BLOCK_LEVEL);
//    GRN_DAT_DEBUG_THROW_IF((x != INVALID_LEADER) && (x >= num_blocks()));
    leaders_[i] = x;
  }

  Mutex *mutable_inter_process_mutex() {
    return &inter_process_mutex_;
  }

 private:
  uint32_t nodes_block_id_;
  uint32_t chunks_block_id_;
  uint32_t entries_block_id_;
  uint32_t keys_block_id_;
  uint64_t root_node_id_;
  uint64_t num_chunks_;
  uint64_t num_phantoms_;
  uint64_t leaders_[DOUBLE_ARRAY_MAX_CHUNK_LEVEL + 1];
  Mutex inter_process_mutex_;
};

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

  // Phantom nodes are doubly linked in each chunk.
  // Each chunk consists of 512 nodes.
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

class DoubleArrayChunk {
 public:
  // Chunks in the same level are doubly linked.
  uint64_t next() const {
    // 44 bits.
    return (qwords_[0] & UPPER_MASK) >> UPPER_SHIFT;
  }
  uint64_t prev() const {
    // 44 bits.
    return (qwords_[1] & UPPER_MASK) >> UPPER_SHIFT;
  }

  void set_next(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~UPPER_MASK) | (value << UPPER_SHIFT);
  }
  void set_prev(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~UPPER_MASK) | (value << UPPER_SHIFT);
  }

  // The chunk level indicates how easily nodes can be put in this chunk.
  uint64_t level() const {
    // 10 bits.
    return (qwords_[0] & MIDDLE_MASK) >> MIDDLE_SHIFT;
  }
  uint64_t failure_count() const {
    // 10 bits.
    return (qwords_[1] & MIDDLE_MASK) >> MIDDLE_SHIFT;
  }

  void set_level(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~MIDDLE_MASK) | (value << MIDDLE_SHIFT);
  }
  void set_failure_count(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~MIDDLE_MASK) | (value << MIDDLE_SHIFT);
  }

  // The first phantom node and the number of phantom nodes in this chunk.
  uint64_t first_phantom() const {
    // 10 bits.
    return (qwords_[0] & LOWER_MASK) >> LOWER_SHIFT;
  }
  uint64_t num_phantoms() const {
    // 10 bits.
    return (qwords_[1] & LOWER_MASK) >> LOWER_SHIFT;
  }

  void set_first_phantom(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~LOWER_MASK) | (value << LOWER_SHIFT);
  }
  void set_num_phantoms(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~LOWER_MASK) | (value << LOWER_SHIFT);
  }

 private:
  uint64_t qwords_[2];

  static constexpr uint8_t  UPPER_SHIFT  = 20;
  static constexpr uint64_t UPPER_MASK   =
      ((uint64_t(1) << 44) - 1) << UPPER_SHIFT;
  static constexpr uint8_t  MIDDLE_SHIFT = 10;
  static constexpr uint64_t MIDDLE_MASK  =
      ((uint64_t(1) << 10) - 1) << MIDDLE_SHIFT;
  static constexpr uint8_t  LOWER_SHIFT  = 0;
  static constexpr uint64_t LOWER_MASK   =
      ((uint64_t(1) << 10) - 1) << LOWER_SHIFT;
};

class DoubleArrayEntry {
 public:
  DoubleArrayEntry() : qword_(0) {}

  // This entry is associated with a key (true) or not (false).
  explicit operator bool() const {
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

  explicit operator bool() const {
    // FIXME: Magic number.
    return id() != DOUBLE_ARRAY_INVALID_ID;
  }

  uint64_t id() const {
    return id_low_ | (static_cast<uint64_t>(id_high_) << 32);
  }
  const void *ptr() const {
    return buf_;
  }

  bool equals_to(const void *ptr, uint64_t length, uint64_t offset = 0) const {
    for ( ; offset < length; ++offset) {
      if (buf_[offset] != static_cast<const uint8_t *>(ptr)[offset]) {
        return false;
      }
    }
    return true;
  }

  static const DoubleArrayKey &invalid_key() {
    static const DoubleArrayKey invalid_key(
        DOUBLE_ARRAY_INVALID_ID, nullptr, 0);
    return invalid_key;
  }

  static uint64_t estimate_size(uint64_t length) {
    return 2 + (length / sizeof(uint32_t));
  }

 private:
  uint32_t id_low_;
  uint8_t id_high_;
  uint8_t buf_[3];
};

// TODO
class DoubleArrayImpl {
 public:
  ~DoubleArrayImpl();

  static std::unique_ptr<DoubleArrayImpl> create(io::Pool pool);
  static std::unique_ptr<DoubleArrayImpl> open(io::Pool pool,
                                               uint32_t block_id);

  bool search(const uint8_t *ptr, uint64_t length,
              uint64_t *key_offset = nullptr);

  // TODO
  bool insert(const uint8_t *ptr, uint64_t length,
              uint64_t *key_offset = nullptr);

  const DoubleArrayKey &get_key(uint64_t key_offset) {
    return *reinterpret_cast<const DoubleArrayKey *>(&keys_[key_offset]);
  }
  const DoubleArrayKey &ith_key(uint64_t key_id) {
    if (entries_[key_id]) {
      return get_key(entries_[key_id].key_offset());
    }
    return DoubleArrayKey::invalid_key();
  }

  uint32_t block_id() const {
    return block_info_->id();
  }
  uint64_t root_node_id() const {
    return header_->root_node_id();
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  DoubleArrayHeader *header_;
  Recycler *recycler_;
  Vector<DoubleArrayNode> nodes_;
  Vector<DoubleArrayChunk> chunks_;
  Vector<DoubleArrayEntry> entries_;
  Vector<uint32_t> keys_;
  bool initialized_;

  DoubleArrayImpl();

  void create_double_array(io::Pool pool);
  void open_double_array(io::Pool pool, uint32_t block_id);

  bool search_leaf(const uint8_t *ptr, uint64_t length,
                   uint64_t &node_id, uint64_t &query_pos);

  void reserve_node(uint64_t node_id);
  void reserve_chunk(uint64_t chunk_id);

  void update_chunk_level(uint64_t chunk_id, uint32_t level);
  void set_chunk_level(uint64_t chunk_id, uint32_t level);
  void unset_chunk_level(uint64_t chunk_id);
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
