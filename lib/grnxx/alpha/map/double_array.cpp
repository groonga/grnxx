/*
  Copyright (C) 2013  Brazil, Inc.

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
#include "grnxx/alpha/map/double_array.hpp"

#include "grnxx/exception.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {
namespace alpha {
namespace map {
namespace {

constexpr  int32_t MIN_KEY_ID     = 0;
constexpr  int32_t MAX_KEY_ID     = 0x7FFFFFFE;

constexpr   size_t MIN_KEY_SIZE   = 1;
constexpr   size_t MAX_KEY_SIZE   = 4095;

constexpr uint32_t INVALID_OFFSET = 0;

constexpr uint32_t ROOT_NODE_ID   = 0;

constexpr uint16_t TERMINAL_LABEL = 0x100;
constexpr uint16_t MAX_LABEL      = TERMINAL_LABEL;
constexpr uint16_t INVALID_LABEL  = 0x1FF;

constexpr uint32_t CHUNK_SIZE     = 0x200;
constexpr uint32_t CHUNK_MASK     = 0x1FF;

// Assume that #nodes per key is 4 and #uint32_ts per key is 8.
// Note that an entries is associated with a key.
constexpr uint32_t INITIAL_NODES_SIZE   = 1 << 16;
constexpr uint32_t INITIAL_ENTRIES_SIZE = 1 << 14;
constexpr uint32_t INITIAL_KEYS_SIZE    = 1 << 17;

constexpr uint32_t MAX_NODES_SIZE    =
    std::numeric_limits<uint32_t>::max() & ~CHUNK_MASK;
constexpr uint32_t MAX_ENTRIES_SIZE  = uint32_t(MAX_KEY_ID) + 1;
constexpr uint32_t MAX_KEYS_SIZE     = uint32_t(1) << 31;

// Chunks are grouped by the level which indicates how easily update operations
// can find a good offset in that chunk. The chunk level rises when
// find_offset() fails in that chunk many times. MAX_FAILURE_COUNT
// is the threshold. Also, in order to limit the time cost, find_offset() scans
// at most MAX_CHUNK_COUNT chunks.
// Larger parameters bring more chances of finding good offsets but it leads to
// more node renumberings, which are costly operations, and thus results in
// degradation of space/time efficiencies.
constexpr uint32_t MAX_FAILURE_COUNT = 4;
constexpr uint32_t MAX_CHUNK_COUNT   = 16;
constexpr uint32_t MAX_CHUNK_LEVEL   = 5;

// Chunks in the same level compose a doubly linked list. The entry chunk of
// a linked list is called a leader. INVALID_LEADER means that
// the linked list is empty and there exists no leader.
constexpr uint32_t INVALID_LEADER    = std::numeric_limits<uint32_t>::max();

class DoubleArrayException : Exception {
 public:
  DoubleArrayException() noexcept : Exception() {}
  ~DoubleArrayException() noexcept {}

  DoubleArrayException(const DoubleArrayException &x) noexcept : Exception(x) {}
  DoubleArrayException &operator=(const DoubleArrayException &) noexcept {
    return *this;
  }

  const char *what() const noexcept {
    return "";
  }
};

}  // namespace

struct DoubleArrayHeader {
  MapType map_type;
  uint32_t nodes_block_id;
  uint32_t chunks_block_id;
  uint32_t entries_block_id;
  uint32_t keys_block_id;
  uint32_t nodes_size;
  uint32_t chunks_size;
  uint32_t entries_size;
  uint32_t keys_size;
  int32_t next_key_id;
  uint32_t next_key_pos;
  int32_t max_key_id;
  uint64_t total_key_length;
  uint32_t num_keys;
  uint32_t num_chunks;
  uint32_t num_phantoms;
  uint32_t num_zombies;
  uint32_t leaders[MAX_CHUNK_LEVEL + 1];
  Mutex inter_process_mutex;

  DoubleArrayHeader();
};

DoubleArrayHeader::DoubleArrayHeader()
  : map_type(MAP_DOUBLE_ARRAY),
    nodes_block_id(io::BLOCK_INVALID_ID),
    chunks_block_id(io::BLOCK_INVALID_ID),
    entries_block_id(io::BLOCK_INVALID_ID),
    keys_block_id(io::BLOCK_INVALID_ID),
    nodes_size(0),
    chunks_size(0),
    entries_size(0),
    keys_size(0),
    next_key_id(0),
    next_key_pos(0),
    max_key_id(-1),
    total_key_length(0),
    num_keys(0),
    num_chunks(0),
    num_phantoms(0),
    num_zombies(0),
    leaders(),
    inter_process_mutex(MUTEX_UNLOCKED) {
  for (uint32_t i = 0; i <= MAX_CHUNK_LEVEL; ++i) {
    leaders[i] = INVALID_LEADER;
  }
}

class DoubleArrayNode {
 public:
  DoubleArrayNode() : qword_(IS_PHANTOM_FLAG) {}

  // Structure overview.
  //  0- 8 ( 9): next (is_phantom).
  //  9-17 ( 9): prev (is_phantom).
  //  0- 8 ( 9): label (!is_phantom).
  //  9-17 ( 9): sibling (!is_phantom).
  // 18-48 (31): key_pos (!is_phantom && is_leaf).
  // 18-49 (32): offset (!is_phantom && !is_leaf).
  // 50-58 ( 9): child (!is_phantom && !is_leaf).
  // 61-61 ( 1): is_leaf.
  // 62-62 ( 1): is_phantom.
  // 63-63 ( 1): is_origin.
  // Note that 0 is the LSB and 63 is the MSB.

  // The ID of this node is used as an offset (true) or not (false).
  bool is_origin() const {
    return qword_ & IS_ORIGIN_FLAG;
  }
  // This node is valid (false) or not (true).
  bool is_phantom() const {
    return qword_ & IS_PHANTOM_FLAG;
  }
  // This node is associated with a key (true) or not (false).
  bool is_leaf() const {
    return qword_ & IS_LEAF_FLAG;
  }

  void set_is_origin(bool value) {
    if (value) {
      qword_ |= IS_ORIGIN_FLAG;
    } else {
      qword_ &= ~IS_ORIGIN_FLAG;
    }
  }
  void set_is_phantom(bool value) {
    if (value) {
      qword_ = (qword_ & IS_ORIGIN_FLAG) | IS_PHANTOM_FLAG;
    } else {
      qword_ = (qword_ & IS_ORIGIN_FLAG) |
               (uint64_t(INVALID_OFFSET) << OFFSET_SHIFT) |
               (uint64_t(INVALID_LABEL) << CHILD_SHIFT) |
               (uint64_t(INVALID_LABEL) << SIBLING_SHIFT) | INVALID_LABEL;
    }
  }

  // Phantom nodes are doubly linked in each chunk.
  // Each chunk consists of 512 nodes.
  uint16_t next() const {
    return static_cast<uint16_t>(qword_ & NEXT_MASK);
  }
  uint16_t prev() const {
    return static_cast<uint16_t>((qword_ >> PREV_SHIFT) & PREV_MASK);
  }

  void set_next(uint16_t value) {
    qword_ = (qword_ & ~NEXT_MASK) | value;
  }
  void set_prev(uint16_t value) {
    qword_ = (qword_ & ~(PREV_MASK << PREV_SHIFT)) |
             (static_cast<uint64_t>(value) << PREV_SHIFT);
  }

  // A non-phantom node stores its label and the label of its next sibling.
  // A phantom node returns an invalid label with IS_PHANTOM_FLAG.
  // sibling() == INVALID_LABEL means that the node doesn't have next sibling.
  uint64_t label() const {
    return qword_ & (IS_PHANTOM_FLAG | LABEL_MASK);
  }
  uint16_t sibling() const {
    return static_cast<uint16_t>((qword_ >> SIBLING_SHIFT) & SIBLING_MASK);
  }

  void set_label(uint16_t value) {
    qword_ = (qword_ & ~LABEL_MASK) | value;
  }
  void set_sibling(uint16_t value) {
    qword_ = (qword_ & ~(SIBLING_MASK << SIBLING_SHIFT)) |
             (static_cast<uint64_t>(value) << SIBLING_SHIFT);
  }

  // A leaf node stores the start position of the associated key.
  uint32_t key_pos() const {
    return static_cast<uint32_t>((qword_ >> KEY_POS_SHIFT) & KEY_POS_MASK);
  }

  void set_key_pos(uint32_t value) {
    qword_ = (qword_ & ~(KEY_POS_MASK << KEY_POS_SHIFT)) |
             (static_cast<uint64_t>(value) << KEY_POS_SHIFT) | IS_LEAF_FLAG;
  }

  // A non-phantom and non-leaf node stores the offset to its children and the
  // label of its first child.
  // child() == INVALID_LABEL means that the node has no child.
  uint32_t offset() const {
    return static_cast<uint32_t>((qword_ >> OFFSET_SHIFT) & OFFSET_MASK);
  }
  uint16_t child() const {
    return static_cast<uint16_t>((qword_ >> CHILD_SHIFT) & CHILD_MASK);
  }

  void set_offset(uint32_t value) {
    if (qword_ & IS_LEAF_FLAG) {
      qword_ = ((qword_ & ~IS_LEAF_FLAG) & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (static_cast<uint64_t>(value) << OFFSET_SHIFT) |
               (uint64_t(INVALID_LABEL) << CHILD_SHIFT);
    } else {
      qword_ = (qword_ & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (static_cast<uint64_t>(value) << OFFSET_SHIFT);
    }
  }
  void set_child(uint16_t value) {
    qword_ = (qword_ & ~(CHILD_MASK << CHILD_SHIFT)) |
             (static_cast<uint64_t>(value) << CHILD_SHIFT);
  }

 private:
  uint64_t qword_;

  // 61-63.
  static constexpr uint64_t IS_ORIGIN_FLAG  = uint64_t(1) << 63;
  static constexpr uint64_t IS_PHANTOM_FLAG = uint64_t(1) << 62;
  static constexpr uint64_t IS_LEAF_FLAG    = uint64_t(1) << 61;

  //  0-17 (is_phantom).
  static constexpr uint64_t NEXT_MASK       = (uint64_t(1) << 9) - 1;
  static constexpr uint64_t PREV_MASK       = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  PREV_SHIFT      = 9;

  //  0-17 (!is_phantom).
  static constexpr uint64_t LABEL_MASK      = (uint64_t(1) << 9) - 1;
  static constexpr uint64_t SIBLING_MASK    = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  SIBLING_SHIFT   = 9;

  // 18-48 (!is_phantom && is_leaf)
  static constexpr uint64_t KEY_POS_MASK    = (uint64_t(1) << 31) - 1;
  static constexpr uint8_t  KEY_POS_SHIFT   = 18;

  // 18-58 (!is_phantom && !is_leaf)
  static constexpr uint64_t OFFSET_MASK     = (uint64_t(1) << 32) - 1;
  static constexpr uint8_t  OFFSET_SHIFT    = 18;
  static constexpr uint64_t CHILD_MASK      = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  CHILD_SHIFT     = 50;
};

class DoubleArrayChunk {
 public:
  DoubleArrayChunk() : next_(0), prev_(0), others_(0) {}

  // Chunks in the same level are doubly linked.
  uint32_t next() const {
    return next_;
  }
  uint32_t prev() const {
    return prev_;
  }

  void set_next(uint32_t value) {
    next_ = value;
  }
  void set_prev(uint32_t value) {
    prev_ = value;
  }

  // The chunk level indicates how easily nodes can be put in this chunk.
  uint32_t level() const {
    return (others_ >> LEVEL_SHIFT) & LEVEL_MASK;
  }
  uint32_t failure_count() const {
    return (others_ >> FAILURE_COUNT_SHIFT) & FAILURE_COUNT_MASK;
  }

  void set_level(uint32_t value) {
    others_ = (others_ & ~(LEVEL_MASK << LEVEL_SHIFT)) |
              (value << LEVEL_SHIFT);
  }
  void set_failure_count(uint32_t value) {
    others_ = (others_ & ~(FAILURE_COUNT_MASK << FAILURE_COUNT_SHIFT)) |
              (value << FAILURE_COUNT_SHIFT);
  }

  // The first phantom node and the number of phantom nodes in this chunk.
  uint32_t first_phantom() const {
    return (others_ >> FIRST_PHANTOM_SHIFT) & FIRST_PHANTOM_MASK;
  }
  uint32_t num_phantoms() const {
    return (others_ >> NUM_PHANTOMS_SHIFT) & NUM_PHANTOMS_MASK;
  }

  void set_first_phantom(uint32_t value) {
    others_ = (others_ & ~(FIRST_PHANTOM_MASK << FIRST_PHANTOM_SHIFT)) |
              (value << FIRST_PHANTOM_SHIFT);
  }
  void set_num_phantoms(uint32_t value) {
    others_ = (others_ & ~(NUM_PHANTOMS_MASK << NUM_PHANTOMS_SHIFT)) |
              (value << NUM_PHANTOMS_SHIFT);
  }

 private:
  uint32_t next_;
  uint32_t prev_;
  uint32_t others_;

  static constexpr uint32_t LEVEL_MASK          = (1 << 4) - 1;
  static constexpr uint8_t  LEVEL_SHIFT         = 0;

  static constexpr uint32_t FAILURE_COUNT_MASK  = (1 << 6) - 1;
  static constexpr uint8_t  FAILURE_COUNT_SHIFT = 4;

  static constexpr uint32_t FIRST_PHANTOM_MASK  = (1 << 10) - 1;
  static constexpr uint32_t FIRST_PHANTOM_SHIFT = 10;

  static constexpr uint32_t NUM_PHANTOMS_MASK   = (1 << 10) - 1;
  static constexpr uint32_t NUM_PHANTOMS_SHIFT  = 20;
};

class DoubleArrayEntry {
 public:
  // Create a valid entry.
  static DoubleArrayEntry valid_entry(uint32_t key_pos) {
    return DoubleArrayEntry(IS_VALID_FLAG | key_pos);
  }
  // Create an invalid entry.
  static DoubleArrayEntry invalid_entry(uint32_t next) {
    return DoubleArrayEntry(next);
  }

  // Return true iff "*this" is valid (associated with a key).
  explicit operator bool() const {
    return dword_ & IS_VALID_FLAG;
  }

  // Return the starting address of the associated key.
  // Available iff "*this' is valid.
  uint32_t key_pos() const {
    return dword_ & ~IS_VALID_FLAG;
  }

  // Return the next invalid entry.
  // Available iff "*this' is invalid.
  uint32_t next() const {
    return dword_;
  }

 private:
  uint32_t dword_;

  static constexpr uint32_t IS_VALID_FLAG = uint32_t(1) << 31;

  explicit DoubleArrayEntry(uint32_t x) : dword_(x) {}
};

class DoubleArrayKey {
 public:
  DoubleArrayKey(int32_t id, const Slice &key)
    : id_(id),
      size_(static_cast<uint16_t>(key.size())),
      buf_{ '\0', '\0' } {
    std::memcpy(buf_, key.ptr(), key.size());
  }

  const uint8_t &operator[](size_t i) const {
    return buf_[i];
  }

  int32_t id() const {
    return id_;
  }
  size_t size() const {
    return size_;
  }
  const uint8_t *ptr() const {
    return buf_;
  }
  Slice slice() const {
    return Slice(buf_, size_);
  }

  bool equals_to(const Slice &key, size_t offset = 0) const {
    if (key.size() != size_) {
      return false;
    }
    for ( ; offset < key.size(); ++offset) {
      if (buf_[offset] != key[offset]) {
        return false;
      }
    }
    return true;
  }

  static uint32_t estimate_size(const size_t key_size) {
    return (9 + key_size) / sizeof(uint32_t);
  }

 private:
  int32_t id_;
  uint16_t size_;
  uint8_t buf_[2];
};

template <typename T>
DoubleArray<T>::~DoubleArray() {
  if (!initialized_) try {
    // Free allocated blocks if initialization failed.
    if (header_->nodes_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->nodes_block_id);
    }
    if (header_->chunks_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->chunks_block_id);
    }
    if (header_->entries_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->entries_block_id);
    }
    if (header_->keys_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->keys_block_id);
    }
    if (block_info_) {
      pool_.free_block(*block_info_);
    }
  } catch (...) {
  }
}

template <typename T>
DoubleArray<T> *DoubleArray<T>::create(io::Pool pool,
                                       const MapOptions &options) {
  std::unique_ptr<DoubleArray<T>> double_array(
      new (std::nothrow) DoubleArray<T>);
  if (!double_array) {
    GRNXX_ERROR() << "new grnxx::alpha::map::DoubleArray failed";
    GRNXX_THROW();
  }
  double_array->create_double_array(pool, options);
  return double_array.release();
}

template <typename T>
DoubleArray<T> *DoubleArray<T>::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<DoubleArray<T>> double_array(
      new (std::nothrow) DoubleArray<T>);
  if (!double_array) {
    GRNXX_ERROR() << "new grnxx::alpha::map::DoubleArray failed";
    GRNXX_THROW();
  }
  double_array->open_double_array(pool, block_id);
  return double_array.release();
}

template <typename T>
bool DoubleArray<T>::unlink(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<DoubleArray<T>> double_array(open(pool, block_id));

  pool.free_block(double_array->header_->nodes_block_id);
  pool.free_block(double_array->header_->chunks_block_id);
  pool.free_block(double_array->header_->entries_block_id);
  pool.free_block(double_array->header_->keys_block_id);
  pool.free_block(block_id);
  return true;
}

template <typename T>
uint32_t DoubleArray<T>::block_id() const {
  return block_info_->id();
}

template <typename T>
MapType DoubleArray<T>::type() const {
  return MAP_DOUBLE_ARRAY;
}

template <typename T>
int64_t DoubleArray<T>::max_key_id() const {
  return header_->max_key_id;
}

template <typename T>
bool DoubleArray<T>::get(int64_t key_id, T *key) {
  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }

  const DoubleArrayEntry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  if (key) {
    const DoubleArrayKey &found_key = get_key(entry.key_pos());
    *key = found_key.slice();
  }
  return true;
}

template <typename T>
bool DoubleArray<T>::unset(int64_t key_id) {
  Lock lock(&header_->inter_process_mutex);

  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }
  const DoubleArrayEntry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const DoubleArrayKey &key = get_key(entry.key_pos());
  return remove_key(key.slice());
}

template <typename T>
bool DoubleArray<T>::reset(int64_t key_id, T dest_key) {
  if ((dest_key.size() < MIN_KEY_SIZE) || (dest_key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << dest_key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }
  const DoubleArrayEntry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const DoubleArrayKey &key = get_key(entry.key_pos());
  return update_key(key_id, key.slice(), dest_key);
}

template <typename T>
bool DoubleArray<T>::find(T key, int64_t *key_id) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    return false;
  }

  uint32_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;
  if (!find_leaf(key, node_id, query_pos)) {
    return false;
  }

  // Note that nodes_[node_id] might be updated by other threads/processes.
  const DoubleArrayNode node = nodes_[node_id];
  if (!node.is_leaf()) {
    return false;
  }

  const DoubleArrayKey &found_key = get_key(node.key_pos());
  if (found_key.equals_to(key, query_pos)) {
    if (key_id) {
      *key_id = found_key.id();
    }
    return true;
  }
  return false;
}

template <typename T>
bool DoubleArray<T>::insert(T key, int64_t *key_id) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, INSERTING_FLAG);

  uint32_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;

  find_leaf(key, node_id, query_pos);
  if (!insert_leaf(key, node_id, query_pos)) {
    if (key_id) {
      *key_id = get_key(nodes_[node_id].key_pos()).id();
    }
    return false;
  }

  const int32_t new_key_id = header_->next_key_id;
  const uint32_t new_key_pos = append_key(key, new_key_id);

  header_->total_key_length += key.size();
  ++header_->num_keys;

  if (new_key_id > header_->max_key_id) {
    header_->max_key_id = new_key_id;
    header_->next_key_id = new_key_id + 1;
  } else {
    header_->next_key_id = entries_[new_key_id].next();
  }

  entries_[new_key_id] = DoubleArrayEntry::valid_entry(new_key_pos);
  nodes_[node_id].set_key_pos(new_key_pos);
  if (key_id) {
    *key_id = new_key_id;
  }
  return true;
}

template <typename T>
bool DoubleArray<T>::remove(T key) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, REMOVING_FLAG);

  return remove_key(key);
}

template <typename T>
bool DoubleArray<T>::update(T src_key, T dest_key, int64_t *key_id) {
  if ((src_key.size() < MIN_KEY_SIZE) || (src_key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid source key: size = " << src_key.size();
    GRNXX_THROW();
  }
  if ((dest_key.size() < MIN_KEY_SIZE) || (dest_key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid destination key: size = " << dest_key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    return false;
  }
  if (update_key(static_cast<int32_t>(src_key_id), src_key, dest_key)) {
    if (key_id) {
      *key_id = src_key_id;
    }
    return true;
  }
  return false;
}

template <typename T>
bool DoubleArray<T>::find_longest_prefix_match(T query, int64_t *key_id,
                                               T *key) {
  bool found = false;
  uint32_t node_id = ROOT_NODE_ID;
  uint32_t query_pos = 0;

  for ( ; query_pos < query.size(); ++query_pos) {
    const DoubleArrayNode node = nodes_[node_id];
    if (node.is_leaf()) {
      const DoubleArrayKey &match = get_key(node.key_pos());
      if ((match.size() <= query.size()) &&
          match.equals_to(Slice(query.address(), match.size()), query_pos)) {
        if (key_id) {
          *key_id = match.id();
        }
        if (key) {
          *key = match.slice();
        }
        found = true;
      }
      return found;
    }

    if (nodes_[node_id].child() == TERMINAL_LABEL) {
      const DoubleArrayNode leaf_node = nodes_[node.offset() ^ TERMINAL_LABEL];
      if (leaf_node.is_leaf()) {
        if (key_id || key) {
          const DoubleArrayKey &match = get_key(leaf_node.key_pos());
          if (key_id) {
            *key_id = match.id();
          }
          if (key) {
            *key = match.slice();
          }
        }
        found = true;
      }
    }

    node_id = node.offset() ^ query[query_pos];
    if (nodes_[node_id].label() != query[query_pos]) {
      return found;
    }
  }

  const DoubleArrayNode node = nodes_[node_id];
  if (node.is_leaf()) {
    const DoubleArrayKey &match = get_key(node.key_pos());
    if (match.size() <= query.size()) {
      if (key_id) {
        *key_id = match.id();
      }
      if (key) {
        *key = match.slice();
      }
      found = true;
    }
  } else if (nodes_[node_id].child() == TERMINAL_LABEL) {
    const DoubleArrayNode leaf_node = nodes_[node.offset() ^ TERMINAL_LABEL];
    if (leaf_node.is_leaf()) {
      if (key_id || key) {
        const DoubleArrayKey &match = get_key(leaf_node.key_pos());
        if (key_id) {
          *key_id = match.id();
        }
        if (key) {
          *key = match.slice();
        }
      }
      found = true;
    }
  }
  return found;
}

template <typename T>
void DoubleArray<T>::truncate() {
  // TODO
}

template <typename T>
DoubleArray<T>::DoubleArray()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    nodes_(nullptr),
    chunks_(nullptr),
    entries_(nullptr),
    keys_(nullptr),
    initialized_(false) {}

template <typename T>
void DoubleArray<T>::create_double_array(io::Pool pool, const MapOptions &) {
  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(DoubleArrayHeader));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);
  *header_ = DoubleArrayHeader();

  // TODO: The size should be given as options.
  header_->nodes_size = static_cast<uint32_t>(INITIAL_NODES_SIZE);
  header_->nodes_size &= ~CHUNK_MASK;
  if (header_->nodes_size == 0) {
    header_->nodes_size = INITIAL_NODES_SIZE;
  }
  header_->chunks_size = header_->nodes_size / CHUNK_SIZE;
  header_->entries_size = static_cast<uint32_t>(INITIAL_ENTRIES_SIZE);
  if (header_->entries_size == 0) {
    header_->entries_size = INITIAL_ENTRIES_SIZE;
  }
  header_->keys_size = static_cast<uint32_t>(INITIAL_KEYS_SIZE);
  if (header_->keys_size == 0) {
    header_->keys_size = INITIAL_KEYS_SIZE;
  }

  create_arrays();

  reserve_node(ROOT_NODE_ID);
  nodes_[INVALID_OFFSET].set_is_origin(true);

  initialized_ = true;
}

template <typename T>
void DoubleArray<T>::open_double_array(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  initialized_ = true;

  block_info_ = pool_.get_block_info(block_id);

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<DoubleArrayHeader *>(block_address);

  // TODO: Check the format.

  nodes_ = static_cast<DoubleArrayNode *>(
      pool_.get_block_address(header_->nodes_block_id));
  chunks_ = static_cast<DoubleArrayChunk *>(
      pool_.get_block_address(header_->chunks_block_id));
  entries_ = static_cast<DoubleArrayEntry *>(
      pool_.get_block_address(header_->entries_block_id));
  keys_ = static_cast<uint32_t *>(
      pool_.get_block_address(header_->keys_block_id));
}

template <typename T>
void DoubleArray<T>::create_arrays() {
  const io::BlockInfo *block_info;

  block_info = pool_.create_block(
      sizeof(DoubleArrayNode) * header_->nodes_size);
  header_->nodes_block_id = block_info->id();
  nodes_ = static_cast<DoubleArrayNode *>(
      pool_.get_block_address(*block_info));

  block_info = pool_.create_block(
      sizeof(DoubleArrayChunk) * header_->chunks_size);
  header_->chunks_block_id = block_info->id();
  chunks_ = static_cast<DoubleArrayChunk *>(
      pool_.get_block_address(*block_info));

  block_info = pool_.create_block(
      sizeof(DoubleArrayEntry) * header_->entries_size);
  header_->entries_block_id = block_info->id();
  entries_ = static_cast<DoubleArrayEntry *>(
      pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(uint32_t) * header_->keys_size);
  header_->keys_block_id = block_info->id();
  keys_ = static_cast<uint32_t *>(pool_.get_block_address(*block_info));
}

template <typename T>
bool DoubleArray<T>::remove_key(const Slice &key) {
  uint32_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;
  if (!find_leaf(key, node_id, query_pos)) {
    return false;
  }

  const uint32_t key_pos = nodes_[node_id].key_pos();
  const DoubleArrayKey &found_key = get_key(key_pos);
  if (!found_key.equals_to(key, query_pos)) {
    return false;
  }

  const int32_t key_id = found_key.id();
  nodes_[node_id].set_offset(INVALID_OFFSET);
  entries_[key_id] = DoubleArrayEntry::invalid_entry(header_->next_key_id);

  header_->next_key_id = key_id;
  header_->total_key_length -= key.size();
  --header_->num_keys;
  return true;
}

template <typename T>
bool DoubleArray<T>::update_key(int32_t key_id, const Slice &src_key,
                                const Slice &dest_key) {
  uint32_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;

  find_leaf(dest_key, node_id, query_pos);
  if (!insert_leaf(dest_key, node_id, query_pos)) {
    return false;
  }

  const uint32_t new_key_pos = append_key(dest_key, key_id);
  header_->total_key_length =
      header_->total_key_length + dest_key.size() - src_key.size();
  entries_[key_id] = DoubleArrayEntry::valid_entry(new_key_pos);
  nodes_[node_id].set_key_pos(new_key_pos);

  node_id = ROOT_NODE_ID;
  query_pos = 0;
  if (!find_leaf(src_key, node_id, query_pos)) {
    GRNXX_ERROR() << "key not found (unexpected)";
    GRNXX_THROW();
  }
  nodes_[node_id].set_offset(INVALID_OFFSET);
  return true;
}

template <typename T>
bool DoubleArray<T>::find_leaf(const Slice &key, uint32_t &node_id,
                               size_t &query_pos) {
  for ( ; query_pos < key.size(); ++query_pos) {
    const DoubleArrayNode node = nodes_[node_id];
    if (node.is_leaf()) {
      return true;
    }

    const uint32_t next = node.offset() ^ key[query_pos];
    if (nodes_[next].label() != key[query_pos]) {
      return false;
    }
    node_id = next;
  }

  const DoubleArrayNode node = nodes_[node_id];
  if (node.is_leaf()) {
    return true;
  }

  if (node.child() != TERMINAL_LABEL) {
    return false;
  }
  node_id = node.offset() ^ TERMINAL_LABEL;
  return nodes_[node_id].is_leaf();
}

template <typename T>
bool DoubleArray<T>::insert_leaf(const Slice &key, uint32_t &node_id,
                                 size_t query_pos) {
  const DoubleArrayNode node = nodes_[node_id];
  if (node.is_leaf()) {
    const DoubleArrayKey &found_key = get_key(node.key_pos());
    size_t i = query_pos;
    while ((i < key.size()) && (i < found_key.size())) {
      if (key[i] != found_key[i]) {
        break;
      }
      ++i;
    }
    if ((i == key.size()) && (i == found_key.size())) {
      return false;
    }

    if (header_->num_keys >= header_->entries_size) {
      GRNXX_NOTICE() << "too many keys: num_keys = " << header_->num_keys
                     << ", entries_size = " << header_->entries_size;
      throw DoubleArrayException();
    }

//    GRNXX_DEBUG_THROW_IF(static_cast<uint32_t>(header_->next_key_id) >= header_->entries_size);

    for (size_t j = query_pos; j < i; ++j) {
      node_id = insert_node(node_id, key[j]);
    }
    node_id = separate(key, node_id, i);
    return true;
  } else if (node.label() == TERMINAL_LABEL) {
    return true;
  } else {
    if (header_->num_keys >= header_->entries_size) {
      GRNXX_NOTICE() << "too many keys: num_keys = " << header_->num_keys
                     << ", entries_size = " << header_->entries_size;
      throw DoubleArrayException();
    }

    const uint16_t label = (query_pos < key.size()) ?
        static_cast<uint16_t>(key[query_pos]) : TERMINAL_LABEL;
    if ((node.offset() == INVALID_OFFSET) ||
        !nodes_[node.offset() ^ label].is_phantom()) {
      // The offset of this node must be updated.
      resolve(node_id, label);
    }
    // The new node will be the leaf node associated with the query.
    node_id = insert_node(node_id, label);
    return true;
  }
}

template <typename T>
uint32_t DoubleArray<T>::insert_node(uint32_t node_id, uint16_t label) {
//  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
//  GRNXX_DEBUG_THROW_IF(label > MAX_LABEL);

  const DoubleArrayNode node = nodes_[node_id];
  uint32_t offset;
  if (node.is_leaf() || (node.offset() == INVALID_OFFSET)) {
    offset = find_offset(&label, 1);
  } else {
    offset = node.offset();
  }

  const uint32_t next = offset ^ label;
  reserve_node(next);

  nodes_[next].set_label(label);
  if (node.is_leaf()) {
//    GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
    nodes_[next].set_key_pos(node.key_pos());
  } else if (node.offset() == INVALID_OFFSET) {
//    GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
  } else {
//    GRNXX_DEBUG_THROW_IF(!nodes_[offset].is_origin());
  }
  nodes_[node_id].set_offset(offset);

  const uint16_t child_label = nodes_[node_id].child();
//  GRNXX_DEBUG_THROW_IF(child_label == label);
  if (child_label == INVALID_LABEL) {
    nodes_[node_id].set_child(label);
  } else if ((label == TERMINAL_LABEL) ||
             ((child_label != TERMINAL_LABEL) &&
              (label < child_label))) {
    // The next node becomes the first child.
//    GRNXX_DEBUG_THROW_IF(nodes_[offset ^ child_label].is_phantom());
//    GRNXX_DEBUG_THROW_IF(nodes_[offset ^ child_label].label() != child_label);
    nodes_[next].set_sibling(child_label);
    nodes_[node_id].set_child(label);
  } else {
    uint32_t prev = offset ^ child_label;
//    GRNXX_DEBUG_THROW_IF(nodes_[prev].label() != child_label);
    uint16_t sibling_label = nodes_[prev].sibling();
    while (label > sibling_label) {
      prev = offset ^ sibling_label;
//      GRNXX_DEBUG_THROW_IF(nodes_[prev].label() != sibling_label);
      sibling_label = nodes_[prev].sibling();
    }
//    GRNXX_DEBUG_THROW_IF(label == sibling_label);
    nodes_[next].set_sibling(nodes_[prev].sibling());
    nodes_[prev].set_sibling(label);
  }
  return next;
}

template <typename T>
uint32_t DoubleArray<T>::append_key(const Slice &key, int32_t key_id) {
  if (static_cast<uint32_t>(key_id) >= header_->entries_size) {
    GRNXX_NOTICE() << "too many keys: key_id = " << key_id
                   << ", entries_size = " << header_->entries_size;
    throw DoubleArrayException();
  }

  const uint32_t key_pos = header_->next_key_pos;
  const uint32_t key_size = DoubleArrayKey::estimate_size(key.size());

  if (key_size > (header_->keys_size - key_pos)) {
    GRNXX_NOTICE() << "too many keys: key_size = " << key_size
                   << ", keys_size = " << header_->keys_size
                   << ", key_pos = " << key_pos;
    throw DoubleArrayException();
  }
  new (&keys_[key_pos]) DoubleArrayKey(key_id, key);

  header_->next_key_pos = key_pos + key_size;
  return key_pos;
}

template <typename T>
uint32_t DoubleArray<T>::separate(const Slice &key, uint32_t node_id,
                                  size_t i) {
//  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
//  GRNXX_DEBUG_THROW_IF(!nodes_[node_id].is_leaf());
//  GRNXX_DEBUG_THROW_IF(i > key.size());

  const DoubleArrayNode node = nodes_[node_id];
  const DoubleArrayKey &found_key = get_key(node.key_pos());

  uint16_t labels[2];
  labels[0] = (i < found_key.size()) ?
      static_cast<uint16_t>(found_key[i]) : TERMINAL_LABEL;
  labels[1] = (i < key.size()) ?
      static_cast<uint16_t>(key[i]) : TERMINAL_LABEL;
//  GRNXX_DEBUG_THROW_IF(labels[0] == labels[1]);

  const uint32_t offset = find_offset(labels, 2);

  uint32_t next = offset ^ labels[0];
  reserve_node(next);
//  GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());

  nodes_[next].set_label(labels[0]);
  nodes_[next].set_key_pos(node.key_pos());

  next = offset ^ labels[1];
  reserve_node(next);

  nodes_[next].set_label(labels[1]);

  nodes_[offset].set_is_origin(true);
  nodes_[node_id].set_offset(offset);

  if ((labels[0] == TERMINAL_LABEL) ||
      ((labels[1] != TERMINAL_LABEL) &&
       (labels[0] < labels[1]))) {
    nodes_[offset ^ labels[0]].set_sibling(labels[1]);
    nodes_[node_id].set_child(labels[0]);
  } else {
    nodes_[offset ^ labels[1]].set_sibling(labels[0]);
    nodes_[node_id].set_child(labels[1]);
  }
  return next;
}

template <typename T>
void DoubleArray<T>::resolve(uint32_t node_id, uint16_t label) {
//  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
//  GRNXX_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRNXX_DEBUG_THROW_IF(label > MAX_LABEL);

  uint32_t offset = nodes_[node_id].offset();
  if (offset != INVALID_OFFSET) {
    uint16_t labels[MAX_LABEL + 1];
    uint16_t num_labels = 0;

    uint16_t next_label = nodes_[node_id].child();
//    GRNXX_DEBUG_THROW_IF(next_label == INVALID_LABEL);
    while (next_label != INVALID_LABEL) {
//      GRNXX_DEBUG_THROW_IF(next_label > MAX_LABEL);
      labels[num_labels++] = next_label;
      next_label = nodes_[offset ^ next_label].sibling();
    }
//    GRNXX_DEBUG_THROW_IF(num_labels == 0);

    labels[num_labels] = label;
    offset = find_offset(labels, num_labels + 1);
    migrate_nodes(node_id, offset, labels, num_labels);
  } else {
    offset = find_offset(&label, 1);
    if (offset >= (header_->num_chunks * CHUNK_SIZE)) {
//      GRNXX_DEBUG_THROW_IF((offset / CHUNK_SIZE) != header_->num_chunks);
      reserve_chunk(header_->num_chunks);
    }
    nodes_[offset].set_is_origin(true);
    nodes_[node_id].set_offset(offset);
  }
}

template <typename T>
void DoubleArray<T>::migrate_nodes(uint32_t node_id, uint32_t dest_offset,
                                   const uint16_t *labels,
                                   uint16_t num_labels) {
//  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
//  GRNXX_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRNXX_DEBUG_THROW_IF(labels == nullptr);
//  GRNXX_DEBUG_THROW_IF(num_labels == 0);
//  GRNXX_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  const uint32_t src_offset = nodes_[node_id].offset();
//  GRNXX_DEBUG_THROW_IF(src_offset == INVALID_OFFSET);
//  GRNXX_DEBUG_THROW_IF(!nodes_[src_offset].is_origin());

  for (uint16_t i = 0; i < num_labels; ++i) {
    const uint32_t src_node_id = src_offset ^ labels[i];
    const uint32_t dest_node_id = dest_offset ^ labels[i];
//    GRNXX_DEBUG_THROW_IF(nodes_[src_node_id].is_phantom());
//    GRNXX_DEBUG_THROW_IF(nodes_[src_node_id].label() != labels[i]);

    reserve_node(dest_node_id);
    DoubleArrayNode dest_node = nodes_[src_node_id];
    dest_node.set_is_origin(nodes_[dest_node_id].is_origin());
    nodes_[dest_node_id] = dest_node;
  }
  header_->num_zombies += num_labels;

//  GRNXX_DEBUG_THROW_IF(nodes_[dest_offset].is_origin());
  nodes_[dest_offset].set_is_origin(true);
  nodes_[node_id].set_offset(dest_offset);
}

template <typename T>
uint32_t DoubleArray<T>::find_offset(const uint16_t *labels,
                                     uint16_t num_labels) {
//  GRNXX_DEBUG_THROW_IF(labels == nullptr);
//  GRNXX_DEBUG_THROW_IF(num_labels == 0);
//  GRNXX_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  // Chunks are tested in descending order of level. Basically, lower level
  // chunks contain more phantom nodes.
  uint32_t level = 1;
  while (num_labels >= (1U << level)) {
    ++level;
  }
  level = (level < MAX_CHUNK_LEVEL) ? (MAX_CHUNK_LEVEL - level) : 0;

  uint32_t chunk_count = 0;
  do {
    uint32_t leader = header_->leaders[level];
    if (leader == INVALID_LEADER) {
      // This level group is skipped because it is empty.
      continue;
    }

    uint32_t chunk_id = leader;
    do {
      const DoubleArrayChunk &chunk = chunks_[chunk_id];
//      GRNXX_DEBUG_THROW_IF(chunk.level() != level);

      const uint32_t first = (chunk_id * CHUNK_SIZE) | chunk.first_phantom();
      uint32_t node_id = first;
      do {
//        GRNXX_DEBUG_THROW_IF(!nodes_[node_id].is_phantom());
        const uint32_t offset = node_id ^ labels[0];
        if (!nodes_[offset].is_origin()) {
          uint16_t i = 1;
          for ( ; i < num_labels; ++i) {
            if (!nodes_[offset ^ labels[i]].is_phantom()) {
              break;
            }
          }
          if (i >= num_labels) {
            return offset;
          }
        }
        node_id = (chunk_id * CHUNK_SIZE) | nodes_[node_id].next();
      } while (node_id != first);

      const uint32_t prev = chunk_id;
      const uint32_t next = chunk.next();
      chunk_id = next;
      chunks_[prev].set_failure_count(chunks_[prev].failure_count() + 1);

      // The level of a chunk is updated when this function fails many times,
      // actually MAX_FAILURE_COUNT times, in that chunk.
      if (chunks_[prev].failure_count() == MAX_FAILURE_COUNT) {
        update_chunk_level(prev, level + 1);
        if (next == leader) {
          break;
        } else {
          // Note that the leader might be updated in the level update.
          leader = header_->leaders[level];
          continue;
        }
      }
    } while ((++chunk_count < MAX_CHUNK_COUNT) &&
             (chunk_id != leader));
  } while ((chunk_count < MAX_CHUNK_COUNT) && (level-- != 0));

  return (header_->num_chunks * CHUNK_SIZE) ^ labels[0];
}

template <typename T>
void DoubleArray<T>::reserve_node(uint32_t node_id) {
  if (node_id >= (header_->num_chunks * CHUNK_SIZE)) {
    reserve_chunk(node_id / CHUNK_SIZE);
  }

  DoubleArrayNode &node = nodes_[node_id];
//  GRNXX_DEBUG_THROW_IF(!node.is_phantom());

  const uint32_t chunk_id = node_id / CHUNK_SIZE;
  DoubleArrayChunk &chunk = chunks_[chunk_id];
//  GRNXX_DEBUG_THROW_IF(chunk.num_phantoms() == 0);

  const uint32_t next = (chunk_id * CHUNK_SIZE) | node.next();
  const uint32_t prev = (chunk_id * CHUNK_SIZE) | node.prev();
//  GRNXX_DEBUG_THROW_IF(next >= (header_->num_chunks * CHUNK_SIZE));
//  GRNXX_DEBUG_THROW_IF(prev >= (header_->num_chunks * CHUNK_SIZE));

  if ((node_id & CHUNK_MASK) == chunk.first_phantom()) {
    // The first phantom node is removed from the chunk and the second phantom
    // node comes first.
    chunk.set_first_phantom(next & CHUNK_MASK);
  }

  nodes_[next].set_prev(prev & CHUNK_MASK);
  nodes_[prev].set_next(next & CHUNK_MASK);

  if (chunk.level() != MAX_CHUNK_LEVEL) {
    const uint32_t threshold =
        uint32_t(1) << ((MAX_CHUNK_LEVEL - chunk.level() - 1) * 2);
    if (chunk.num_phantoms() == threshold) {
      update_chunk_level(chunk_id, chunk.level() + 1);
    }
  }
  chunk.set_num_phantoms(chunk.num_phantoms() - 1);

  node.set_is_phantom(false);

//  GRNXX_DEBUG_THROW_IF(node.offset() != INVALID_OFFSET);
//  GRNXX_DEBUG_THROW_IF(node.label() != INVALID_LABEL);

  --header_->num_phantoms;
}

template <typename T>
void DoubleArray<T>::reserve_chunk(uint32_t chunk_id) {
//  GRNXX_DEBUG_THROW_IF(chunk_id != header_->num_chunks);

  if (chunk_id >= header_->chunks_size) {
    GRNXX_NOTICE() << "too many chunks: chunk_id = " << chunk_id
                   << ", chunks_size = " << header_->chunks_size;
    throw DoubleArrayException();
  }

  header_->num_chunks = chunk_id + 1;

  DoubleArrayChunk chunk;
  chunk.set_failure_count(0);
  chunk.set_first_phantom(0);
  chunk.set_num_phantoms(CHUNK_SIZE);
  chunks_[chunk_id] = chunk;

  const uint32_t begin = chunk_id * CHUNK_SIZE;
  const uint32_t end = begin + CHUNK_SIZE;
//  GRNXX_DEBUG_THROW_IF(end != (header_->num_chunks * CHUNK_SIZE));

  DoubleArrayNode node;
  node.set_is_phantom(true);
  for (uint32_t i = begin; i < end; ++i) {
    node.set_prev((i - 1) & CHUNK_MASK);
    node.set_next((i + 1) & CHUNK_MASK);
    nodes_[i] = node;
  }

  // The level of the new chunk is 0.
  set_chunk_level(chunk_id, 0);
  header_->num_phantoms += CHUNK_SIZE;
}

template <typename T>
void DoubleArray<T>::update_chunk_level(uint32_t chunk_id, uint32_t level) {
//  GRNXX_DEBUG_THROW_IF(chunk_id >= header_->num_chunks);
//  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  unset_chunk_level(chunk_id);
  set_chunk_level(chunk_id, level);
}

template <typename T>
void DoubleArray<T>::set_chunk_level(uint32_t chunk_id, uint32_t level) {
//  GRNXX_DEBUG_THROW_IF(chunk_id >= header_->num_chunks);
//  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  const uint32_t leader = header_->leaders[level];
  if (leader == INVALID_LEADER) {
    // The chunk becomes the only one member of the level group.
    chunks_[chunk_id].set_next(chunk_id);
    chunks_[chunk_id].set_prev(chunk_id);
    header_->leaders[level] = chunk_id;
  } else {
    // The chunk is appended to the level group.
    const uint32_t next = leader;
    const uint32_t prev = chunks_[leader].prev();
//    GRNXX_DEBUG_THROW_IF(next >= header_->num_chunks);
//    GRNXX_DEBUG_THROW_IF(prev >= header_->num_chunks);
    chunks_[chunk_id].set_next(next);
    chunks_[chunk_id].set_prev(prev);
    chunks_[next].set_prev(chunk_id);
    chunks_[prev].set_next(chunk_id);
  }
  chunks_[chunk_id].set_level(level);
  chunks_[chunk_id].set_failure_count(0);
}

template <typename T>
void DoubleArray<T>::unset_chunk_level(uint32_t chunk_id) {
  const uint32_t level = chunks_[chunk_id].level();
//  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);
  const uint32_t leader = header_->leaders[level];
//  GRNXX_DEBUG_THROW_IF(leader == INVALID_LEADER);
  const uint32_t next = chunks_[chunk_id].next();
  const uint32_t prev = chunks_[chunk_id].prev();
//  GRNXX_DEBUG_THROW_IF(next >= header_->num_chunks);
//  GRNXX_DEBUG_THROW_IF(prev >= header_->num_chunks);

  if (next == chunk_id) {
    // The level group becomes empty.
    header_->leaders[level] = INVALID_LEADER;
  } else {
    chunks_[next].set_prev(prev);
    chunks_[prev].set_next(next);
    if (chunk_id == leader) {
      // The second chunk becomes the leader of the level group.
      header_->leaders[level] = next;
    }
  }
}

template class DoubleArray<Slice>;

}  // namespace map
}  // namespace alpha
}  // namespace grnxx
