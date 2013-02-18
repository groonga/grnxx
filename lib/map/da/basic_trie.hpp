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
#ifndef GRNXX_MAP_DA_BASIC_TRIE_HPP
#define GRNXX_MAP_DA_BASIC_TRIE_HPP

#include "trie.hpp"

#if 0
# define GRNXX_DEBUG_THROW(msg)\
   ({ GRNXX_ERROR() << msg; GRNXX_THROW(); })
# define GRNXX_DEBUG_THROW_IF(cond)\
   (void)((!(cond)) || (GRNXX_DEBUG_THROW(#cond), 0))
#else
# define GRNXX_DEBUG_THROW(msg)
# define GRNXX_DEBUG_THROW_IF(cond)
#endif

namespace grnxx {
namespace map {
namespace da {
namespace basic {

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
constexpr uint32_t INVALID_LEADER    = 0x7FFFFFFFU;

struct Header {
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

  Header();
};

class Node {
 public:
  Node() : qword_(IS_PHANTOM_FLAG) {}

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

  // TODO: The functions should be updated.

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

class Chunk {
 public:
  Chunk() : next_(0), prev_(0), others_(0) {}

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

class Entry {
 public:
  // Create a valid entry.
  static Entry valid_entry(uint32_t key_pos) {
    Entry entry;
    entry.dword_ = IS_VALID_FLAG | key_pos;
    return entry;
  }
  // Create an invalid entry.
  static Entry invalid_entry(uint32_t next) {
    Entry entry;
    entry.dword_ = next;
    return entry;
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
};

class Key {
 public:
  Key(int32_t id, const Slice &key)
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

class Trie : public da::Trie {
 public:
  ~Trie();

  static Trie *create(const TrieOptions &options, io::Pool pool);
  static Trie *open(io::Pool pool, uint32_t block_id);

  static void unlink(io::Pool pool, uint32_t block_id);

  Trie *defrag(const TrieOptions &options);

  uint32_t block_id() const;

  bool search(int64_t key_id, Slice *key = nullptr);
  bool search(const Slice &key, int64_t *key_id = nullptr);

  bool lcp_search(const Slice &query, int64_t *key_id = nullptr,
                  Slice *key = nullptr);

  bool insert(const Slice &key, int64_t *key_id = nullptr);

  bool remove(int64_t key_id);
  bool remove(const Slice &key);

  bool update(int64_t key_id, const Slice &dest_key);
  bool update(const Slice &src_key, const Slice &dest_key,
              int64_t *key_id = nullptr);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  Header *header_;
  Node *nodes_;
  Chunk *chunks_;
  Entry *entries_;
  uint32_t *keys_;
  bool initialized_;

  Trie();

  void create_trie(const TrieOptions &options, io::Pool pool);
  void open_trie(io::Pool pool, uint32_t block_id);

  void defrag_trie(const TrieOptions &options, const Trie &trie,
                   io::Pool pool);
  void defrag_trie(const Trie &trie, uint32_t src, uint32_t dest);

  void create_arrays();

  const Key &get_key(uint32_t key_pos) const {
    return *reinterpret_cast<const Key *>(&keys_[key_pos]);
  }

  bool remove_key(const Slice &key);
  bool update_key(int32_t key_id, const Slice &src_key,
                  const Slice &dest_key);

  bool search_leaf(const Slice &key, uint32_t &node_id, size_t &query_pos);
  bool insert_leaf(const Slice &key, uint32_t &node_id, size_t query_pos);

  uint32_t insert_node(uint32_t node_id, uint16_t label);
  uint32_t append_key(const Slice &key, int32_t key_id);

  uint32_t separate(const Slice &key, uint32_t node_id, size_t i);
  void resolve(uint32_t node_id, uint16_t label);
  void migrate_nodes(uint32_t node_id, uint32_t dest_offset,
                     const uint16_t *labels, uint16_t num_labels);

  uint32_t find_offset(const uint16_t *labels, uint16_t num_labels);

  void reserve_node(uint32_t node_id);
  void reserve_chunk(uint32_t chunk_id);

  void update_chunk_level(uint32_t chunk_id, uint32_t level);
  void set_chunk_level(uint32_t chunk_id, uint32_t level);
  void unset_chunk_level(uint32_t chunk_id);
};

}  // namespace basic
}  // namespace da
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DA_BASIC_TRIE_HPP
