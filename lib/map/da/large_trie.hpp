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
#ifndef GRNXX_MAP_DA_LARGE_TRIE_HPP
#define GRNXX_MAP_DA_LARGE_TRIE_HPP

#include "basic_trie.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace large {

constexpr  int64_t MIN_KEY_ID     = 0;
constexpr  int64_t MAX_KEY_ID     = 0x7FFFFFFFFFLL;

constexpr   size_t MIN_KEY_SIZE   = 1;
constexpr   size_t MAX_KEY_SIZE   = 4095;

constexpr uint64_t INVALID_OFFSET = 0;

constexpr uint64_t ROOT_NODE_ID   = 0;

constexpr uint16_t TERMINAL_LABEL = 0x100;
constexpr uint16_t MAX_LABEL      = TERMINAL_LABEL;
constexpr uint16_t INVALID_LABEL  = 0x1FF;

constexpr uint64_t CHUNK_SIZE     = 0x200;
constexpr uint64_t CHUNK_MASK     = 0x1FF;

// Assume that #nodes per key is 4 and #uint32_ts per key is 8.
// Note that an entries is associated with a key.
constexpr uint64_t INITIAL_NODES_SIZE   = 1 << 16;
constexpr uint64_t INITIAL_ENTRIES_SIZE = 1 << 14;
constexpr uint64_t INITIAL_KEYS_SIZE    = 1 << 17;

constexpr uint64_t MAX_NODES_SIZE    = uint64_t(1) << 42;
constexpr uint64_t MAX_ENTRIES_SIZE  = uint64_t(MAX_KEY_ID) + 1;
constexpr uint64_t MAX_KEYS_SIZE     = uint64_t(1) << 39;

// Chunks are grouped by the level which indicates how easily update operations
// can find a good offset in that chunk. The chunk level rises when
// find_offset() fails in that chunk many times. MAX_FAILURE_COUNT
// is the threshold. Also, in order to limit the time cost, find_offset() scans
// at most MAX_CHUNK_COUNT chunks.
// Larger parameters bring more chances of finding good offsets but it leads to
// more node renumberings, which are costly operations, and thus results in
// degradation of space/time efficiencies.
constexpr uint64_t MAX_FAILURE_COUNT = 4;
constexpr uint64_t MAX_CHUNK_COUNT   = 16;
constexpr uint64_t MAX_CHUNK_LEVEL   = 5;

// Chunks in the same level compose a doubly linked list. The entry chunk of
// a linked list is called a leader. INVALID_LEADER means that
// the linked list is empty and there exists no leader.
constexpr uint64_t INVALID_LEADER    = std::numeric_limits<uint64_t>::max();

struct Header {
  TrieType type;
  uint32_t nodes_block_id;
  uint32_t siblings_block_id;
  uint32_t chunks_block_id;
  uint32_t entries_block_id;
  uint32_t keys_block_id;
  uint64_t nodes_size;
  uint64_t chunks_size;
  uint64_t entries_size;
  uint64_t keys_size;
  int64_t next_key_id;
  uint64_t next_key_pos;
  int64_t max_key_id;
  uint64_t total_key_length;
  uint64_t num_keys;
  uint64_t num_chunks;
  uint64_t num_phantoms;
  uint64_t num_zombies;
  uint64_t leaders[MAX_CHUNK_LEVEL + 1];
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
  //  9-47 (39): key_pos (!is_phantom && is_leaf).
  // 48-59 (12): key_size (!is_phantom && is_leaf).
  //  9-50 (42): offset (!is_phantom && !is_leaf).
  // 51-59 ( 9): child (!is_phantom && !is_leaf).
  // 60-60 ( 1): has_sibling.
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
  // This node has next sibling (true) or not (false).
  bool has_sibling() const {
    return qword_ & HAS_SIBLING_FLAG;
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
      qword_ = (qword_ & IS_ORIGIN_FLAG) | (INVALID_OFFSET << OFFSET_SHIFT) |
               (uint64_t(INVALID_LABEL) << CHILD_SHIFT) | INVALID_LABEL;
    }
  }
  void set_has_sibling(bool value) {
    if (value) {
      qword_ |= HAS_SIBLING_FLAG;
    } else {
      qword_ &= ~HAS_SIBLING_FLAG;
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

  // A non-phantom node stores its label.
  // A phantom node returns an invalid label with IS_PHANTOM_FLAG.
  uint64_t label() const {
    return qword_ & (IS_PHANTOM_FLAG | LABEL_MASK);
  }

  void set_label(uint16_t value) {
    qword_ = (qword_ & ~LABEL_MASK) | value;
  }

  // A leaf node stores the start position and the size of the associated key.
  uint64_t key_pos() const {
    return (qword_ >> KEY_POS_SHIFT) & KEY_POS_MASK;
  }
  uint64_t key_size() const {
    return (qword_ >> KEY_SIZE_SHIFT) & KEY_SIZE_MASK;
  }

  void set_key(uint64_t key_pos, size_t key_size) {
    qword_ = (qword_ & ~((KEY_POS_MASK << KEY_POS_SHIFT) |
                         (KEY_SIZE_MASK << KEY_SIZE_SHIFT))) |
             (key_pos << KEY_POS_SHIFT) | (key_size << KEY_SIZE_SHIFT) |
             IS_LEAF_FLAG;
  }

  // A non-phantom and non-leaf node stores the offset to its children and the
  // label of its first child.
  // child() == INVALID_LABEL means that the node has no child.
  uint64_t offset() const {
    return (qword_ >> OFFSET_SHIFT) & OFFSET_MASK;
  }
  uint16_t child() const {
    return (qword_ >> CHILD_SHIFT) & CHILD_MASK;
  }

  void set_offset(uint64_t value) {
    if (qword_ & IS_LEAF_FLAG) {
      qword_ = ((qword_ & ~IS_LEAF_FLAG) & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (value << OFFSET_SHIFT) |
               (uint64_t(INVALID_LABEL) << CHILD_SHIFT);
    } else {
      qword_ = (qword_ & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (value << OFFSET_SHIFT);
    }
  }
  void set_child(uint16_t value) {
    qword_ = (qword_ & ~(CHILD_MASK << CHILD_SHIFT)) |
             (static_cast<uint64_t>(value) << CHILD_SHIFT);
  }

 private:
  uint64_t qword_;

  // 60-63.
  static constexpr uint64_t IS_ORIGIN_FLAG   = uint64_t(1) << 63;
  static constexpr uint64_t IS_PHANTOM_FLAG  = uint64_t(1) << 62;
  static constexpr uint64_t IS_LEAF_FLAG     = uint64_t(1) << 61;
  static constexpr uint64_t HAS_SIBLING_FLAG = uint64_t(1) << 60;

  //  0-17 (is_phantom).
  static constexpr uint64_t NEXT_MASK       = (uint64_t(1) << 9) - 1;
  static constexpr uint64_t PREV_MASK       = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  PREV_SHIFT      = 9;

  //  0- 8 (!is_phantom).
  static constexpr uint64_t LABEL_MASK      = (uint64_t(1) << 9) - 1;

  //  9-59 (!is_phantom && is_leaf)
  static constexpr uint64_t KEY_POS_MASK    = (uint64_t(1) << 39) - 1;
  static constexpr uint8_t  KEY_POS_SHIFT   = 9;
  static constexpr uint64_t KEY_SIZE_MASK   = (uint64_t(1) << 12) - 1;
  static constexpr uint8_t  KEY_SIZE_SHIFT  = 48;

  //  9-59 (!is_phantom && !is_leaf)
  static constexpr uint64_t OFFSET_MASK     = (uint64_t(1) << 42) - 1;
  static constexpr uint8_t  OFFSET_SHIFT    = 9;
  static constexpr uint64_t CHILD_MASK      = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  CHILD_SHIFT     = 51;
};

class Chunk {
 public:
  Chunk() : qwords_{ 0, 0 } {}

  // Chunks in the same level are doubly linked.
  uint64_t next() const {
    return (qwords_[0] >> UPPER_SHIFT) & UPPER_MASK;
  }
  uint64_t prev() const {
    return (qwords_[1] >> UPPER_SHIFT) & UPPER_MASK;
  }

  void set_next(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~(UPPER_MASK << UPPER_SHIFT)) |
                 (value << UPPER_SHIFT);
  }
  void set_prev(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~(UPPER_MASK << UPPER_SHIFT)) |
                 (value << UPPER_SHIFT);
  }

  // The chunk level indicates how easily nodes can be put in this chunk.
  uint64_t level() const {
    return (qwords_[0] >> MIDDLE_SHIFT) & MIDDLE_MASK;
  }
  uint64_t failure_count() const {
    return (qwords_[1] >> MIDDLE_SHIFT) & MIDDLE_MASK;
  }

  void set_level(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~(MIDDLE_MASK << MIDDLE_SHIFT)) |
                 (value << MIDDLE_SHIFT);
  }
  void set_failure_count(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~(MIDDLE_MASK << MIDDLE_SHIFT)) |
                 (value << MIDDLE_SHIFT);
  }

  // The first phantom node and the number of phantom nodes in this chunk.
  uint64_t first_phantom() const {
    return (qwords_[0] >> LOWER_SHIFT) & LOWER_MASK;
  }
  uint64_t num_phantoms() const {
    return (qwords_[1] >> LOWER_SHIFT) & LOWER_MASK;
  }

  void set_first_phantom(uint64_t value) {
    qwords_[0] = (qwords_[0] & ~(LOWER_MASK << LOWER_SHIFT)) |
                 (value << LOWER_SHIFT);
  }
  void set_num_phantoms(uint64_t value) {
    qwords_[1] = (qwords_[1] & ~(LOWER_MASK << LOWER_SHIFT)) |
                 (value << LOWER_SHIFT);
  }

 private:
  uint64_t qwords_[2];

  static constexpr uint64_t UPPER_MASK   = (uint64_t(1) << 44) - 1;
  static constexpr uint8_t  UPPER_SHIFT  = 20;
  static constexpr uint64_t MIDDLE_MASK  = (uint64_t(1) << 10) - 1;
  static constexpr uint8_t  MIDDLE_SHIFT = 10;
  static constexpr uint64_t LOWER_MASK   = (uint64_t(1) << 10) - 1;
  static constexpr uint8_t  LOWER_SHIFT  = 0;
};

class Entry {
 public:
  // Create a valid entry.
  static Entry valid_entry(uint64_t key_pos, size_t key_size) {
    Entry entry;
    entry.qword_ = IS_VALID_FLAG | (key_pos << KEY_POS_SHIFT) | key_size;
    return entry;
  }
  // Create an invalid entry.
  static Entry invalid_entry(uint64_t next) {
    Entry entry;
    entry.qword_ = next;
    return entry;
  }

  // Return true iff "*this" is valid (associated with a key).
  explicit operator bool() const {
    return qword_ & IS_VALID_FLAG;
  }

  // Return the starting address of the associated key.
  // Available iff "*this' is valid.
  uint64_t key_pos() const {
    return (qword_ >> KEY_POS_SHIFT) & KEY_POS_MASK;
  }
  // Return the size of the associated key.
  // Available iff "*this' is valid.
  size_t key_size() const {
    return qword_ & KEY_SIZE_MASK;
  }

  // Return the next invalid entry.
  // Available iff "*this' is invalid.
  uint64_t next() const {
    return qword_;
  }

 private:
  uint64_t qword_;

  static constexpr uint64_t IS_VALID_FLAG = uint64_t(1) << 63;

  static constexpr uint64_t KEY_POS_MASK  = (uint64_t(1) << 39) - 1;
  static constexpr uint8_t  KEY_POS_SHIFT = 12;
  static constexpr uint64_t KEY_SIZE_MASK = (uint64_t(1) << 12) - 1;
};

class Key {
 public:
  Key(int64_t id, const Slice &key)
    : id_low_(static_cast<uint32_t>(id)),
      id_high_(static_cast<uint8_t>(id >> 32)),
      buf_{ '\0', '\0', '\0' } {
    std::memcpy(buf_, key.ptr(), key.size());
  }

  const uint8_t &operator[](size_t i) const {
    return buf_[i];
  }

  int64_t id() const {
    return static_cast<int64_t>(
        id_low_ + (static_cast<uint64_t>(id_high_) << 32));
  }
  const uint8_t *ptr() const {
    return buf_;
  }
  Slice slice(size_t size) const {
    return Slice(buf_, size);
  }

  bool equals_to(const Slice &key, size_t size, size_t offset) const {
    if (size != key.size()) {
      return false;
    }
    for ( ; offset < size; ++offset) {
      if (buf_[offset] != key[offset]) {
        return false;
      }
    }
    return true;
  }

  static uint64_t estimate_size(const size_t key_size) {
    return 2 + (key_size / sizeof(uint32_t));
  }

 private:
  uint32_t id_low_;
  uint8_t id_high_;
  uint8_t buf_[3];
};

class Trie : public da::Trie {
 public:
  ~Trie();

  static Trie *create(const TrieOptions &options, io::Pool pool);
  static Trie *open(io::Pool pool, uint32_t block_id);

  static void unlink(io::Pool pool, uint32_t block_id);

  static da::Trie *defrag(const TrieOptions &options,
                          const basic::Trie &basic_trie, io::Pool pool);

  da::Trie *defrag(const TrieOptions &options);

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
  uint8_t *siblings_;
  Chunk *chunks_;
  Entry *entries_;
  uint32_t *keys_;
  bool initialized_;

  Trie();

  void create_trie(const TrieOptions &options, io::Pool pool);
  void open_trie(io::Pool pool, uint32_t block_id);

  void defrag_trie(const TrieOptions &options, const basic::Trie &trie,
                   io::Pool pool);
  void defrag_trie(const basic::Trie &trie, uint64_t src, uint64_t dest);

  void defrag_trie(const TrieOptions &options, const Trie &trie,
                   io::Pool pool);
  void defrag_trie(const Trie &trie, uint64_t src, uint64_t dest);

  void create_arrays();

  const Key &get_key(uint64_t key_pos) const {
    return *reinterpret_cast<const Key *>(&keys_[key_pos]);
  }

  bool remove_key(const Slice &key);
  bool update_key(int64_t key_id, const Slice &src_key,
                  const Slice &dest_key);

  bool search_leaf(const Slice &key, uint64_t &node_id, size_t &query_pos);
  bool insert_leaf(const Slice &key, uint64_t &node_id, size_t query_pos);

  uint64_t insert_node(uint64_t node_id, uint16_t label);
  uint64_t append_key(const Slice &key, int64_t key_id);

  uint64_t separate(const Slice &key, uint64_t node_id, size_t i);
  void resolve(uint64_t node_id, uint16_t label);
  void migrate_nodes(uint64_t node_id, uint64_t dest_offset,
                     const uint16_t *labels, uint16_t num_labels);

  uint64_t find_offset(const uint16_t *labels, uint16_t num_labels);

  void reserve_node(uint64_t node_id);
  void reserve_chunk(uint64_t chunk_id);

  void update_chunk_level(uint64_t chunk_id, uint64_t level);
  void set_chunk_level(uint64_t chunk_id, uint64_t level);
  void unset_chunk_level(uint64_t chunk_id);
};

}  // namespace large
}  // namespace da
}  // namespace map
}  // namespace grnxx

#endif  // GRNXX_MAP_DA_LARGE_TRIE_HPP
