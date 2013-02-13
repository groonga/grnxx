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
#include "double_array2.hpp"

#include "../db/vector.hpp"
#include "../exception.hpp"
#include "../lock.hpp"
#include "../logger.hpp"

namespace grnxx {
namespace alpha {
namespace {

// FIXME: To be removed in future.
using namespace grnxx::db;

constexpr  int64_t MAX_ID         = 0xFFFFFFFFLL;
constexpr  int64_t INVALID_ID     = -1;
constexpr uint64_t INVALID_OFFSET = 0;

constexpr uint64_t ROOT_NODE_ID   = 0;

constexpr uint16_t TERMINAL_LABEL = 0x100;
constexpr uint16_t MAX_LABEL      = TERMINAL_LABEL;
constexpr uint16_t INVALID_LABEL  = 0x1FF;

constexpr uint64_t CHUNK_SIZE     = 0x200;
constexpr uint64_t CHUNK_MASK     = 0x1FF;

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
constexpr uint64_t INVALID_LEADER    = 0x7FFFFFFFU;

struct Header {
  uint32_t nodes_block_id;
  uint32_t chunks_block_id;
  uint32_t entries_block_id;
  uint32_t keys_block_id;
  uint32_t root_node_id;
  uint64_t total_key_length;
  int64_t next_key_id;
  uint64_t next_key_pos;
  int64_t max_key_id;
  uint64_t num_keys;
  uint64_t num_chunks;
  uint64_t num_phantoms;
  uint64_t num_zombies;
  uint64_t leaders[MAX_CHUNK_LEVEL + 1];
  Mutex inter_process_mutex;

  Header();
};

Header::Header()
  : nodes_block_id(io::BLOCK_INVALID_ID),
    chunks_block_id(io::BLOCK_INVALID_ID),
    entries_block_id(io::BLOCK_INVALID_ID),
    keys_block_id(io::BLOCK_INVALID_ID),
    root_node_id(0),
    total_key_length(0),
    next_key_id(0),
    next_key_pos(0),
    max_key_id(-1),
    num_keys(0),
    num_chunks(0),
    num_phantoms(0),
    num_zombies(0),
    leaders(),
    inter_process_mutex() {
  for (uint32_t i = 0; i <= MAX_CHUNK_LEVEL; ++i) {
    leaders[i] = INVALID_LEADER;
  }
}

class Node {
 public:
  Node() : qword_(IS_PHANTOM_FLAG) {}

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
               (uint64_t(INVALID_LABEL) << CHILD_SHIFT) |
               (uint64_t(INVALID_LABEL) << SIBLING_SHIFT) | INVALID_LABEL;
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
    // 9 bits.
    return qword_ & (IS_PHANTOM_FLAG | LABEL_MASK);
  }
  uint16_t sibling() const {
    // 9 bits.
    return static_cast<uint16_t>((qword_ >> SIBLING_SHIFT) & SIBLING_MASK);
  }

  void set_label(uint16_t value) {
    qword_ = (qword_ & ~LABEL_MASK) | value;
  }
  void set_sibling(uint16_t value) {
    qword_ = (qword_ & ~(SIBLING_MASK << SIBLING_SHIFT)) |
             (static_cast<uint64_t>(value) << SIBLING_SHIFT);
  }

  // A leaf node stores the start position and the length of the associated
  // key.
  uint64_t key_pos() const {
    // 43 bits.
    return (qword_ >> KEY_POS_SHIFT) & KEY_POS_MASK;
  }

  void set_key_pos(uint64_t key_pos) {
    qword_ = (qword_ & ~(KEY_POS_MASK << KEY_POS_SHIFT)) |
             (key_pos << KEY_POS_SHIFT) | IS_LEAF_FLAG;
  }

  // A non-phantom and non-leaf node stores the offset to its children,
  // the label of its next sibling, and the label of its first child.
  uint64_t offset() const {
    // 33 bits.
    return (qword_ >> OFFSET_SHIFT) & OFFSET_MASK;
  }
  uint16_t child() const {
    // 9 bits.
    return static_cast<uint16_t>((qword_ >> CHILD_SHIFT) & CHILD_MASK);
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

  // 61 - 63 (common).
  static constexpr uint64_t IS_ORIGIN_FLAG   = uint64_t(1) << 63;
  static constexpr uint64_t IS_PHANTOM_FLAG  = uint64_t(1) << 62;
  static constexpr uint64_t IS_LEAF_FLAG     = uint64_t(1) << 61;

  // 0 - 17 (phantom).
  static constexpr uint64_t NEXT_MASK  = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  NEXT_SHIFT = 0;
  static constexpr uint64_t PREV_MASK  = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  PREV_SHIFT = 9;

  // 0 - 17 (non-phantom).
  static constexpr uint64_t LABEL_MASK    = (uint64_t(1) << 9) - 1;
  static constexpr uint64_t SIBLING_MASK  = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  SIBLING_SHIFT = 9;

  // 18 - 60 (leaf)
  static constexpr uint64_t KEY_POS_MASK     = (uint64_t(1) << 43) - 1;
  static constexpr uint8_t  KEY_POS_SHIFT    = 18;

  // 18 - 60 (non-leaf)
  static constexpr uint64_t OFFSET_MASK  = (uint64_t(1) << 34) - 1;
  static constexpr uint8_t  OFFSET_SHIFT = 18;
  static constexpr uint64_t CHILD_MASK   = (uint64_t(1) << 9) - 1;
  static constexpr uint8_t  CHILD_SHIFT  = 52;
};

class Chunk {
 public:
  Chunk() : qwords_{ 0, 0 } {}

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

class Entry {
 public:
  Entry() : dword_(0) {}

  // This entry is associated with a key (true) or not (false).
  explicit operator bool() const {
    return dword_ & IS_VALID_FLAG;
  }

  // A valid entry stores the offset and the length of its associated key.
  uint64_t key_pos() const {
    // 31-bit.
    return dword_ & ~IS_VALID_FLAG;
  }

  void set_key_pos(uint64_t value) {
    dword_ = IS_VALID_FLAG | value;
  }

  // An invalid entry stores the index of the next invalid entry.
  uint64_t next() const {
    return dword_;
  }

  void set_next(uint64_t next) {
    dword_ = next;
  }

 private:
  uint32_t dword_;

  static constexpr uint32_t IS_VALID_FLAG = uint32_t(1) << 31;
};

class Key {
 public:
  Key(uint64_t id, const void *address, uint64_t length)
    : id_(id), length_(length), buf_{ '\0', '\0' } {
    std::memcpy(buf_, address, length);
  }

  explicit operator bool() const {
    return id() != INVALID_ID;
  }

  const uint8_t &operator[](uint64_t i) const {
    return buf_[i];
  }

  int64_t id() const {
    return id_;
  }
  uint16_t length() const {
    return length_;
  }
  const void *ptr() const {
    return buf_;
  }

  bool equals_to(const void *ptr, uint64_t length, uint64_t offset = 0) const {
    if (length != length_) {
      return false;
    }
    for ( ; offset < length; ++offset) {
      if (buf_[offset] != static_cast<const uint8_t *>(ptr)[offset]) {
        return false;
      }
    }
    return true;
  }

  static const Key &invalid_key() {
    static const Key invalid_key(INVALID_ID, nullptr, 0);
    return invalid_key;
  }

  static uint64_t estimate_size(uint64_t length) {
    return (9 + length) / sizeof(uint32_t);
  }

 private:
  int32_t id_;
  uint16_t length_;
  uint8_t buf_[2];
};

class Impl : public DoubleArray2 {
 public:
  ~Impl();

  static Impl *create(io::Pool pool);
  static Impl *open(io::Pool pool, uint32_t block_id);

  uint32_t block_id() const {
    return block_info_->id();
  }

  bool search_by_id(int64_t key_id, const void **ptr, uint64_t *length);
  bool search_by_key(const void *ptr, uint64_t length, int64_t *key_id);

  bool insert(const void *ptr, uint64_t length, int64_t *key_id);

  bool remove_by_id(int64_t key_id);
  bool remove_by_key(const void *ptr, uint64_t length);

  bool update_by_id(int64_t key_id, const void *ptr, uint64_t length);
  bool update_by_key(const void *src_ptr, uint64_t src_length,
                     const void *dest_ptr, uint64_t dest_length,
                     int64_t *key_id);

 private:
  io::Pool pool_;
  const io::BlockInfo *block_info_;
  Header *header_;
  Recycler *recycler_;
  Node *nodes_;
  Chunk *chunks_;
  Entry *entries_;
  uint32_t *keys_;
  bool initialized_;

  Impl();

  void create_double_array(io::Pool pool);
  void open_double_array(io::Pool pool, uint32_t block_id);

  const Key &get_key(uint64_t key_pos) {
    return *reinterpret_cast<const Key *>(&keys_[key_pos]);
  }

  bool remove_key(const uint8_t *ptr, uint64_t length);
  bool update_key(const uint8_t *src_ptr, uint64_t src_length,
                  int64_t src_key_id, const uint8_t *dest_ptr,
                  uint64_t dest_length);

  bool search_leaf(const uint8_t *ptr, uint64_t length,
                   uint64_t &node_id, uint64_t &query_pos);

  bool insert_leaf(const uint8_t *ptr, uint64_t length,
                   uint64_t &node_id, uint64_t query_pos);

  uint64_t insert_node(uint64_t node_id, uint16_t label);
  uint64_t append_key(const uint8_t *ptr, uint64_t length, uint64_t key_id);

  uint64_t separate(const uint8_t *ptr, uint64_t length,
                    uint64_t node_id, uint64_t i);
  void resolve(uint64_t node_id, uint16_t label);
  void migrate_nodes(uint64_t node_id, uint64_t dest_offset,
                     const uint16_t *labels, uint16_t num_labels);

  uint64_t find_offset(const uint16_t *labels, uint16_t num_labels);

  void reserve_node(uint64_t node_id);
  void reserve_chunk(uint64_t chunk_id);

  void update_chunk_level(uint64_t chunk_id, uint32_t level);
  void set_chunk_level(uint64_t chunk_id, uint32_t level);
  void unset_chunk_level(uint64_t chunk_id);
};

Impl::~Impl() {
  if (!initialized_) try {
    // Allocated blocks are unlinked if initialization failed.
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

Impl *Impl::create(io::Pool pool) {
  std::unique_ptr<Impl> impl(new (std::nothrow) Impl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::alpha::Impl failed";
    GRNXX_THROW();
  }
  impl->create_double_array(pool);
  return impl.release();
}

Impl *Impl::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Impl> impl(new (std::nothrow) Impl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::alpha::Impl failed";
    GRNXX_THROW();
  }
  impl->open_double_array(pool, block_id);
  return impl.release();
}

bool Impl::search_by_id(int64_t key_id, const void **ptr, uint64_t *length) {
  if (!entries_[key_id]) {
    return false;
  }
  if (ptr || length) {
    const Key &key = get_key(entries_[key_id].key_pos());
    if (ptr) {
      *ptr = key.ptr();
    }
    if (length) {
      *length = key.length();
    }
  }
  return true;
}

bool Impl::search_by_key(const void *ptr, uint64_t length, int64_t *key_id) {
  uint64_t node_id = ROOT_NODE_ID;
  uint64_t query_pos = 0;
  if (!search_leaf(static_cast<const uint8_t *>(ptr), length,
                   node_id, query_pos)) {
    return false;
  }

  // Note that nodes_[node_id] might be updated by other threads/processes.
  const Node node = nodes_[node_id];
  if (!node.is_leaf()) {
    return false;
  }

  const Key &key = get_key(node.key_pos());
  if (key.equals_to(ptr, length, query_pos)) {
    if (key_id) {
      *key_id = key.id();
    }
    return true;
  }
  return false;
}

bool Impl::insert(const void *ptr, uint64_t length, int64_t *key_id) {
  // TODO: Exclusive access control is required.

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, INSERTING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF(!ptr && (length != 0));

  uint64_t node_id = ROOT_NODE_ID;
  uint64_t query_pos = 0;

  search_leaf(static_cast<const uint8_t *>(ptr), length, node_id, query_pos);
  if (!insert_leaf(static_cast<const uint8_t *>(ptr), length,
                   node_id, query_pos)) {
    if (key_id) {
      *key_id = get_key(nodes_[node_id].key_pos()).id();
    }
    return false;
  }

  const int64_t new_key_id = header_->next_key_id;
  const uint64_t new_key_pos =
      append_key(static_cast<const uint8_t *>(ptr), length, new_key_id);

  header_->total_key_length += length;
  ++header_->num_keys;

  // TODO: The first key ID should be fixed to 0 or 1.
  //       Currently, 0 is used.
  if (new_key_id > header_->max_key_id) {
    header_->max_key_id = new_key_id;
    header_->next_key_id = new_key_id + 1;
  } else {
    header_->next_key_id = entries_[new_key_id].next();
  }

  entries_[new_key_id].set_key_pos(new_key_pos);
  nodes_[node_id].set_key_pos(new_key_pos);
  if (key_id) {
    *key_id = new_key_id;
  }
  return true;
}

bool Impl::remove_by_id(int64_t key_id) {
  // TODO: Exclusive access control is required.
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  const Entry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const Key &key = get_key(entry.key_pos());
  return remove_key(static_cast<const uint8_t *>(key.ptr()), key.length());
}

bool Impl::remove_by_key(const void *ptr, uint64_t length) {
  // TODO: Exclusive access control is required.
//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, REMOVING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF((ptr == nullptr) && (length != 0));
  return remove_key(static_cast<const uint8_t *>(ptr), length);
}

bool Impl::update_by_id(int64_t key_id, const void *ptr, uint64_t length) {
  // TODO: Exclusive access control is required.
  if ((key_id < 0) || (key_id > header_->max_key_id)) {
    return false;
  }
  const Entry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const Key &key = get_key(entry.key_pos());
  return update_key(static_cast<const uint8_t *>(key.ptr()), key.length(),
                    key_id, static_cast<const uint8_t *>(ptr), length);
}

bool Impl::update_by_key(const void *src_ptr, uint64_t src_length,
                         const void *dest_ptr, uint64_t dest_length,
                         int64_t *key_id) {
  // TODO: Exclusive access control is required.
  int64_t src_key_id;
  if (!search_by_key(static_cast<const uint8_t *>(src_ptr), src_length,
                     &src_key_id)) {
    return false;
  }
  if (update_key(static_cast<const uint8_t *>(src_ptr), src_length,
                 src_key_id, static_cast<const uint8_t *>(dest_ptr),
                 dest_length)) {
    if (key_id) {
      *key_id = src_key_id;
    }
    return true;
  }
  return false;
}

bool Impl::update_key(const uint8_t *src_ptr, uint64_t src_length,
                      int64_t src_key_id, const uint8_t *dest_ptr,
                      uint64_t dest_length) {
//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, UPDATING_FLAG);

//  GRN_DAT_DEBUG_THROW_IF((ptr == NULL) && (length != 0));

  uint64_t node_id = ROOT_NODE_ID;
  uint64_t query_pos = 0;

  search_leaf(dest_ptr, dest_length, node_id, query_pos);
  if (!insert_leaf(dest_ptr, dest_length, node_id, query_pos)) {
    return false;
  }

  const uint64_t new_key_pos = append_key(dest_ptr, dest_length, src_key_id);
  header_->total_key_length =
      header_->total_key_length + dest_length - src_length;
  entries_[src_key_id].set_key_pos(new_key_pos);
  nodes_[node_id].set_key_pos(new_key_pos);

  node_id = ROOT_NODE_ID;
  query_pos = 0;
  if (!search_leaf(src_ptr, src_length, node_id, query_pos)) {
    // TODO: Unexpected error!
  }
  nodes_[node_id].set_offset(INVALID_OFFSET);
  return true;
}

Impl::Impl()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    recycler_(nullptr),
    nodes_(nullptr),
    chunks_(nullptr),
    entries_(nullptr),
    keys_(nullptr),
    initialized_(false) {}

void Impl::create_double_array(io::Pool pool) {
  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(Header));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<Header *>(block_address);
  *header_ = Header();

  recycler_ = pool_.mutable_recycler();

  // TODO: Tell me the size of buffers!
  const io::BlockInfo *block_info;

  block_info = pool_.create_block(sizeof(Node) * (1 << 27));
  header_->nodes_block_id = block_info->id();
  nodes_ = static_cast<Node *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(Chunk) * (1 << 18));
  header_->chunks_block_id = block_info->id();
  chunks_ = static_cast<Chunk *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(Entry) * (1 << 25));
  header_->entries_block_id = block_info->id();
  entries_ = static_cast<Entry *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(uint32_t) * (1 << 27));
  header_->keys_block_id = block_info->id();
  keys_ = static_cast<uint32_t *>(pool_.get_block_address(*block_info));

  reserve_node(ROOT_NODE_ID);
  nodes_[INVALID_OFFSET].set_is_origin(true);

  initialized_ = true;
}

void Impl::open_double_array(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  initialized_ = true;

  block_info_ = pool_.get_block_info(block_id);

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<Header *>(block_address);

  // TODO: Check the format.

  recycler_ = pool_.mutable_recycler();

  nodes_ = static_cast<Node *>(
      pool_.get_block_address(header_->nodes_block_id));
  chunks_ = static_cast<Chunk *>(
      pool_.get_block_address(header_->chunks_block_id));
  entries_ = static_cast<Entry *>(
      pool_.get_block_address(header_->entries_block_id));
  keys_ = static_cast<uint32_t *>(
      pool_.get_block_address(header_->keys_block_id));
}

bool Impl::remove_key(const uint8_t *ptr, uint64_t length) {
  uint64_t node_id = ROOT_NODE_ID;
  uint64_t query_pos = 0;
  if (!search_leaf(ptr, length, node_id, query_pos)) {
    return false;
  }

  const uint64_t key_pos = nodes_[node_id].key_pos();
  const Key &key = get_key(key_pos);
  if (!key.equals_to(ptr, length, query_pos)) {
    return false;
  }

  const uint64_t key_id = key.id();
  nodes_[node_id].set_offset(INVALID_OFFSET);
  entries_[key_id].set_next(header_->next_key_id);

  header_->next_key_id = key_id;
  header_->total_key_length -= length;
  --header_->num_keys;
  return true;
}

bool Impl::search_leaf(const uint8_t *ptr, uint64_t length,
                       uint64_t &node_id, uint64_t &query_pos) {
  for ( ; query_pos < length; ++query_pos) {
    const Node node = nodes_[node_id];
    if (node.is_leaf()) {
      return true;
    }

    const uint64_t next = node.offset() ^ ptr[query_pos];
    if (nodes_[next].label() != ptr[query_pos]) {
      return false;
    }
    node_id = next;
  }

  const Node node = nodes_[node_id];
  if (node.is_leaf()) {
    return true;
  }

  if (node.child() != TERMINAL_LABEL) {
    return false;
  }
  node_id = node.offset() ^ TERMINAL_LABEL;
  return nodes_[node_id].is_leaf();
}

bool Impl::insert_leaf(const uint8_t *ptr, uint64_t length,
                       uint64_t &node_id, uint64_t query_pos) {
  const Node node = nodes_[node_id];
  if (node.is_leaf()) {
    const Key &key = get_key(node.key_pos());
    uint64_t i = query_pos;
    while ((i < length) && (i < key.length())) {
      if (ptr[i] != key[i]) {
        break;
      }
      ++i;
    }
    if ((i == length) && (i == key.length())) {
      return false;
    }
    // TODO
//    GRN_DAT_THROW_IF(SIZE_ERROR, num_keys() >= max_num_keys());
//    GRN_DAT_DEBUG_THROW_IF(next_key_id() > max_num_keys());

    for (uint64_t j = query_pos; j < i; ++j) {
      node_id = insert_node(node_id, ptr[j]);
    }
    node_id = separate(ptr, length, node_id, i);
    return true;
  } else if (node.label() == TERMINAL_LABEL) {
    return true;
  } else {
    // TODO
//    GRN_DAT_THROW_IF(SIZE_ERROR, num_keys() >= max_num_keys());
    const uint16_t label = (query_pos < length) ?
        static_cast<uint16_t>(ptr[query_pos]) : TERMINAL_LABEL;
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

uint64_t Impl::insert_node(uint64_t node_id, uint16_t label) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(label > MAX_LABEL);

  const Node node = nodes_[node_id];
  uint64_t offset;
  if (node.is_leaf() || (node.offset() == INVALID_OFFSET)) {
    offset = find_offset(&label, 1);
  } else {
    offset = node.offset();
  }

  const uint64_t next = offset ^ label;
  reserve_node(next);

  nodes_[next].set_label(label);
  if (node.is_leaf()) {
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
    nodes_[next].set_key_pos(node.key_pos());
    // TODO: Must be update at once.
  } else if (node.offset() == INVALID_OFFSET) {
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
//  } else {
//    GRN_DAT_DEBUG_THROW_IF(!nodes_[offset].is_origin());
  }
  nodes_[node_id].set_offset(offset);

  const uint16_t child_label = nodes_[node_id].child();
//  GRN_DAT_DEBUG_THROW_IF(child_label == label);
  if (child_label == INVALID_LABEL) {
    nodes_[node_id].set_child(label);
  } else if ((label == TERMINAL_LABEL) ||
             ((child_label != TERMINAL_LABEL) &&
              (label < child_label))) {
    // The next node becomes the first child.
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset ^ child_label).is_phantom());
//    GRN_DAT_DEBUG_THROW_IF(nodes_[offset ^ child_label).label() != child_label);
    nodes_[next].set_sibling(child_label);
    nodes_[node_id].set_child(label);
  } else {
    uint64_t prev = offset ^ child_label;
//    GRN_DAT_DEBUG_THROW_IF(nodes_[prev).label() != child_label);
    uint16_t sibling_label = nodes_[prev].sibling();
    while (label > sibling_label) {
      prev = offset ^ sibling_label;
//      GRN_DAT_DEBUG_THROW_IF(nodes_[prev].label() != sibling_label);
      sibling_label = nodes_[prev].sibling();
    }
//    GRN_DAT_DEBUG_THROW_IF(label == sibling_label);
    nodes_[next].set_sibling(nodes_[prev].sibling());
    nodes_[prev].set_sibling(label);
  }
  return next;
}

uint64_t Impl::append_key(const uint8_t *ptr, uint64_t length,
                                     uint64_t key_id) {
  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, key_id > max_num_keys());

  uint64_t key_pos = header_->next_key_pos;
  const uint64_t key_size = Key::estimate_size(length);

  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, key_size > (key_buf_size() - key_pos));
  new (&keys_[key_pos]) Key(key_id, ptr, length);

  header_->next_key_pos = key_pos + key_size;
  return key_pos;
}

uint64_t Impl::separate(const uint8_t *ptr, uint64_t length,
                        uint64_t node_id, uint64_t i) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(!nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(i > length);

  const Node node = nodes_[node_id];
  const Key &key = get_key(node.key_pos());

  uint16_t labels[2];
  labels[0] = (i < key.length()) ?
      static_cast<uint16_t>(key[i]) : TERMINAL_LABEL;
  labels[1] = (i < length) ?
      static_cast<uint16_t>(ptr[i]) : TERMINAL_LABEL;
//  GRN_DAT_DEBUG_THROW_IF(labels[0] == labels[1]);

  const uint64_t offset = find_offset(labels, 2);

  uint64_t next = offset ^ labels[0];
  reserve_node(next);
//  GRN_DAT_DEBUG_THROW_IF(nodes_[offset).is_origin());

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

void Impl::resolve(uint64_t node_id, uint16_t label) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(label > MAX_LABEL);

  uint64_t offset = nodes_[node_id].offset();
  if (offset != INVALID_OFFSET) {
    uint16_t labels[MAX_LABEL + 1];
    uint16_t num_labels = 0;

    uint16_t next_label = nodes_[node_id].child();
    // FIXME: How to check if the node has children or not.
//    GRN_DAT_DEBUG_THROW_IF(next_label == INVALID_LABEL);
    while (next_label != INVALID_LABEL) {
//      GRN_DAT_DEBUG_THROW_IF(next_label > MAX_LABEL);
      labels[num_labels++] = next_label;
      next_label = nodes_[offset ^ next_label].sibling();
    }
//    GRN_DAT_DEBUG_THROW_IF(num_labels == 0);

    labels[num_labels] = label;
    offset = find_offset(labels, num_labels + 1);
    migrate_nodes(node_id, offset, labels, num_labels);
  } else {
    offset = find_offset(&label, 1);
    if (offset >= (header_->num_chunks * CHUNK_SIZE)) {
//      GRN_DAT_DEBUG_THROW_IF((offset / BLOCK_SIZE) != num_blocks());
      reserve_chunk(header_->num_chunks);
    }
    nodes_[offset].set_is_origin(true);
    nodes_[node_id].set_offset(offset);
  }
}

void Impl::migrate_nodes(uint64_t node_id, uint64_t dest_offset,
                         const uint16_t *labels, uint16_t num_labels) {
//  GRN_DAT_DEBUG_THROW_IF(node_id >= num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
//  GRN_DAT_DEBUG_THROW_IF(labels == nullptr);
//  GRN_DAT_DEBUG_THROW_IF(num_labels == 0);
//  GRN_DAT_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  const uint64_t src_offset = nodes_[node_id].offset();
//  GRN_DAT_DEBUG_THROW_IF(src_offset == INVALID_OFFSET);
//  GRN_DAT_DEBUG_THROW_IF(!nodes_[src_offset].is_origin());

  for (uint16_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
//    GRN_DAT_DEBUG_THROW_IF(ith_node(src_node_id).is_phantom());
//    GRN_DAT_DEBUG_THROW_IF(ith_node(src_node_id).label() != labels[i]);

    reserve_node(dest_node_id);
    Node dest_node = nodes_[src_node_id];
    dest_node.set_is_origin(nodes_[dest_node_id].is_origin());
    nodes_[dest_node_id] = dest_node;
  }
  header_->num_zombies += num_labels;

//  GRN_DAT_DEBUG_THROW_IF(nodes_[dest_offset].is_origin());
  nodes_[dest_offset].set_is_origin(true);
  nodes_[node_id].set_offset(dest_offset);
}

uint64_t Impl::find_offset(const uint16_t *labels, uint16_t num_labels) {
//  GRN_DAT_DEBUG_THROW_IF(labels == nullptr);
//  GRN_DAT_DEBUG_THROW_IF(num_labels == 0);
//  GRN_DAT_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  // Chunks are tested in descending order of level. Basically, lower level
  // chunks contain more phantom nodes.
  uint32_t level = 1;
  while (num_labels >= (1U << level)) {
    ++level;
  }
  level = (level < MAX_CHUNK_LEVEL) ? (MAX_CHUNK_LEVEL - level) : 0;

  uint64_t chunk_count = 0;
  do {
    uint64_t leader = header_->leaders[level];
    if (leader == INVALID_LEADER) {
      // This level group is skipped because it is empty.
      continue;
    }

    uint64_t chunk_id = leader;
    do {
      const Chunk &chunk = chunks_[chunk_id];
//      GRN_DAT_DEBUG_THROW_IF(chunk.level() != level);

      const uint64_t first = (chunk_id * CHUNK_SIZE) | chunk.first_phantom();
      uint64_t node_id = first;
      do {
//        GRN_DAT_DEBUG_THROW_IF(!nodes_[node_id=]).is_phantom());
        const uint64_t offset = node_id ^ labels[0];
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

      const uint64_t prev = chunk_id;
      const uint64_t next = chunk.next();
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

void Impl::reserve_node(uint64_t node_id) {
  if (node_id >= (header_->num_chunks * CHUNK_SIZE)) {
    reserve_chunk(node_id / CHUNK_SIZE);
  }

  Node &node = nodes_[node_id];
//  GRN_DAT_DEBUG_THROW_IF(!node.is_phantom());

  const uint64_t chunk_id = node_id / CHUNK_SIZE;
  Chunk &chunk = chunks_[chunk_id];
//  GRN_DAT_DEBUG_THROW_IF(chunk.num_phantoms() == 0);

  const uint64_t next = (chunk_id * CHUNK_SIZE) | node.next();
  const uint64_t prev = (chunk_id * CHUNK_SIZE) | node.prev();
//  GRN_DAT_DEBUG_THROW_IF(next >= header_->num_nodes());
//  GRN_DAT_DEBUG_THROW_IF(prev >= header_->num_nodes());

  if ((node_id & CHUNK_MASK) == chunk.first_phantom()) {
    // The first phantom node is removed from the chunk and the second phantom
    // node comes first.
    chunk.set_first_phantom(next & CHUNK_MASK);
  }

  nodes_[next].set_prev(prev & CHUNK_MASK);
  nodes_[prev].set_next(next & CHUNK_MASK);

  if (chunk.level() != MAX_CHUNK_LEVEL) {
    const uint64_t threshold =
        uint64_t(1) << ((MAX_CHUNK_LEVEL - chunk.level() - 1) * 2);
    if (chunk.num_phantoms() == threshold) {
      update_chunk_level(chunk_id, chunk.level() + 1);
    }
  }
  chunk.set_num_phantoms(chunk.num_phantoms() - 1);

  node.set_is_phantom(false);

//  GRN_DAT_DEBUG_THROW_IF(node.offset() != INVALID_OFFSET);
//  GRN_DAT_DEBUG_THROW_IF(node.label() != INVALID_LABEL);

  --header_->num_phantoms;
}

void Impl::reserve_chunk(uint64_t chunk_id) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id != num_chunks());
  // TODO
//  GRN_DAT_THROW_IF(SIZE_ERROR, chunk_id >= max_num_chunks());

  header_->num_chunks = chunk_id + 1;
  chunks_[chunk_id].set_failure_count(0);
  chunks_[chunk_id].set_first_phantom(0);
  chunks_[chunk_id].set_num_phantoms(CHUNK_SIZE);

  const uint64_t begin = chunk_id * CHUNK_SIZE;
  const uint64_t end = begin + CHUNK_SIZE;
//  GRN_DAT_DEBUG_THROW_IF(end != num_nodes());

  Node node;
  node.set_is_phantom(true);

  for (uint64_t i = begin; i < end; ++i) {
    node.set_prev((i - 1) & CHUNK_MASK);
    node.set_next((i + 1) & CHUNK_MASK);
    nodes_[i] = node;
  }

  // The level of the new chunk is 0.
  set_chunk_level(chunk_id, 0);
  header_->num_phantoms += CHUNK_SIZE;
}

void Impl::update_chunk_level(uint64_t chunk_id, uint32_t level) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  unset_chunk_level(chunk_id);
  set_chunk_level(chunk_id, level);
}

void Impl::set_chunk_level(uint64_t chunk_id, uint32_t level) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->leaders[level];
  if (leader == INVALID_LEADER) {
    // The chunk becomes the only one member of the level group.
    chunks_[chunk_id].set_next(chunk_id);
    chunks_[chunk_id].set_prev(chunk_id);
    header_->leaders[level] = chunk_id;
  } else {
    // The chunk is appended to the level group, in practice.
    const uint64_t next = leader;
    const uint64_t prev = chunks_[leader].prev();
//    GRN_DAT_DEBUG_THROW_IF(next >= num_chunks());
//    GRN_DAT_DEBUG_THROW_IF(prev >= num_chunks());
    chunks_[chunk_id].set_next(next);
    chunks_[chunk_id].set_prev(prev);
    chunks_[next].set_prev(chunk_id);
    chunks_[prev].set_next(chunk_id);
  }
  chunks_[chunk_id].set_level(level);
  chunks_[chunk_id].set_failure_count(0);
}

void Impl::unset_chunk_level(uint64_t chunk_id) {
//  GRN_DAT_DEBUG_THROW_IF(chunk_id >= num_chunk());

  const uint32_t level = chunks_[chunk_id].level();
//  GRN_DAT_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->leaders[level];
//  GRN_DAT_DEBUG_THROW_IF(leader == INVALID_LEADER);

  const uint64_t next = chunks_[chunk_id].next();
  const uint64_t prev = chunks_[chunk_id].prev();
//  GRN_DAT_DEBUG_THROW_IF(next >= num_chunks());
//  GRN_DAT_DEBUG_THROW_IF(prev >= num_chunks());

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

}  // namespace

DoubleArray2::DoubleArray2() {}
DoubleArray2::~DoubleArray2() {}

DoubleArray2 *DoubleArray2::create(io::Pool pool) {
  return Impl::create(pool);
}

DoubleArray2 *DoubleArray2::open(io::Pool pool, uint32_t block_id) {
  return Impl::open(pool, block_id);
}

}  // namespace alpha
}  // namespace grnxx
