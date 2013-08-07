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
#include "grnxx/map/double_array.hpp"

#include <new>

#include "grnxx/array.hpp"
#include "grnxx/exception.hpp"
#include "grnxx/geo_point.hpp"
#include "grnxx/intrinsic.hpp"
#include "grnxx/logger.hpp"
#include "grnxx/map/common_header.hpp"
#include "grnxx/map/helper.hpp"
#include "grnxx/map/key_pool.hpp"
#include "grnxx/storage.hpp"

namespace grnxx {
namespace map {
namespace {

constexpr char FORMAT_STRING[] = "grnxx::map::DoubleArray";

constexpr uint64_t BLOCK_MAX_FAILURE_COUNT = 4;
constexpr uint64_t BLOCK_MAX_LEVEL         = 5;
constexpr uint64_t BLOCK_INVALID_ID        = (1ULL << 40) - 1;
constexpr uint64_t BLOCK_SIZE              = 1ULL << 9;
constexpr uint64_t BLOCK_MAX_COUNT         = 16;

constexpr uint64_t NODE_TERMINAL_LABEL = 0x100;
constexpr uint64_t NODE_MAX_LABEL      = NODE_TERMINAL_LABEL;
constexpr uint64_t NODE_INVALID_LABEL  = NODE_MAX_LABEL + 1;
constexpr uint64_t NODE_INVALID_OFFSET = 0;

constexpr uint64_t ROOT_NODE_ID = 0;

struct ImplHeader {
  uint32_t nodes_storage_node_id;
  uint32_t siblings_storage_node_id;
  uint32_t blocks_storage_node_id;
  uint64_t num_blocks;
  uint64_t num_phantoms;
  uint64_t num_zombies;
  uint64_t latest_blocks[BLOCK_MAX_LEVEL + 1];

  ImplHeader();
};

ImplHeader::ImplHeader()
    : nodes_storage_node_id(STORAGE_INVALID_NODE_ID),
      siblings_storage_node_id(STORAGE_INVALID_NODE_ID),
      blocks_storage_node_id(STORAGE_INVALID_NODE_ID),
      num_blocks(0),
      num_phantoms(0),
      num_zombies(0),
      latest_blocks() {
  for (uint64_t i = 0; i <= BLOCK_MAX_LEVEL; ++i) {
    latest_blocks[i] = BLOCK_INVALID_ID;
  }
}

// The internal structure of Block is as follows:
// - values_[0]
//    0-15 (16): first_phantom
//   16-23 ( 8): level
//   24-63 (40): next
// - values_[1]
//    0-15 (16): num_phantoms
//   16-23 ( 8): failure_count
//   24-63 (40): prev
// where 0 is the LSB and 63 is the MSB.
class Block {
  // For values_[0].
  static constexpr uint64_t FIRST_PHANTOM_MASK  = (1ULL << 16) - 1;
  static constexpr uint8_t  FIRST_PHANTOM_SHIFT = 0;
  static constexpr uint64_t LEVEL_MASK          = (1ULL << 8) - 1;
  static constexpr uint8_t  LEVEL_SHIFT         = 16;
  static constexpr uint64_t NEXT_MASK           = (1ULL << 40) - 1;
  static constexpr uint8_t  NEXT_SHIFT          = 24;

  // For values_[1].
  static constexpr uint64_t NUM_PHANTOMS_MASK   = (1ULL << 16) - 1;
  static constexpr uint8_t  NUM_PHANTOMS_SHIFT  = 0;
  static constexpr uint64_t FAILURE_COUNT_MASK  = (1ULL << 8) - 1;
  static constexpr uint8_t  FAILURE_COUNT_SHIFT = 16;
  static constexpr uint64_t PREV_MASK           = (1ULL << 40) - 1;
  static constexpr uint8_t  PREV_SHIFT          = 24;

 public:
  static Block empty_block() {
    return Block(0, BLOCK_SIZE << NUM_PHANTOMS_SHIFT);
  }

  // Return the first phantom node.
  uint64_t first_phantom() const {
    return (values_[0] >> FIRST_PHANTOM_SHIFT) & FIRST_PHANTOM_MASK;
  }
  // Return the level.
  uint64_t level() const {
    return (values_[0] >> LEVEL_SHIFT) & LEVEL_MASK;
  }
  // Return the next block ID of the same level.
  uint64_t next() const {
    return (values_[0] >> NEXT_SHIFT) & NEXT_MASK;
  }

  // Return the number of phantom nodes.
  uint64_t num_phantoms() const {
    return (values_[1] >> NUM_PHANTOMS_SHIFT) & NUM_PHANTOMS_MASK;
  }
  // Return the failure count.
  uint64_t failure_count() const {
    return (values_[1] >> FAILURE_COUNT_SHIFT) & FAILURE_COUNT_MASK;
  }
  // Return the previous block ID of the same level.
  uint64_t prev() const {
    return (values_[1] >> PREV_SHIFT) & PREV_MASK;
  }

  void set_first_phantom(uint64_t first_phantom) {
    values_[0] = (values_[0] & ~(FIRST_PHANTOM_MASK << FIRST_PHANTOM_SHIFT)) |
                 ((first_phantom & FIRST_PHANTOM_MASK) << FIRST_PHANTOM_SHIFT);
  }
  void set_level(uint64_t level) {
    values_[0] = (values_[0] & ~(LEVEL_MASK << LEVEL_SHIFT)) |
                 ((level & LEVEL_MASK) << LEVEL_SHIFT);
  }
  void set_next(uint64_t next) {
    values_[0] = (values_[0] & ~(NEXT_MASK << NEXT_SHIFT)) |
                 ((next & NEXT_MASK) << NEXT_SHIFT);
  }

  void set_num_phantoms(uint64_t num_phantoms) {
    values_[1] = (values_[1] & ~(NUM_PHANTOMS_MASK << NUM_PHANTOMS_SHIFT)) |
                 ((num_phantoms & NUM_PHANTOMS_MASK) << NUM_PHANTOMS_SHIFT);
  }
  void set_failure_count(uint64_t failure_count) {
    values_[1] = (values_[1] & ~(FAILURE_COUNT_MASK << FAILURE_COUNT_SHIFT)) |
                 ((failure_count & FAILURE_COUNT_MASK) << FAILURE_COUNT_SHIFT);
  }
  void set_prev(uint64_t prev) {
    values_[1] = (values_[1] & ~(PREV_MASK << PREV_SHIFT)) |
                 ((prev & PREV_MASK) << PREV_SHIFT);
  }

 private:
  uint64_t values_[2];

  Block(uint64_t value_0, uint64_t value_1) : values_{ value_0, value_1 } {}
};

// The internal structure of DoubleArray is as follows:
// - Common
//      62 ( 1): is_phantom
//      63 ( 1): is_origin
// - Phantom: is_phantom
//    0- 8 ( 9): next
//    9-17 ( 9): prev
//   18-61 (44): reserved
// - NonPhantom: !is_phantom
//    0- 8 ( 9): label
//      60 ( 1): has_sibling
//      61 ( 1): is_leaf
// - Leaf: !is_phantom && is_leaf
//    9-48 (40): key_id
//   49-59 (11): reserved
// - NonLeaf: !is_phantom && !is_leaf
//    9-17 ( 9): child
//   18-59 (42): offset
// where 0 is the LSB and 63 is the MSB.
class Node {
  static constexpr uint64_t IS_PHANTOM_FLAG  = 1ULL << 62;
  static constexpr uint64_t IS_ORIGIN_FLAG   = 1ULL << 63;

  static constexpr uint64_t NEXT_MASK        = (1ULL << 9) - 1;
  static constexpr uint8_t  NEXT_SHIFT       = 0;
  static constexpr uint64_t PREV_MASK        = (1ULL << 9) - 1;
  static constexpr uint8_t  PREV_SHIFT       = 9;

  static constexpr uint64_t LABEL_MASK       = (1ULL << 9) - 1;
  static constexpr uint8_t  LABEL_SHIFT      = 0;
  static constexpr uint64_t HAS_SIBLING_FLAG = 1ULL << 60;
  static constexpr uint64_t IS_LEAF_FLAG     = 1ULL << 61;

  static constexpr uint64_t KEY_ID_MASK      = (1ULL << 40) - 1;
  static constexpr uint8_t  KEY_ID_SHIFT     = 9;

  static constexpr uint64_t CHILD_MASK       = (1ULL << 9) - 1;
  static constexpr uint8_t  CHILD_SHIFT      = 9;
  static constexpr uint64_t OFFSET_MASK      = (1ULL << 42) - 1;
  static constexpr uint8_t  OFFSET_SHIFT     = 18;

 public:
  // Create a phantom node.
  static Node phantom_node(uint64_t next, uint64_t prev) {
    return Node(IS_PHANTOM_FLAG | ((next & NEXT_MASK) << NEXT_SHIFT) |
                                  ((prev & PREV_MASK) << PREV_SHIFT));
  }

  // Return true iff this node is a phantom node.
  bool is_phantom() const {
    return value_ & IS_PHANTOM_FLAG;
  }
  // Return true iff the ID of this node is used as an offset.
  bool is_origin() const {
    return value_ & IS_ORIGIN_FLAG;
  }

  // Return the ID of the next phantom node in the same block.
  uint64_t next() const {
    return (value_ >> NEXT_SHIFT) & NEXT_MASK;
  }
  // Return the ID of the prev phantom node in the same block.
  uint64_t prev() const {
    return (value_ >> PREV_SHIFT) & PREV_MASK;
  }

  // Return the label.
  // Note that a phantom node returns an invalid label.
  uint64_t label() const {
    return (value_ >> LABEL_SHIFT) &
           ((IS_PHANTOM_FLAG >> LABEL_SHIFT) | LABEL_MASK);
  }
  // Return true iff this node has a sibling with a greater label.
  bool has_sibling() const {
    return value_ & HAS_SIBLING_FLAG;
  }
  // Return true iff this node is a leaf node.
  bool is_leaf() const {
    return value_ & IS_LEAF_FLAG;
  }

  // Return the associated key ID.
  uint64_t key_id() const {
    return (value_ >> KEY_ID_SHIFT) & KEY_ID_MASK;
  }

  // Return the ID of the child node with the least label.
  uint64_t child() const {
    return (value_ >> CHILD_SHIFT) & CHILD_MASK;
  }
  // Return the offset to child nodes.
  uint64_t offset() const {
    return (value_ >> OFFSET_SHIFT) & OFFSET_MASK;
  }

  void unset_is_phantom() {
    value_ = (value_ & IS_ORIGIN_FLAG) |
             (NODE_INVALID_LABEL << LABEL_SHIFT) |
             (NODE_INVALID_LABEL << CHILD_SHIFT) |
             (NODE_INVALID_OFFSET << OFFSET_SHIFT);
  }
  void set_is_origin(bool is_origin) {
    if (is_origin) {
      value_ |= IS_ORIGIN_FLAG;
    } else {
      value_ &= ~IS_ORIGIN_FLAG;
    }
  }

  void set_next(uint64_t next) {
    value_ = (value_ & ~(NEXT_MASK << NEXT_SHIFT)) |
             ((next & NEXT_MASK) << NEXT_SHIFT);
  }
  void set_prev(uint64_t prev) {
    value_ = (value_ & ~(PREV_MASK << PREV_SHIFT)) |
             ((prev & PREV_MASK) << PREV_SHIFT);
  }
  void set_next_and_prev(uint64_t next, uint64_t prev) {
    constexpr uint64_t NEXT_AND_PREV_MASK =
        (NEXT_MASK << NEXT_SHIFT) | (PREV_MASK << PREV_SHIFT);
    value_ = (value_ & ~NEXT_AND_PREV_MASK) |
             ((next & NEXT_MASK) << NEXT_SHIFT) |
             ((prev & PREV_MASK) << PREV_SHIFT);
  }

  void set_label(uint64_t label) {
    value_ = (value_ & ~(LABEL_MASK << LABEL_SHIFT)) |
             ((label & LABEL_MASK) << LABEL_SHIFT);
  }
  void set_has_sibling() {
    value_ |= HAS_SIBLING_FLAG;
  }
  // set_is_leaf() is not provided because set_key_id() sets IS_LEAF_FLAG.

  void set_key_id(uint64_t key_id) {
    value_ = (value_ & ~(KEY_ID_MASK << KEY_ID_SHIFT)) | IS_LEAF_FLAG |
             ((key_id & KEY_ID_MASK) << KEY_ID_SHIFT);
  }

  void set_child(uint64_t child) {
    value_ = (value_ & ~(CHILD_MASK << CHILD_SHIFT)) |
             ((child & CHILD_MASK) << CHILD_SHIFT);
  }
  void set_offset(uint64_t offset) {
    if (value_ & IS_LEAF_FLAG) {
      value_ = (value_ & ~(IS_LEAF_FLAG | (OFFSET_MASK << OFFSET_SHIFT) |
                           (CHILD_MASK << CHILD_SHIFT))) |
               (offset << OFFSET_SHIFT) |
               (NODE_INVALID_LABEL << CHILD_SHIFT);
    } else {
      value_ = (value_ & ~(OFFSET_MASK << OFFSET_SHIFT)) |
               (offset << OFFSET_SHIFT);
    }
  }

 private:
  uint64_t value_;

  explicit Node(uint64_t value) : value_(value) {}
};

}  // namespace

class DoubleArrayImpl {
  using Header       = ImplHeader;
  using NodeArray    = Array<Node,     65536, 8192>;  // 42-bit
  using SiblingArray = Array<uint8_t, 262144, 4096>;  // 42-bit
  using BlockArray   = Array<Block,     8192, 1024>;  // 33-bit
  using Pool         = KeyPool<Bytes>;

  static constexpr uint64_t NODE_ARRAY_SIZE    = 1ULL << 42;
  static constexpr uint64_t SIBLING_ARRAY_SIZE = 1ULL << 42;
  static constexpr uint64_t BLOCK_ARRAY_SIZE   = 1ULL << 33;

 public:
  using Key = typename Map<Bytes>::Key;
  using KeyArg = typename Map<Bytes>::KeyArg;
  using Cursor = typename Map<Bytes>::Cursor;

  DoubleArrayImpl();
  ~DoubleArrayImpl();

  static DoubleArrayImpl *create(Storage *storage, uint32_t storage_node_id,
                                 const MapOptions &options);
  static DoubleArrayImpl *open(Storage *storage, uint32_t storage_node_id);

  void set_pool(Pool *pool) {
    pool_ = pool;
  }

  uint32_t storage_node_id() const {
    return storage_node_id_;
  }

  int64_t max_key_id() const {
    return pool_->max_key_id();
  }
  uint64_t num_keys() const {
    return pool_->num_keys();
  }

  bool get(int64_t key_id, Key *key = nullptr);
  bool unset(int64_t key_id);
  bool reset(int64_t key_id, KeyArg dest_key);

  bool find(KeyArg key, int64_t *key_id = nullptr);
  bool add(KeyArg key, int64_t *key_id = nullptr);
  bool remove(KeyArg key);
  bool replace(KeyArg src_key, KeyArg dest_key, int64_t *key_id = nullptr);

  void defrag(double usage_rate_threshold);

  void truncate();

  bool find_longest_prefix_match(KeyArg query,
                                 int64_t *key_id = nullptr,
                                 Key *key = nullptr);

//  Cursor *create_cursor(
//      MapCursorAllKeys<Bytes> query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyIDRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());
//  Cursor *create_cursor(
//      const MapCursorKeyRange<Bytes> &query,
//      const MapCursorOptions &options = MapCursorOptions());

 private:
  Storage *storage_;
  uint32_t storage_node_id_;
  Header *header_;
  std::unique_ptr<NodeArray> nodes_;
  std::unique_ptr<SiblingArray> siblings_;
  std::unique_ptr<BlockArray> blocks_;
  Pool *pool_;

  void create_impl(Storage *storage, uint32_t storage_node_id,
                   const MapOptions &options);
  void open_impl(Storage *storage, uint32_t storage_node_id);

  bool replace_key(int64_t key_id, KeyArg src_key, KeyArg dest_key);

  bool find_leaf(KeyArg key, Node **leaf_node, uint64_t *leaf_key_pos);
  bool insert_leaf(KeyArg key, Node *node, uint64_t key_pos, Node **leaf_node);

  Node *insert_node(Node *node, uint64_t label);
  Node *separate(Node *node, uint64_t labels[2]);

  void resolve(Node *node, uint64_t label);
  void migrate_nodes(Node *node, uint64_t dest_offset,
                     const uint64_t *labels, uint64_t num_labels);

  uint64_t find_offset(const uint64_t *labels, uint64_t num_labels);

  Node *reserve_node(uint64_t node_id);
  Block *reserve_block(uint64_t block_id);

  void update_block_level(uint64_t block_id, Block *block, uint64_t level);
  void set_block_level(uint64_t block_id, Block *block, uint64_t level);
  void unset_block_level(uint64_t block_id, Block *block);
};

DoubleArrayImpl::DoubleArrayImpl()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      nodes_(),
      siblings_(),
      blocks_(),
      pool_(nullptr) {}

DoubleArrayImpl::~DoubleArrayImpl() {}

DoubleArrayImpl *DoubleArrayImpl::create(Storage *storage,
                                         uint32_t storage_node_id,
                                         const MapOptions &options) {
  std::unique_ptr<DoubleArrayImpl> impl(new (std::nothrow) DoubleArrayImpl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArrayImpl failed";
    throw MemoryError();
  }
  impl->create_impl(storage, storage_node_id, options);
  return impl.release();
}

DoubleArrayImpl *DoubleArrayImpl::open(Storage *storage,
                                       uint32_t storage_node_id) {
  std::unique_ptr<DoubleArrayImpl> impl(new (std::nothrow) DoubleArrayImpl);
  if (!impl) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArrayImpl failed";
    throw MemoryError();
  }
  impl->open_impl(storage, storage_node_id);
  return impl.release();
}

bool DoubleArrayImpl::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

bool DoubleArrayImpl::unset(int64_t key_id) {
  Key key;
  if (!get(key_id, &key)) {
    // Not found.
    return false;
  }
  return remove(key);
}

bool DoubleArrayImpl::reset(int64_t key_id, KeyArg dest_key) {
  Key src_key;
  if (!get(key_id, &src_key)) {
    // Not found.
    return false;
  }
  return replace(src_key, dest_key);
}

bool DoubleArrayImpl::find(KeyArg key, int64_t *key_id) {
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node.is_leaf()) {
      // Found.
      break;
    }
    node_id = node.offset() ^ key[key_pos];
    node = nodes_->get(node_id);
    if (node.label() != key[key_pos]) {
      // Not found.
      return false;
    }
  }
  if (!node.is_leaf()) {
    if (node.child() != NODE_TERMINAL_LABEL) {
      // Not found.
      return false;
    }
    node_id = node.offset() ^ NODE_TERMINAL_LABEL;
    node = nodes_->get(node_id);
    if (!node.is_leaf()) {
      // Not found.
      return false;
    }
  }
  Key stored_key;
  if (!pool_->get(node.key_id(), &stored_key)) {
    // Not found.
    return false;
  }
  if (key.except_prefix(key_pos) != stored_key.except_prefix(key_pos)) {
    // Not found.
    return false;
  }
  if (key_id) {
    *key_id = node.key_id();
  }
  return true;
}

bool DoubleArrayImpl::add(KeyArg key, int64_t *key_id) {
  Node *node;
  uint64_t key_pos;
  find_leaf(key, &node, &key_pos);
  if (!insert_leaf(key, node, key_pos, &node)) {
    if (key_id) {
      *key_id = node->key_id();
    }
    return false;
  }
  const int64_t next_key_id = pool_->add(key);
  node->set_key_id(next_key_id);
  if (key_id) {
    *key_id = next_key_id;
  }
  return true;
}

bool DoubleArrayImpl::remove(KeyArg key) {
  Node *node;
  uint64_t key_pos;
  if (!find_leaf(key, &node, &key_pos)) {
    // Not found.
    return false;
  }
  Key stored_key;
  if (!pool_->get(node->key_id(), &stored_key)) {
    // Not found.
    return false;
  }
  if (key.except_prefix(key_pos) != stored_key.except_prefix(key_pos)) {
    // Not found.
    return false;
  }
  pool_->unset(node->key_id());
  node->set_offset(NODE_INVALID_OFFSET);
  return true;
}

bool DoubleArrayImpl::replace(KeyArg src_key, KeyArg dest_key,
                                 int64_t *key_id) {
  int64_t src_key_id;
  if (!find(src_key, &src_key_id)) {
    // Not found.
    return false;
  }
  if (!replace_key(src_key_id, src_key, dest_key)) {
    // Found.
    return false;
  }
  if (key_id) {
    *key_id = src_key_id;
  }
  return true;
}

void DoubleArrayImpl::defrag(double usage_rate_threshold) {
  // TODO
  pool_->defrag(usage_rate_threshold);
}

void DoubleArrayImpl::truncate() {
  // TODO: How to recycle nodes.
  Node * const node = &nodes_->get_value(ROOT_NODE_ID);
  node->set_child(NODE_INVALID_LABEL);
  node->set_offset(NODE_INVALID_OFFSET);
  pool_->truncate();
}

bool DoubleArrayImpl::find_longest_prefix_match(KeyArg query,
                                                   int64_t *key_id,
                                                   Key *key) {
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  Node node = nodes_->get(node_id);
  uint64_t query_pos;
  for (query_pos = 0; query_pos < query.size(); ++query_pos) {
    if (node.is_leaf()) {
      Key stored_key;
      if (pool_->get(node.key_id(), &stored_key)) {
        if ((stored_key.size() <= query.size()) &&
            (stored_key.except_prefix(query_pos) ==
             query.prefix(stored_key.size()).except_prefix(query_pos))) {
          if (key_id) {
            *key_id = node.key_id();
          }
          if (key) {
            *key = stored_key;
          }
          found = true;
        }
      }
      return found;
    }

    if (node.child() == NODE_TERMINAL_LABEL) {
      Node leaf_node = nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL);
      if (leaf_node.is_leaf()) {
        if (pool_->get(leaf_node.key_id(), key)) {
          if (key_id) {
            *key_id = leaf_node.key_id();
          }
          found = true;
        }
      }
    }

    node_id = node.offset() ^ query[query_pos];
    node = nodes_->get(node_id);
    if (node.label() != query[query_pos]) {
      return found;
    }
  }

  if (node.is_leaf()) {
    Key stored_key;
    if (pool_->get(node.key_id(), &stored_key)) {
      if (stored_key.size() <= query.size()) {
        if (key_id) {
          *key_id = node.key_id();
        }
        if (key) {
          *key = stored_key;
        }
        found = true;
      }
    }
  } else if (node.child() == NODE_TERMINAL_LABEL) {
    node = nodes_->get(node.offset() ^ NODE_TERMINAL_LABEL);
    if (pool_->get(node.key_id(), key)) {
      if (key_id) {
        *key_id = node.key_id();
      }
      found = true;
    }
  }
  return found;
}

void DoubleArrayImpl::create_impl(Storage *storage, uint32_t storage_node_id,
                                  const MapOptions &) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    nodes_.reset(NodeArray::create(storage, storage_node_id_,
                                   NODE_ARRAY_SIZE));
    siblings_.reset(SiblingArray::create(storage, storage_node_id_,
                                         SIBLING_ARRAY_SIZE));
    blocks_.reset(BlockArray::create(storage, storage_node_id_,
                                     BLOCK_ARRAY_SIZE));
    header_->nodes_storage_node_id = nodes_->storage_node_id();
    header_->siblings_storage_node_id = siblings_->storage_node_id();
    header_->blocks_storage_node_id = blocks_->storage_node_id();
    Node * const root_node = reserve_node(ROOT_NODE_ID);
    root_node[NODE_INVALID_OFFSET - ROOT_NODE_ID].set_is_origin(true);
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void DoubleArrayImpl::open_impl(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  storage_node_id_ = storage_node_id;
  StorageNode storage_node = storage->open_node(storage_node_id_);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  header_ = static_cast<Header *>(storage_node.body());
  nodes_.reset(NodeArray::open(storage, header_->nodes_storage_node_id));
  siblings_.reset(
      SiblingArray::open(storage, header_->siblings_storage_node_id));
  blocks_.reset(BlockArray::open(storage, header_->blocks_storage_node_id));
}

bool DoubleArrayImpl::replace_key(int64_t key_id, KeyArg src_key,
                                  KeyArg dest_key) {
  Node *dest_node;
  uint64_t key_pos;
  find_leaf(dest_key, &dest_node, &key_pos);
  if (!insert_leaf(dest_key, dest_node, key_pos, &dest_node)) {
    return false;
  }
  Node *src_node;
  if (!find_leaf(src_key, &src_node, &key_pos)) {
    // Critical error.
    GRNXX_ERROR() << "not found: src_key = " << src_key;
    throw LogicError();
  }
  pool_->reset(key_id, dest_key);
  dest_node->set_key_id(key_id);
  src_node->set_offset(NODE_INVALID_OFFSET);
  return true;
}

bool DoubleArrayImpl::find_leaf(KeyArg key, Node **leaf_node,
                                uint64_t *leaf_key_pos) {
  Node *node = &nodes_->get_value(ROOT_NODE_ID);
  uint64_t key_pos;
  for (key_pos = 0; key_pos < key.size(); ++key_pos) {
    if (node->is_leaf()) {
      // Found.
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return true;
    }
    const uint64_t child_node_id = node->offset() ^ key[key_pos];
    Node * const child_node = &nodes_->get_value(child_node_id);
    if (child_node->label() != key[key_pos]) {
      // Not found.
      *leaf_node = node;
      *leaf_key_pos = key_pos;
      return false;
    }
    node = child_node;
  }
  *leaf_node = node;
  *leaf_key_pos = key_pos;
  if (node->is_leaf()) {
    // Found.
    return true;
  }
  if (node->child() != NODE_TERMINAL_LABEL) {
    // Not found.
    return false;
  }
  const uint64_t node_id = node->offset() ^ NODE_TERMINAL_LABEL;
  node = &nodes_->get_value(node_id);
  *leaf_node = node;
  return node->is_leaf();
}

bool DoubleArrayImpl::insert_leaf(KeyArg key, Node *node,
                                  uint64_t key_pos, Node **leaf_node) {
  if (node->is_leaf()) {
    Key stored_key;
    if (!pool_->get(node->key_id(), &stored_key)) {
      GRNXX_ERROR() << "not found: key = " << key << ", key_pos = " << key_pos;
      throw LogicError();
    }
    uint64_t i = key_pos;
    while ((i < key.size()) && (i < stored_key.size())) {
      if (key[i] != stored_key[i]) {
        break;
      }
      ++i;
    }
    if ((i == key.size()) && (i == stored_key.size())) {
      return false;
    }
    while (key_pos < i) {
      node = insert_node(node, key[key_pos]);
      ++key_pos;
    }
    uint64_t labels[2];
    labels[0] = (key_pos < stored_key.size()) ?
        stored_key[key_pos] : NODE_TERMINAL_LABEL;
    labels[1] = (key_pos < key.size()) ? key[key_pos] : NODE_TERMINAL_LABEL;
    *leaf_node = separate(node, labels);
    return true;
  } else if (node->label() == NODE_TERMINAL_LABEL) {
    *leaf_node = node;
    return true;
  } else {
    const uint64_t label = (key_pos < key.size()) ?
        key[key_pos] : NODE_TERMINAL_LABEL;
    resolve(node, label);
    *leaf_node = insert_node(node, label);
    return true;
  }
}

Node *DoubleArrayImpl::insert_node(Node *node, uint64_t label) {
  uint64_t offset = node->offset();
  if (node->is_leaf() || (offset == NODE_INVALID_OFFSET)) {
    offset = find_offset(&label, 1);
  }
  const uint64_t next_node_id = offset ^ label;
  uint8_t *next_sibling = &siblings_->get_value(next_node_id);
  Node * const next_node = reserve_node(next_node_id);
  next_node->set_label(label);
  if (node->is_leaf()) {
    next_node[offset - next_node_id].set_is_origin(true);
    next_node->set_key_id(node->key_id());
  } else if (node->offset() == NODE_INVALID_OFFSET) {
    next_node[offset - next_node_id].set_is_origin(true);
  }
  node->set_offset(offset);
  const uint64_t child_label = node->child();
  if (child_label == NODE_INVALID_LABEL) {
    node->set_child(label);
  } else if ((label == NODE_TERMINAL_LABEL) ||
             ((child_label != NODE_TERMINAL_LABEL) && (label < child_label))) {
    // The child node becomes the first child.
    *next_sibling = child_label;
    next_node->set_has_sibling();
    node->set_child(label);
  } else {
    uint64_t prev_node_id = offset ^ child_label;
    Node *prev_node = &next_node[prev_node_id - next_node_id];
    uint8_t *prev_sibling = &next_sibling[prev_node_id - next_node_id];
    uint64_t sibling_label = prev_node->has_sibling() ?
        *prev_sibling : NODE_INVALID_LABEL;
    while (label > sibling_label) {
      prev_node_id = offset ^ sibling_label;
      prev_node = &next_node[prev_node_id - next_node_id];
      prev_sibling = &next_sibling[prev_node_id - next_node_id];
      sibling_label = prev_node->has_sibling() ?
          *prev_sibling : NODE_INVALID_LABEL;
    }
    *next_sibling = *prev_sibling;
    *prev_sibling = label;
    if (prev_node->has_sibling()) {
      next_node->set_has_sibling();
    }
    prev_node->set_has_sibling();
  }
  return next_node;
}

Node *DoubleArrayImpl::separate(Node *node, uint64_t labels[2]) {
  const uint64_t offset = find_offset(labels, 2);
  uint64_t node_ids[2] = { offset ^ labels[0], offset ^ labels[1] };
  Node *nodes[2];
  nodes[0] = reserve_node(node_ids[0]);
  nodes[1] = reserve_node(node_ids[1]);
  uint8_t * const sibling_block =
      &siblings_->get_value(offset & ~(BLOCK_SIZE - 1));
  nodes[0]->set_label(labels[0]);
  nodes[0]->set_key_id(node->key_id());
  nodes[1]->set_label(labels[1]);
  nodes[0][offset - node_ids[0]].set_is_origin(true);
  node->set_offset(offset);
  if ((labels[0] == NODE_TERMINAL_LABEL) ||
      ((labels[1] != NODE_TERMINAL_LABEL) && (labels[0] < labels[1]))) {
    sibling_block[node_ids[0] % BLOCK_SIZE] = static_cast<uint8_t>(labels[1]);
    nodes[0]->set_has_sibling();
    node->set_child(labels[0]);
  } else {
    sibling_block[node_ids[1] % BLOCK_SIZE] = static_cast<uint8_t>(labels[0]);
    nodes[1]->set_has_sibling();
    node->set_child(labels[1]);
  }
  return nodes[1];
}

void DoubleArrayImpl::resolve(Node *node, uint64_t label) {
  uint64_t offset = node->offset();
  if (offset == NODE_INVALID_OFFSET) {
    return;
  }
  uint64_t dest_node_id = offset ^ label;
  Node * const dest_node = &nodes_->get_value(dest_node_id);
  if (dest_node->is_phantom()) {
    return;
  }
  Node * const node_block = dest_node - (dest_node_id % BLOCK_SIZE);
  uint8_t * const sibling_block =
      &siblings_->get_value(dest_node_id & ~(BLOCK_SIZE - 1));
  uint64_t labels[NODE_MAX_LABEL + 1];
  uint64_t num_labels = 0;
  uint64_t child_label = node->child();
  while (child_label != NODE_INVALID_LABEL) {
    labels[num_labels++] = child_label;
    const uint64_t child_node_id = offset ^ child_label;
    if (node_block[child_node_id % BLOCK_SIZE].has_sibling()) {
      child_label = sibling_block[child_node_id % BLOCK_SIZE];
    } else {
      child_label = NODE_INVALID_LABEL;
    }
  }
  labels[num_labels] = label;
  offset = find_offset(labels, num_labels + 1);
  migrate_nodes(node, offset, labels, num_labels);
}

void DoubleArrayImpl::migrate_nodes(Node *node, uint64_t dest_offset,
                                    const uint64_t *labels,
                                    uint64_t num_labels) {
  const uint64_t src_offset = node->offset();
  Node * const src_node_block =
      &nodes_->get_value(src_offset & ~(BLOCK_SIZE - 1));
  uint8_t * const src_sibling_block =
      &siblings_->get_value(src_offset & ~(BLOCK_SIZE - 1));
  Node * const dest_node_block =
      &nodes_->get_value(dest_offset & ~(BLOCK_SIZE - 1));
  uint8_t * const dest_sibling_block =
      &siblings_->get_value(dest_offset & ~(BLOCK_SIZE - 1));
  for (uint64_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    Node * const src_node = &src_node_block[src_node_id % BLOCK_SIZE];
    uint8_t * const src_sibling = &src_sibling_block[src_node_id % BLOCK_SIZE];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
    Node * const dest_node = reserve_node(dest_node_id);
    uint8_t * const dest_sibling =
        &dest_sibling_block[dest_node_id % BLOCK_SIZE];
    Node dummy_node = *src_node;
    dummy_node.set_is_origin(dest_node->is_origin());
    *dest_node = dummy_node;
    *dest_sibling = *src_sibling;
  }
  header_->num_zombies += num_labels;
  dest_node_block[dest_offset % BLOCK_SIZE].set_is_origin(true);
  node->set_offset(dest_offset);
}

uint64_t DoubleArrayImpl::find_offset(const uint64_t *labels,
                                      uint64_t num_labels) {
  // Blocks are tested in descending order of level.
  // Generally, a lower level contains more phantom nodes.
  uint64_t level = bit_scan_reverse(num_labels) + 1;
  level = (level < BLOCK_MAX_LEVEL) ? (BLOCK_MAX_LEVEL - level) : 0;
  uint64_t block_count = 0;
  do {
    uint64_t latest_block_id = header_->latest_blocks[level];
    if (latest_block_id == BLOCK_INVALID_ID) {
      // This level group is skipped because it is empty.
      continue;
    }
    uint64_t block_id = latest_block_id;
    do {
      Block * const block = &blocks_->get_value(block_id);
      Node * const node_block = &nodes_->get_value(block_id * BLOCK_SIZE);
      const uint64_t first_phantom_node_id = block->first_phantom();
      uint64_t node_id = first_phantom_node_id;
      do {
        const uint64_t offset = node_id ^ labels[0];
        if (!node_block[offset].is_origin()) {
          uint64_t i;
          for (i = 1; i < num_labels; ++i) {
            if (!node_block[offset ^ labels[i]].is_phantom()) {
              break;
            }
          }
          if (i >= num_labels) {
            // Found.
            return (block_id * BLOCK_SIZE) | offset;
          }
        }
        node_id = node_block[node_id].next();
      } while (node_id != first_phantom_node_id);

      Block * const prev_block = block;
      const uint64_t prev_block_id = block_id;
      const uint64_t next_block_id = block->next();
      block_id = next_block_id;

      // A block level is updated if this function fails many times.
      prev_block->set_failure_count(prev_block->failure_count() + 1);
      if (prev_block->failure_count() >= BLOCK_MAX_FAILURE_COUNT) {
        update_block_level(prev_block_id, prev_block, level + 1);
        if (next_block_id == latest_block_id) {
          // All the blocks are tested.
          break;
        } else {
          latest_block_id = header_->latest_blocks[level];
          continue;
        }
      }
    } while ((++block_count < BLOCK_MAX_COUNT) &&
             (block_id != latest_block_id));
  } while ((block_count < BLOCK_MAX_COUNT) && (level-- != 0));
  // Use a new block.
  return (header_->num_blocks * BLOCK_SIZE) ^ labels[0];
}

Node *DoubleArrayImpl::reserve_node(uint64_t node_id) {
  const uint64_t block_id = node_id / BLOCK_SIZE;
  Block *block;
  if (node_id >= (header_->num_blocks * BLOCK_SIZE)) {
    block = reserve_block(block_id);
  } else {
    block = &blocks_->get_value(block_id);
  }
  Node * const node = &nodes_->get_value(node_id);
  Node * const node_block = node - (node_id % BLOCK_SIZE);
  Node * const next_node = &node_block[node->next()];
  Node * const prev_node = &node_block[node->prev()];
  if ((node_id % BLOCK_SIZE) == block->first_phantom()) {
    block->set_first_phantom(node->next());
  }
  prev_node->set_next(node->next());
  next_node->set_prev(node->prev());
  if (block->level() != BLOCK_MAX_LEVEL) {
    const uint64_t threshold =
        1ULL << ((BLOCK_MAX_LEVEL - block->level() - 1) * 2);
    if (block->num_phantoms() == threshold) {
      update_block_level(block_id, block, block->level() + 1);
    }
  }
  block->set_num_phantoms(block->num_phantoms() - 1);
  node->unset_is_phantom();
  --header_->num_phantoms;
  return node;
}

Block *DoubleArrayImpl::reserve_block(uint64_t block_id) {
  if (block_id >= blocks_->size()) {
    GRNXX_ERROR() << "too many blocks: block_id = " << block_id
                  << ", max_block_id = " << (blocks_->size() - 1);
    throw LogicError();
  }
  Block * const block = &blocks_->get_value(block_id);
  Node * const node = &nodes_->get_value(block_id * BLOCK_SIZE);
  *block = Block::empty_block();
  for (uint64_t i = 0; i < BLOCK_SIZE; ++i) {
    node[i] = Node::phantom_node(i + 1, i - 1);
  }
  // The level of a new block is 0.
  set_block_level(block_id, block, 0);
  header_->num_blocks = block_id + 1;
  header_->num_phantoms += BLOCK_SIZE;
  return block;
}

void DoubleArrayImpl::update_block_level(uint64_t block_id, Block *block,
                                         uint64_t level) {
  // FIXME: If set_block_level() fails, the block gets lost.
  unset_block_level(block_id, block);
  set_block_level(block_id, block, level);
}

void DoubleArrayImpl::set_block_level(uint64_t block_id, Block *block,
                                      uint64_t level) {
  if (header_->latest_blocks[level] == BLOCK_INVALID_ID) {
    // The block becomes the only one member of the level group.
    block->set_next(block_id);
    block->set_prev(block_id);
    header_->latest_blocks[level] = block_id;
  } else {
    // The block is appended to the level group.
    const uint64_t next_block_id = header_->latest_blocks[level];
    Block * const next_block = &blocks_->get_value(next_block_id);
    const uint64_t prev_block_id = next_block->prev();
    Block * const prev_block = &blocks_->get_value(prev_block_id);
    block->set_next(next_block_id);
    block->set_prev(prev_block_id);
    prev_block->set_next(block_id);
    next_block->set_prev(block_id);
  }
  block->set_level(level);
  block->set_failure_count(0);
}

void DoubleArrayImpl::unset_block_level(uint64_t block_id, Block *block) {
  const uint64_t level = block->level();
  const uint64_t next_block_id = block->next();
  const uint64_t prev_block_id = block->prev();
  if (next_block_id == prev_block_id) {
    // The level group becomes empty.
    header_->latest_blocks[level] = BLOCK_INVALID_ID;
  } else {
    Block * const next_block = &blocks_->get_value(next_block_id);
    Block * const prev_block = &blocks_->get_value(prev_block_id);
    prev_block->set_next(next_block_id);
    next_block->set_prev(prev_block_id);
    if (block_id == header_->latest_blocks[level]) {
      // The next block becomes the latest block of the level group.
      header_->latest_blocks[level] = next_block_id;
    }
  }
}

struct DoubleArrayHeader {
  CommonHeader common_header;
  uint32_t impl_storage_node_id;
  uint32_t pool_storage_node_id;

  // Initialize the member variables.
  DoubleArrayHeader();

  // Return true iff the header seems to be correct.
  explicit operator bool() const;
};

DoubleArrayHeader::DoubleArrayHeader()
    : common_header(FORMAT_STRING, MAP_DOUBLE_ARRAY),
      impl_storage_node_id(STORAGE_INVALID_NODE_ID),
      pool_storage_node_id(STORAGE_INVALID_NODE_ID) {}

DoubleArrayHeader::operator bool() const {
  return common_header.format() == FORMAT_STRING;
}

template <typename T>
Map<T> *DoubleArray<T>::create(Storage *, uint32_t, const MapOptions &) {
  GRNXX_ERROR() << "unsupported type";
  throw LogicError();
}

template <typename T>
Map<T> *DoubleArray<T>::open(Storage *, uint32_t) {
  GRNXX_ERROR() << "unsupported type";
  throw LogicError();
}

template class DoubleArray<int8_t>;
template class DoubleArray<uint8_t>;
template class DoubleArray<int16_t>;
template class DoubleArray<uint16_t>;
template class DoubleArray<int32_t>;
template class DoubleArray<uint32_t>;
template class DoubleArray<int64_t>;
template class DoubleArray<uint64_t>;
template class DoubleArray<double>;
template class DoubleArray<GeoPoint>;

DoubleArray<Bytes>::DoubleArray()
    : storage_(nullptr),
      storage_node_id_(STORAGE_INVALID_NODE_ID),
      header_(nullptr),
      impl_(),
      pool_() {}

DoubleArray<Bytes>::~DoubleArray() {}

DoubleArray<Bytes> *DoubleArray<Bytes>::create(Storage *storage,
                                               uint32_t storage_node_id,
                                               const MapOptions &options) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    throw MemoryError();
  }
  map->create_map(storage, storage_node_id, options);
  return map.release();
}

DoubleArray<Bytes> *DoubleArray<Bytes>::open(Storage *storage,
                                             uint32_t storage_node_id) {
  std::unique_ptr<DoubleArray> map(new (std::nothrow) DoubleArray);
  if (!map) {
    GRNXX_ERROR() << "new grnxx::map::DoubleArray failed";
    throw MemoryError();
  }
  map->open_map(storage, storage_node_id);
  return map.release();
}

uint32_t DoubleArray<Bytes>::storage_node_id() const {
  return storage_node_id_;
}

MapType DoubleArray<Bytes>::type() const {
  return MAP_DOUBLE_ARRAY;
}

int64_t DoubleArray<Bytes>::max_key_id() const {
  return pool_->max_key_id();
}

uint64_t DoubleArray<Bytes>::num_keys() const {
  return pool_->num_keys();
}

bool DoubleArray<Bytes>::get(int64_t key_id, Key *key) {
  if ((key_id < MAP_MIN_KEY_ID) || (key_id > max_key_id())) {
    // Out of range.
    return false;
  }
  return pool_->get(key_id, key);
}

bool DoubleArray<Bytes>::unset(int64_t key_id) {
  return impl_->unset(key_id);
}

bool DoubleArray<Bytes>::reset(int64_t key_id, KeyArg dest_key) {
  return impl_->reset(key_id, dest_key);
}

bool DoubleArray<Bytes>::find(KeyArg key, int64_t *key_id) {
  return impl_->find(key, key_id);
}

bool DoubleArray<Bytes>::add(KeyArg key, int64_t *key_id) {
  return impl_->add(key, key_id);
}

bool DoubleArray<Bytes>::remove(KeyArg key) {
  return impl_->remove(key);
}

bool DoubleArray<Bytes>::replace(KeyArg src_key, KeyArg dest_key,
                                 int64_t *key_id) {
  return impl_->replace(src_key, dest_key, key_id);
}

void DoubleArray<Bytes>::defrag(double usage_rate_threshold) {
  // TODO
  impl_->defrag(usage_rate_threshold);
}

void DoubleArray<Bytes>::truncate() {
  // TODO
  impl_->truncate();
}

bool DoubleArray<Bytes>::find_longest_prefix_match(KeyArg query,
                                                   int64_t *key_id,
                                                   Key *key) {
  return impl_->find_longest_prefix_match(query, key_id, key);
}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    MapCursorAllKeys<Bytes> query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    const MapCursorKeyIDRange<Bytes> &query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

//MapCursor<Bytes> *DoubleArray<Bytes>::create_cursor(
//    const MapCursorKeyRange<Bytes> &query, const MapCursorOptions &options) {
//  // TODO
//  return nullptr;
//}

void DoubleArray<Bytes>::create_map(Storage *storage, uint32_t storage_node_id,
                                    const MapOptions &options) {
  storage_ = storage;
  StorageNode storage_node =
      storage->create_node(storage_node_id, sizeof(Header));
  storage_node_id_ = storage_node.id();
  try {
    header_ = static_cast<Header *>(storage_node.body());
    *header_ = Header();
    impl_.reset(Impl::create(storage, storage_node_id_, options));
    pool_.reset(Pool::create(storage, storage_node_id_));
    impl_->set_pool(pool_.get());
    header_->impl_storage_node_id = impl_->storage_node_id();
    header_->pool_storage_node_id = pool_->storage_node_id();
  } catch (...) {
    storage->unlink_node(storage_node_id_);
    throw;
  }
}

void DoubleArray<Bytes>::open_map(Storage *storage, uint32_t storage_node_id) {
  storage_ = storage;
  storage_node_id_ = storage_node_id;
  StorageNode storage_node = storage->open_node(storage_node_id_);
  if (storage_node.size() < sizeof(Header)) {
    GRNXX_ERROR() << "invalid format: size = " << storage_node.size()
                  << ", header_size = " << sizeof(Header);
    throw LogicError();
  }
  header_ = static_cast<Header *>(storage_node.body());
  if (!*header_) {
    GRNXX_ERROR() << "wrong format: expected = " << FORMAT_STRING
                  << ", actual = " << header_->common_header.format();
    throw LogicError();
  }
  impl_.reset(Impl::open(storage, header_->impl_storage_node_id));
  pool_.reset(Pool::open(storage, header_->pool_storage_node_id));
  impl_->set_pool(pool_.get());
}

}  // namespace map
}  // namespace grnxx
