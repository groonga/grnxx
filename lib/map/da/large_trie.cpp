#include "large_trie.hpp"

#include "../../lock.hpp"
#include "../../logger.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace large {

Header::Header()
  : nodes_block_id(io::BLOCK_INVALID_ID),
    siblings_block_id(io::BLOCK_INVALID_ID),
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
    inter_process_mutex() {
  for (uint64_t i = 0; i <= MAX_CHUNK_LEVEL; ++i) {
    leaders[i] = INVALID_LEADER;
  }
}

Trie::~Trie() {
  if (!initialized_) try {
    // Free allocated blocks if initialization failed.
    if (header_->nodes_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->nodes_block_id);
    }
    if (header_->siblings_block_id != io::BLOCK_INVALID_ID) {
      pool_.free_block(header_->siblings_block_id);
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

Trie *Trie::create(const TrieOptions &options, io::Pool pool) {
  std::unique_ptr<Trie> trie(new (std::nothrow) Trie);
  if (!trie) {
    GRNXX_ERROR() << "new grnxx::map::Trie failed";
    GRNXX_THROW();
  }
  trie->create_trie(options, pool);
  return trie.release();
}

Trie *Trie::open(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Trie> trie(new (std::nothrow) Trie);
  if (!trie) {
    GRNXX_ERROR() << "new grnxx::map::Trie failed";
    GRNXX_THROW();
  }
  trie->open_trie(pool, block_id);
  return trie.release();
}

void Trie::unlink(io::Pool pool, uint32_t block_id) {
  std::unique_ptr<Trie> trie(Trie::open(pool, block_id));

  pool.free_block(trie->header_->nodes_block_id);
  pool.free_block(trie->header_->siblings_block_id);
  pool.free_block(trie->header_->chunks_block_id);
  pool.free_block(trie->header_->entries_block_id);
  pool.free_block(trie->header_->keys_block_id);
  pool.free_block(*trie->block_info_);
}

Trie *Trie::defrag(const TrieOptions &options) {
  std::unique_ptr<Trie> trie(new (std::nothrow) Trie);
  if (!trie) {
    GRNXX_ERROR() << "new grnxx::map::Trie failed";
    GRNXX_THROW();
  }
  trie->defrag_trie(options, *this, pool_);
  return trie.release();
}

uint32_t Trie::block_id() const {
  return block_info_->id();
}

bool Trie::search(int64_t key_id, Slice *key) {
  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }

  const Entry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  if (key) {
    const Key &found_key = get_key(entry.key_pos());
    *key = found_key.slice(entry.key_size());
  }
  return true;
}

bool Trie::search(const Slice &key, int64_t *key_id) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    return false;
  }

  uint64_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;
  if (!search_leaf(key, node_id, query_pos)) {
    return false;
  }

  // Note that nodes_[node_id] might be updated by other threads/processes.
  const Node node = nodes_[node_id];
  if (!node.is_leaf()) {
    return false;
  }

  const Key &found_key = get_key(node.key_pos());
  if (found_key.equals_to(key, node.key_size(), query_pos)) {
    if (key_id) {
      *key_id = found_key.id();
    }
    return true;
  }
  return false;
}

bool Trie::lcp_search(const Slice &query, int64_t *key_id, Slice *key) {
  bool found = false;
  uint64_t node_id = ROOT_NODE_ID;
  uint64_t query_pos = 0;

  for ( ; query_pos < query.size(); ++query_pos) {
    const Node node = nodes_[node_id];
    if (node.is_leaf()) {
      const Key &match = get_key(node.key_pos());
      if ((node.key_size() <= query.size()) &&
          match.equals_to(Slice(query.address(), node.key_size()),
                          node.key_size(), query_pos)) {
        if (key_id) {
          *key_id = match.id();
        }
        if (key) {
          *key = match.slice(node.key_size());
        }
        found = true;
      }
      return found;
    }

    if (nodes_[node_id].child() == TERMINAL_LABEL) {
      const Node leaf_node = nodes_[node.offset() ^ TERMINAL_LABEL];
      if (leaf_node.is_leaf()) {
        if (key_id || key) {
          const Key &match = get_key(leaf_node.key_pos());
          if (key_id) {
            *key_id = match.id();
          }
          if (key) {
            *key = match.slice(leaf_node.key_size());
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

  const Node node = nodes_[node_id];
  if (node.is_leaf()) {
    const Key &match = get_key(node.key_pos());
    if (node.key_size() <= query.size()) {
      if (key_id) {
        *key_id = match.id();
      }
      if (key) {
        *key = match.slice(node.key_size());
      }
      found = true;
    }
  } else if (nodes_[node_id].child() == TERMINAL_LABEL) {
    const Node leaf_node = nodes_[node.offset() ^ TERMINAL_LABEL];
    if (leaf_node.is_leaf()) {
      if (key_id || key) {
        const Key &match = get_key(leaf_node.key_pos());
        if (key_id) {
          *key_id = match.id();
        }
        if (key) {
          *key = match.slice(leaf_node.key_size());
        }
      }
      found = true;
    }
  }
  return found;
}

bool Trie::insert(const Slice &key, int64_t *key_id) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, INSERTING_FLAG);

  uint64_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;

  search_leaf(key, node_id, query_pos);
  if (!insert_leaf(key, node_id, query_pos)) {
    if (key_id) {
      *key_id = get_key(nodes_[node_id].key_pos()).id();
    }
    return false;
  }

  const int64_t new_key_id = header_->next_key_id;
  const uint64_t new_key_pos = append_key(key, new_key_id);

  header_->total_key_length += key.size();
  ++header_->num_keys;

  if (new_key_id > header_->max_key_id) {
    header_->max_key_id = new_key_id;
    header_->next_key_id = new_key_id + 1;
  } else {
    header_->next_key_id = entries_[new_key_id].next();
  }

  entries_[new_key_id] = Entry::valid_entry(new_key_pos, key.size());
  nodes_[node_id].set_key(new_key_pos, key.size());
  if (key_id) {
    *key_id = new_key_id;
  }
  return true;
}

bool Trie::remove(int64_t key_id) {
  Lock lock(&header_->inter_process_mutex);

  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }
  const Entry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const Key &key = get_key(entry.key_pos());
  return remove_key(key.slice(entry.key_size()));
}

bool Trie::remove(const Slice &key) {
  if ((key.size() < MIN_KEY_SIZE) || (key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

//  GRN_DAT_THROW_IF(STATUS_ERROR, (status_flags() & CHANGING_MASK) != 0);
//  StatusFlagManager status_flag_manager(header_, REMOVING_FLAG);

  return remove_key(key);
}

bool Trie::update(int64_t key_id, const Slice &dest_key) {
  if ((dest_key.size() < MIN_KEY_SIZE) || (dest_key.size() > MAX_KEY_SIZE)) {
    GRNXX_ERROR() << "invalid key: size = " << dest_key.size();
    GRNXX_THROW();
  }

  Lock lock(&header_->inter_process_mutex);

  if ((key_id < MIN_KEY_ID) || (key_id > header_->max_key_id)) {
    return false;
  }
  const Entry entry = entries_[key_id];
  if (!entry) {
    return false;
  }
  const Key &key = get_key(entry.key_pos());
  return update_key(key_id, key.slice(entry.key_size()), dest_key);
}

bool Trie::update(const Slice &src_key, const Slice &dest_key,
                  int64_t *key_id) {
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
  if (!search(src_key, &src_key_id)) {
    return false;
  }
  if (update_key(static_cast<int64_t>(src_key_id), src_key, dest_key)) {
    if (key_id) {
      *key_id = src_key_id;
    }
    return true;
  }
  return false;
}

Trie::Trie()
  : pool_(),
    block_info_(nullptr),
    header_(nullptr),
    nodes_(nullptr),
    siblings_(nullptr),
    chunks_(nullptr),
    entries_(nullptr),
    keys_(nullptr),
    initialized_(false) {}

void Trie::create_trie(const TrieOptions &options, io::Pool pool) {
  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(Header));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<Header *>(block_address);
  *header_ = Header();

  header_->nodes_size = static_cast<uint64_t>(options.nodes_size);
  header_->nodes_size &= ~CHUNK_MASK;
  if (header_->nodes_size == 0) {
    header_->nodes_size = INITIAL_NODES_SIZE;
  }
  header_->chunks_size = header_->nodes_size / CHUNK_SIZE;
  header_->entries_size = static_cast<uint64_t>(options.entries_size);
  if (header_->entries_size == 0) {
    header_->entries_size = INITIAL_ENTRIES_SIZE;
  }
  header_->keys_size = static_cast<uint64_t>(options.keys_size);
  if (header_->keys_size == 0) {
    header_->keys_size = INITIAL_KEYS_SIZE;
  }

  create_arrays();

  reserve_node(ROOT_NODE_ID);
  nodes_[INVALID_OFFSET].set_is_origin(true);

  initialized_ = true;
}

void Trie::open_trie(io::Pool pool, uint32_t block_id) {
  pool_ = pool;
  initialized_ = true;

  block_info_ = pool_.get_block_info(block_id);

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<Header *>(block_address);

  // TODO: Check the format.

  nodes_ = static_cast<Node *>(
      pool_.get_block_address(header_->nodes_block_id));
  siblings_ = static_cast<uint8_t *>(
      pool_.get_block_address(header_->siblings_block_id));
  chunks_ = static_cast<Chunk *>(
      pool_.get_block_address(header_->chunks_block_id));
  entries_ = static_cast<Entry *>(
      pool_.get_block_address(header_->entries_block_id));
  keys_ = static_cast<uint32_t *>(
      pool_.get_block_address(header_->keys_block_id));
}

void Trie::defrag_trie(const TrieOptions &options, const Trie &trie,
                       io::Pool pool) {
  uint64_t nodes_size = options.nodes_size;
  if (nodes_size == 0) {
    nodes_size = trie.header_->num_chunks * CHUNK_SIZE;
    nodes_size *= 2;
  }
  uint64_t entries_size = options.entries_size;
  if (entries_size == 0) {
    entries_size = trie.header_->max_key_id;
    entries_size *= 2;
  }
  uint64_t keys_size = options.keys_size;
  if (keys_size == 0) {
    keys_size = trie.header_->next_key_pos;
    keys_size *= 2;
  }

  if (nodes_size > MAX_NODES_SIZE) {
    GRNXX_ERROR() << "too large request: nodes_size = " << nodes_size
                  << ", MAX_NODES_SIZE = " << MAX_NODES_SIZE;
    GRNXX_THROW();
  }
  if (entries_size > MAX_ENTRIES_SIZE) {
    GRNXX_ERROR() << "too large request: entries_size = " << entries_size
                  << ", MAX_ENTRIES_SIZE = " << MAX_ENTRIES_SIZE;
    GRNXX_THROW();
  }
  if (keys_size > MAX_KEYS_SIZE) {
    GRNXX_ERROR() << "too large request: keys_size = " << keys_size
                  << ", MAX_KEYS_SIZE = " << MAX_KEYS_SIZE;
    GRNXX_THROW();
  }

  pool_ = pool;

  block_info_ = pool_.create_block(sizeof(Header));

  void * const block_address = pool_.get_block_address(*block_info_);
  header_ = static_cast<Header *>(block_address);
  *header_ = Header();

  header_->nodes_size = static_cast<uint64_t>(nodes_size);
  header_->nodes_size &= ~CHUNK_MASK;
  if (header_->nodes_size == 0) {
    header_->nodes_size = INITIAL_NODES_SIZE;
  }
  header_->chunks_size = header_->nodes_size / CHUNK_SIZE;
  header_->entries_size = static_cast<uint64_t>(entries_size);
  if (header_->entries_size == 0) {
    header_->entries_size = INITIAL_ENTRIES_SIZE;
  }
  header_->keys_size = static_cast<uint64_t>(keys_size);
  if (header_->keys_size == 0) {
    header_->keys_size = INITIAL_KEYS_SIZE;
  }

  create_arrays();

  reserve_node(ROOT_NODE_ID);
  nodes_[INVALID_OFFSET].set_is_origin(true);

  header_->total_key_length = trie.header_->total_key_length;
  header_->num_keys = trie.header_->num_keys;
  header_->max_key_id = trie.header_->max_key_id;
  header_->next_key_id = trie.header_->next_key_id;
  for (int64_t key_id = MIN_KEY_ID; key_id <= header_->max_key_id; ++key_id) {
    entries_[key_id] = trie.entries_[key_id];
  }

  defrag_trie(trie, ROOT_NODE_ID, ROOT_NODE_ID);

  initialized_ = true;
}

void Trie::defrag_trie(const Trie &trie, uint64_t src, uint64_t dest) {
  if (trie.nodes_[src].is_leaf()) {
    const Key &key = trie.get_key(trie.nodes_[src].key_pos());
    const uint64_t key_pos = header_->next_key_pos;
    const size_t key_size = trie.nodes_[src].key_size();
    new (&keys_[key_pos]) Key(key.id(), key.slice(key_size));
    nodes_[dest].set_key(key_pos, key_size);
    entries_[key.id()] = Entry::valid_entry(key_pos, key_size);
    header_->next_key_pos += Key::estimate_size(key_size);
    return;
  }

  const uint64_t src_offset = trie.nodes_[src].offset();
  uint64_t dest_offset;
  {
    uint16_t labels[MAX_LABEL + 1];
    uint16_t num_labels = 0;

    uint16_t label = trie.nodes_[src].child();
    while (label != INVALID_LABEL) {
      GRNXX_DEBUG_THROW_IF(label > MAX_LABEL);
      const uint64_t child = src_offset ^ label;
      if (trie.nodes_[child].is_leaf() ||
          (trie.nodes_[child].child() != INVALID_LABEL)) {
        labels[num_labels++] = label;
      }
      label = trie.nodes_[child].has_sibling() ?
          trie.siblings_[child] : INVALID_LABEL;
    }
    if (num_labels == 0) {
      return;
    }

    dest_offset = find_offset(labels, num_labels);
    for (uint16_t i = 0; i < num_labels; ++i) {
      const uint64_t child = dest_offset ^ labels[i];
      reserve_node(child);
      nodes_[child].set_label(labels[i]);
      if ((i + 1) < num_labels) {
        nodes_[child].set_has_sibling(true);
        siblings_[child] = labels[i + 1];
      }
    }

    GRNXX_DEBUG_THROW_IF(nodes_[dest_offset].is_origin());
    nodes_[dest_offset].set_is_origin(true);
    nodes_[dest].set_offset(dest_offset);
    nodes_[dest].set_child(labels[0]);
  }

  uint16_t label = nodes_[dest].child();
  while (label != INVALID_LABEL) {
    defrag_trie(trie, src_offset ^ label, dest_offset ^ label);
    label = nodes_[dest_offset ^ label].has_sibling() ?
        siblings_[dest_offset ^ label] : INVALID_LABEL;
  }
}

void Trie::create_arrays() {
  const io::BlockInfo *block_info;

  block_info = pool_.create_block(sizeof(Node) * header_->nodes_size);
  header_->nodes_block_id = block_info->id();
  nodes_ = static_cast<Node *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(uint8_t) * header_->nodes_size);
  header_->siblings_block_id = block_info->id();
  siblings_ = static_cast<uint8_t *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(Chunk) * header_->chunks_size);
  header_->chunks_block_id = block_info->id();
  chunks_ = static_cast<Chunk *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(Entry) * header_->entries_size);
  header_->entries_block_id = block_info->id();
  entries_ = static_cast<Entry *>(pool_.get_block_address(*block_info));

  block_info = pool_.create_block(sizeof(uint32_t) * header_->keys_size);
  header_->keys_block_id = block_info->id();
  keys_ = static_cast<uint32_t *>(pool_.get_block_address(*block_info));
}

bool Trie::remove_key(const Slice &key) {
  uint64_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;
  if (!search_leaf(key, node_id, query_pos)) {
    return false;
  }

  const uint64_t key_pos = nodes_[node_id].key_pos();
  const Key &found_key = get_key(key_pos);
  if (!found_key.equals_to(key, nodes_[node_id].key_size(), query_pos)) {
    return false;
  }

  const int64_t key_id = found_key.id();
  nodes_[node_id].set_offset(INVALID_OFFSET);
  entries_[key_id] = Entry::invalid_entry(header_->next_key_id);

  header_->next_key_id = key_id;
  header_->total_key_length -= key.size();
  --header_->num_keys;
  return true;
}

bool Trie::update_key(int64_t key_id, const Slice &src_key,
                      const Slice &dest_key) {
  uint64_t node_id = ROOT_NODE_ID;
  size_t query_pos = 0;

  search_leaf(dest_key, node_id, query_pos);
  if (!insert_leaf(dest_key, node_id, query_pos)) {
    return false;
  }

  const uint64_t new_key_pos = append_key(dest_key, key_id);
  header_->total_key_length =
      header_->total_key_length + dest_key.size() - src_key.size();
  entries_[key_id] = Entry::valid_entry(new_key_pos, dest_key.size());
  nodes_[node_id].set_key(new_key_pos, dest_key.size());

  node_id = ROOT_NODE_ID;
  query_pos = 0;
  if (!search_leaf(src_key, node_id, query_pos)) {
    GRNXX_ERROR() << "key not found (unexpected)";
    GRNXX_THROW();
  }
  nodes_[node_id].set_offset(INVALID_OFFSET);
  return true;
}

bool Trie::search_leaf(const Slice &key, uint64_t &node_id,
                       size_t &query_pos) {
  for ( ; query_pos < key.size(); ++query_pos) {
    const Node node = nodes_[node_id];
    if (node.is_leaf()) {
      return true;
    }

    const uint64_t next = node.offset() ^ key[query_pos];
    if (nodes_[next].label() != key[query_pos]) {
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

bool Trie::insert_leaf(const Slice &key, uint64_t &node_id, size_t query_pos) {
  const Node node = nodes_[node_id];
  if (node.is_leaf()) {
    const Key &found_key = get_key(node.key_pos());
    size_t i = query_pos;
    while ((i < key.size()) && (i < node.key_size())) {
      if (key[i] != found_key[i]) {
        break;
      }
      ++i;
    }
    if ((i == key.size()) && (i == node.key_size())) {
      return false;
    }

    if (header_->num_keys >= header_->entries_size) {
      GRNXX_NOTICE() << "too many keys: num_keys = " << header_->num_keys
                     << ", entries_size = " << header_->entries_size;
      throw TrieException();
    }

    GRNXX_DEBUG_THROW_IF(static_cast<uint64_t>(header_->next_key_id) >= header_->entries_size);

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
      throw TrieException();
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

uint64_t Trie::insert_node(uint64_t node_id, uint16_t label) {
  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
  GRNXX_DEBUG_THROW_IF(label > MAX_LABEL);

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
    GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
    nodes_[next].set_key(node.key_pos(), node.key_size());
  } else if (node.offset() == INVALID_OFFSET) {
    GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());
    nodes_[offset].set_is_origin(true);
  } else {
    GRNXX_DEBUG_THROW_IF(!nodes_[offset].is_origin());
  }
  nodes_[node_id].set_offset(offset);

  const uint16_t child_label = nodes_[node_id].child();
  GRNXX_DEBUG_THROW_IF(child_label == label);
  if (child_label == INVALID_LABEL) {
    nodes_[node_id].set_child(label);
  } else if ((label == TERMINAL_LABEL) ||
             ((child_label != TERMINAL_LABEL) &&
              (label < child_label))) {
    // The next node becomes the first child.
    GRNXX_DEBUG_THROW_IF(nodes_[offset ^ child_label].is_phantom());
    GRNXX_DEBUG_THROW_IF(nodes_[offset ^ child_label].label() != child_label);
    siblings_[next] = child_label;
    nodes_[next].set_has_sibling(true);
    nodes_[node_id].set_child(label);
  } else {
    uint64_t prev = offset ^ child_label;
    GRNXX_DEBUG_THROW_IF(nodes_[prev].label() != child_label);
    uint16_t sibling_label = nodes_[prev].has_sibling() ?
        siblings_[prev] : INVALID_LABEL;
    while (label > sibling_label) {
      prev = offset ^ sibling_label;
      GRNXX_DEBUG_THROW_IF(nodes_[prev].label() != sibling_label);
      sibling_label = nodes_[prev].has_sibling() ?
          siblings_[prev] : INVALID_LABEL;
    }
    GRNXX_DEBUG_THROW_IF(label == sibling_label);
    siblings_[next] = siblings_[prev];
    siblings_[prev] = label;
    nodes_[next].set_has_sibling(nodes_[prev].has_sibling());
    nodes_[prev].set_has_sibling(true);
  }
  return next;
}

uint64_t Trie::append_key(const Slice &key, int64_t key_id) {
  if (static_cast<uint64_t>(key_id) >= header_->entries_size) {
    GRNXX_NOTICE() << "too many keys: key_id = " << key_id
                   << ", entries_size = " << header_->entries_size;
    throw TrieException();
  }

  const uint64_t key_pos = header_->next_key_pos;
  const uint64_t key_size = Key::estimate_size(key.size());

  if (key_size > (header_->keys_size - key_pos)) {
    GRNXX_NOTICE() << "too many keys: key_size = " << key_size
                   << ", keys_size = " << header_->keys_size
                   << ", key_pos = " << key_pos;
    throw TrieException();
  }
  new (&keys_[key_pos]) Key(key_id, key);

  header_->next_key_pos = key_pos + key_size;
  return key_pos;
}

uint64_t Trie::separate(const Slice &key, uint64_t node_id, size_t i) {
  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
  GRNXX_DEBUG_THROW_IF(!nodes_[node_id].is_leaf());
  GRNXX_DEBUG_THROW_IF(i > key.size());

  const Node node = nodes_[node_id];
  const Key &found_key = get_key(node.key_pos());

  uint16_t labels[2];
  labels[0] = (i < node.key_size()) ?
      static_cast<uint16_t>(found_key[i]) : TERMINAL_LABEL;
  labels[1] = (i < key.size()) ?
      static_cast<uint16_t>(key[i]) : TERMINAL_LABEL;
  GRNXX_DEBUG_THROW_IF(labels[0] == labels[1]);

  const uint64_t offset = find_offset(labels, 2);

  uint64_t next = offset ^ labels[0];
  reserve_node(next);
  GRNXX_DEBUG_THROW_IF(nodes_[offset].is_origin());

  nodes_[next].set_label(labels[0]);
  nodes_[next].set_key(node.key_pos(), node.key_size());

  next = offset ^ labels[1];
  reserve_node(next);

  nodes_[next].set_label(labels[1]);

  nodes_[offset].set_is_origin(true);
  nodes_[node_id].set_offset(offset);

  if ((labels[0] == TERMINAL_LABEL) ||
      ((labels[1] != TERMINAL_LABEL) &&
       (labels[0] < labels[1]))) {
    siblings_[offset ^ labels[0]] = labels[1];
    nodes_[offset ^ labels[0]].set_has_sibling(true);
    nodes_[node_id].set_child(labels[0]);
  } else {
    siblings_[offset ^ labels[1]] = labels[0];
    nodes_[offset ^ labels[1]].set_has_sibling(true);
    nodes_[node_id].set_child(labels[1]);
  }
  return next;
}

void Trie::resolve(uint64_t node_id, uint16_t label) {
  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
  GRNXX_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
  GRNXX_DEBUG_THROW_IF(label > MAX_LABEL);

  uint64_t offset = nodes_[node_id].offset();
  if (offset != INVALID_OFFSET) {
    uint16_t labels[MAX_LABEL + 1];
    uint16_t num_labels = 0;

    uint16_t next_label = nodes_[node_id].child();
    GRNXX_DEBUG_THROW_IF(next_label == INVALID_LABEL);
    while (next_label != INVALID_LABEL) {
      GRNXX_DEBUG_THROW_IF(next_label > MAX_LABEL);
      labels[num_labels++] = next_label;
      next_label = nodes_[offset ^ next_label].has_sibling() ?
          siblings_[offset ^ next_label] : INVALID_LABEL;
    }
    GRNXX_DEBUG_THROW_IF(num_labels == 0);

    labels[num_labels] = label;
    offset = find_offset(labels, num_labels + 1);
    migrate_nodes(node_id, offset, labels, num_labels);
  } else {
    offset = find_offset(&label, 1);
    if (offset >= (header_->num_chunks * CHUNK_SIZE)) {
      GRNXX_DEBUG_THROW_IF((offset / CHUNK_SIZE) != header_->num_chunks);
      reserve_chunk(header_->num_chunks);
    }
    nodes_[offset].set_is_origin(true);
    nodes_[node_id].set_offset(offset);
  }
}

void Trie::migrate_nodes(uint64_t node_id, uint64_t dest_offset,
                         const uint16_t *labels, uint16_t num_labels) {
  GRNXX_DEBUG_THROW_IF(node_id >= (header_->num_chunks * CHUNK_SIZE));
  GRNXX_DEBUG_THROW_IF(nodes_[node_id].is_leaf());
  GRNXX_DEBUG_THROW_IF(labels == nullptr);
  GRNXX_DEBUG_THROW_IF(num_labels == 0);
  GRNXX_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  const uint64_t src_offset = nodes_[node_id].offset();
  GRNXX_DEBUG_THROW_IF(src_offset == INVALID_OFFSET);
  GRNXX_DEBUG_THROW_IF(!nodes_[src_offset].is_origin());

  for (uint16_t i = 0; i < num_labels; ++i) {
    const uint64_t src_node_id = src_offset ^ labels[i];
    const uint64_t dest_node_id = dest_offset ^ labels[i];
    GRNXX_DEBUG_THROW_IF(nodes_[src_node_id].is_phantom());
    GRNXX_DEBUG_THROW_IF(nodes_[src_node_id].label() != labels[i]);

    reserve_node(dest_node_id);
    Node dest_node = nodes_[src_node_id];
    dest_node.set_is_origin(nodes_[dest_node_id].is_origin());
    nodes_[dest_node_id] = dest_node;
    siblings_[dest_node_id] = siblings_[src_node_id];
  }
  header_->num_zombies += num_labels;

  GRNXX_DEBUG_THROW_IF(nodes_[dest_offset].is_origin());
  nodes_[dest_offset].set_is_origin(true);
  nodes_[node_id].set_offset(dest_offset);
}

uint64_t Trie::find_offset(const uint16_t *labels, uint16_t num_labels) {
  GRNXX_DEBUG_THROW_IF(labels == nullptr);
  GRNXX_DEBUG_THROW_IF(num_labels == 0);
  GRNXX_DEBUG_THROW_IF(num_labels > (MAX_LABEL + 1));

  // Chunks are tested in descending order of level. Basically, lower level
  // chunks contain more phantom nodes.
  uint64_t level = 1;
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
      GRNXX_DEBUG_THROW_IF(chunk.level() != level);

      const uint64_t first = (chunk_id * CHUNK_SIZE) | chunk.first_phantom();
      uint64_t node_id = first;
      do {
        GRNXX_DEBUG_THROW_IF(!nodes_[node_id].is_phantom());
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

void Trie::reserve_node(uint64_t node_id) {
  if (node_id >= (header_->num_chunks * CHUNK_SIZE)) {
    reserve_chunk(node_id / CHUNK_SIZE);
  }

  Node &node = nodes_[node_id];
  GRNXX_DEBUG_THROW_IF(!node.is_phantom());

  const uint64_t chunk_id = node_id / CHUNK_SIZE;
  Chunk &chunk = chunks_[chunk_id];
  GRNXX_DEBUG_THROW_IF(chunk.num_phantoms() == 0);

  const uint64_t next = (chunk_id * CHUNK_SIZE) | node.next();
  const uint64_t prev = (chunk_id * CHUNK_SIZE) | node.prev();
  GRNXX_DEBUG_THROW_IF(next >= (header_->num_chunks * CHUNK_SIZE));
  GRNXX_DEBUG_THROW_IF(prev >= (header_->num_chunks * CHUNK_SIZE));

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

  GRNXX_DEBUG_THROW_IF(node.offset() != INVALID_OFFSET);
  GRNXX_DEBUG_THROW_IF(node.label() != INVALID_LABEL);

  --header_->num_phantoms;
}

void Trie::reserve_chunk(uint64_t chunk_id) {
  GRNXX_DEBUG_THROW_IF(chunk_id != header_->num_chunks);

  if (chunk_id >= header_->chunks_size) {
    GRNXX_NOTICE() << "too many chunks: chunk_id = " << chunk_id
                   << ", chunks_size = " << header_->chunks_size;
    throw TrieException();
  }

  header_->num_chunks = chunk_id + 1;

  Chunk chunk;
  chunk.set_failure_count(0);
  chunk.set_first_phantom(0);
  chunk.set_num_phantoms(CHUNK_SIZE);
  chunks_[chunk_id] = chunk;

  const uint64_t begin = chunk_id * CHUNK_SIZE;
  const uint64_t end = begin + CHUNK_SIZE;
  GRNXX_DEBUG_THROW_IF(end != (header_->num_chunks * CHUNK_SIZE));

  Node node;
  node.set_is_phantom(true);
  for (uint64_t i = begin; i < end; ++i) {
    node.set_prev((i - 1) & CHUNK_MASK);
    node.set_next((i + 1) & CHUNK_MASK);
    nodes_[i] = node;
    siblings_[i] = '\0';
  }

  // The level of the new chunk is 0.
  set_chunk_level(chunk_id, 0);
  header_->num_phantoms += CHUNK_SIZE;
}

void Trie::update_chunk_level(uint64_t chunk_id, uint64_t level) {
  GRNXX_DEBUG_THROW_IF(chunk_id >= header_->num_chunks);
  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  unset_chunk_level(chunk_id);
  set_chunk_level(chunk_id, level);
}

void Trie::set_chunk_level(uint64_t chunk_id, uint64_t level) {
  GRNXX_DEBUG_THROW_IF(chunk_id >= header_->num_chunks);
  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->leaders[level];
  if (leader == INVALID_LEADER) {
    // The chunk becomes the only one member of the level group.
    chunks_[chunk_id].set_next(chunk_id);
    chunks_[chunk_id].set_prev(chunk_id);
    header_->leaders[level] = chunk_id;
  } else {
    // The chunk is appended to the level group.
    const uint64_t next = leader;
    const uint64_t prev = chunks_[leader].prev();
    GRNXX_DEBUG_THROW_IF(next >= header_->num_chunks);
    GRNXX_DEBUG_THROW_IF(prev >= header_->num_chunks);
    chunks_[chunk_id].set_next(next);
    chunks_[chunk_id].set_prev(prev);
    chunks_[next].set_prev(chunk_id);
    chunks_[prev].set_next(chunk_id);
  }
  chunks_[chunk_id].set_level(level);
  chunks_[chunk_id].set_failure_count(0);
}

void Trie::unset_chunk_level(uint64_t chunk_id) {
  GRNXX_DEBUG_THROW_IF(chunk_id >= header_->num_chunks);

  const uint64_t level = chunks_[chunk_id].level();
  GRNXX_DEBUG_THROW_IF(level > MAX_CHUNK_LEVEL);

  const uint64_t leader = header_->leaders[level];
  GRNXX_DEBUG_THROW_IF(leader == INVALID_LEADER);

  const uint64_t next = chunks_[chunk_id].next();
  const uint64_t prev = chunks_[chunk_id].prev();
  GRNXX_DEBUG_THROW_IF(next >= header_->num_chunks);
  GRNXX_DEBUG_THROW_IF(prev >= header_->num_chunks);

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

}  // namespace large
}  // namespace da
}  // namespace map
}  // namespace grnxx
