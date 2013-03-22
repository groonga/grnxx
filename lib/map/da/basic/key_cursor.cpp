#include "map/da/basic/key_cursor.hpp"

#include "exception.hpp"
#include "logger.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace basic {
namespace {

constexpr uint64_t POST_ORDER_FLAG = uint64_t(1) << 63;

}  // namespace

KeyCursor::~KeyCursor() {}

KeyCursor *KeyCursor::open(Trie *trie, MapCursorFlags flags,
                           const Slice &min, const Slice &max,
                           int64_t offset, int64_t limit) {
  std::unique_ptr<KeyCursor> cursor(new (std::nothrow) KeyCursor);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::da::basic::KeyCursor failed";
    GRNXX_THROW();
  }
  cursor->open_cursor(trie, flags, min, max, offset, limit);
  return cursor.release();
}

bool KeyCursor::next() {
  if (limit_ == 0) {
    return false;
  }

  if (flags_ & MAP_CURSOR_DESCENDING) {
    return descending_next();
  } else {
    return ascending_next();
  }
}

  Trie *trie_;
  std::vector<uint64_t> node_ids_;
  int64_t offset_;
  int64_t limit_;
  MapCursorFlags flags_;

KeyCursor::KeyCursor()
 : MapCursor(),
   trie_(),
   node_ids_(),
   offset_(),
   limit_(),
   flags_(),
   has_end_(),
   end_() {}

void KeyCursor::open_cursor(Trie *trie, MapCursorFlags flags,
                            const Slice &min, const Slice &max,
                            int64_t offset, int64_t limit) {
  trie_ = trie;
  offset_ = offset;
  limit_ = (limit >= 0) ? limit : std::numeric_limits<int64_t>::max();
  flags_ = flags;

  if (flags & MAP_CURSOR_DESCENDING) {
    descending_init(min, max);
  } else {
    ascending_init(min, max);
  }
}

// TODO: std::vector::push_back() may throw an exception.
void KeyCursor::ascending_init(const Slice &min, const Slice &max) {
  if (max) {
    has_end_ = true;
    end_ = String(reinterpret_cast<const char *>(max.ptr()), max.size());
  }

  if (!min) {
    node_ids_.push_back(ROOT_NODE_ID);
    return;
  }

  uint64_t node_id = ROOT_NODE_ID;
  Node node;
  for (size_t i = 0; i < min.size(); ++i) {
    node = trie_->nodes_[node_id];
    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      const int result = key.slice().compare(min, i);
      if ((result > 0) ||
          ((result == 0) && (~flags_ & MAP_CURSOR_EXCEPT_MIN))) {
        node_ids_.push_back(node_id);
      } else if (node.sibling() != INVALID_LABEL) {
        node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
      }
      return;
    } else if (node.sibling() != INVALID_LABEL) {
      node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
    }

    node_id = node.offset() ^ min[i];
    if (trie_->nodes_[node_id].label() != min[i]) {
      uint16_t label = node.child();
      if (label == TERMINAL_LABEL) {
        label = trie_->nodes_[node.offset() ^ label].sibling();
      }
      while (label != INVALID_LABEL) {
        if (label > min[i]) {
          node_ids_.push_back(node.offset() ^ label);
          break;
        }
        label = trie_->nodes_[node.offset() ^ label].sibling();
      }
      return;
    }
  }

  node = trie_->nodes_[node_id];
  if (node.is_leaf()) {
    const Key &key = trie_->get_key(node.key_pos());
    if ((key.size() != min.size()) || (~flags_ & MAP_CURSOR_EXCEPT_MIN)) {
      node_ids_.push_back(node_id);
    } else if (node.sibling() != INVALID_LABEL) {
      node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
    }
    return;
  } else if (node.sibling() != INVALID_LABEL) {
    node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
  }

  uint16_t label = node.child();
  if ((label == TERMINAL_LABEL) && (flags_ & MAP_CURSOR_EXCEPT_MIN)) {
    label = trie_->nodes_[node.offset() ^ label].sibling();
  }
  if (label != INVALID_LABEL) {
    node_ids_.push_back(node.offset() ^ label);
  }
}

// TODO: std::vector::push_back() may throw an exception.
void KeyCursor::descending_init(const Slice &min, const Slice &max) {
  if (min) {
    has_end_ = true;
    end_ = String(reinterpret_cast<const char *>(min.ptr()), min.size());
  }

  if (!max) {
    node_ids_.push_back(ROOT_NODE_ID);
    return;
  }

  uint64_t node_id = ROOT_NODE_ID;
  for (size_t i = 0; i < max.size(); ++i) {
    const Node node = trie_->nodes_[node_id];
    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      const int result = key.slice().compare(max, i);
      if ((result < 0) ||
          ((result == 0) && (~flags_ & MAP_CURSOR_EXCEPT_MAX))) {
        node_ids_.push_back(node_id | POST_ORDER_FLAG);
      }
      return;
    }

    uint16_t label = trie_->nodes_[node_id].child();
    if (label == TERMINAL_LABEL) {
      node_id = node.offset() ^ label;
      node_ids_.push_back(node_id | POST_ORDER_FLAG);
      label = trie_->nodes_[node_id].sibling();
    }
    while (label != INVALID_LABEL) {
      node_id = node.offset() ^ label;
      if (label < max[i]) {
        node_ids_.push_back(node_id);
      } else if (label > max[i]) {
        return;
      } else {
        break;
      }
      label = trie_->nodes_[node_id].sibling();
    }
    if (label == INVALID_LABEL) {
      return;
    }
  }

  const Node node = trie_->nodes_[node_id];
  if (node.is_leaf()) {
    const Key &key = trie_->get_key(node.key_pos());
    if ((key.size() == max.size()) && (~flags_ & MAP_CURSOR_EXCEPT_MAX)) {
      node_ids_.push_back(node_id | POST_ORDER_FLAG);
    }
    return;
  }

  uint16_t label = trie_->nodes_[node_id].child();
  if ((label == TERMINAL_LABEL) && (~flags_ & MAP_CURSOR_EXCEPT_MAX)) {
    node_ids_.push_back((node.offset() ^ label) | POST_ORDER_FLAG);
  }
}

// TODO: std::vector::push_back() may throw an exception.
bool KeyCursor::ascending_next() {
  while (!node_ids_.empty()) {
    const uint64_t node_id = node_ids_.back();
    node_ids_.pop_back();

    const Node node = trie_->nodes_[node_id];
    if (node.sibling() != INVALID_LABEL) {
      node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
    }

    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      if (has_end_) {
        const int result =
            key.slice().compare(Slice(end_.c_str(), end_.length()));
        if ((result > 0) ||
            ((result == 0) && (flags_ & MAP_CURSOR_EXCEPT_MAX))) {
          limit_ = 0;
          return false;
        }
      }
      if (offset_ > 0) {
        --offset_;
      } else if (limit_ > 0) {
        key_id_ = key.id();
        key_ = key.slice();
        --limit_;
        return true;
      }
    } else if (node.child() != INVALID_LABEL) {
      node_ids_.push_back(node.offset() ^ node.child());
    }
  }
  return false;
}

// TODO: std::vector::push_back() may throw an exception.
bool KeyCursor::descending_next() {
  while (!node_ids_.empty()) {
    const bool post_order = node_ids_.back() & POST_ORDER_FLAG;
    const uint64_t node_id = node_ids_.back() & ~POST_ORDER_FLAG;

    const Node node = trie_->nodes_[node_id];
    if (post_order) {
      node_ids_.pop_back();
      if (node.is_leaf()) {
        const Key &key = trie_->get_key(node.key_pos());
        if (has_end_) {
          const int result =
              key.slice().compare(Slice(end_.c_str(), end_.length()));
          if ((result < 0) ||
              ((result == 0) && (flags_ & MAP_CURSOR_EXCEPT_MIN))) {
            limit_ = 0;
            return false;
          }
        }
        if (offset_ > 0) {
          --offset_;
        } else if (limit_ > 0) {
          key_id_ = key.id();
          key_ = key.slice();
          --limit_;
          return true;
        }
      }
    } else {
      node_ids_.back() |= POST_ORDER_FLAG;
      uint16_t label = trie_->nodes_[node_id].child();
      while (label != INVALID_LABEL) {
        node_ids_.push_back(node.offset() ^ label);
        label = trie_->nodes_[node.offset() ^ label].sibling();
      }
    }
  }
  return false;
}

}  // namespace basic
}  // namespace da
}  // namespace map
}  // namespace grnxx
