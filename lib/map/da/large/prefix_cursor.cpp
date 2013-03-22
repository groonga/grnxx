#include "map/da/large/prefix_cursor.hpp"

#include "exception.hpp"
#include "logger.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace large {

PrefixCursor::~PrefixCursor() {}

PrefixCursor *PrefixCursor::open(Trie *trie, MapCursorFlags flags,
                                 size_t min, const Slice &max,
                                 int64_t offset, int64_t limit) {
  std::unique_ptr<PrefixCursor> cursor(new (std::nothrow) PrefixCursor);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::da::large::PrefixCursor failed";
    GRNXX_THROW();
  }
  cursor->open_cursor(trie, flags, min, max, offset, limit);
  return cursor.release();
}

bool PrefixCursor::next() {
  if (current_ == end_) {
    return false;
  }
  const Key &key = trie_->get_key(nodes_[current_].key_pos());
  key_id_ = key.id();
  key_ = key.slice(nodes_[current_].key_size());
  current_ += step_;
  return true;
}

PrefixCursor::PrefixCursor()
 : MapCursor(),
   trie_(),
   nodes_(),
   current_(),
   end_(),
   step_() {}

void PrefixCursor::open_cursor(Trie *trie, MapCursorFlags flags,
                               size_t min, const Slice &max,
                               int64_t offset, int64_t limit) try {
  trie_ = trie;

  if (flags & MAP_CURSOR_EXCEPT_MIN) {
    ++min;
  }

  Slice query = max;
  if ((max.size() > 0) && (flags & MAP_CURSOR_EXCEPT_MAX)) {
    query.remove_suffix(1);
  }

  if (limit < 0) {
    limit = std::numeric_limits<int64_t>::max();
  }

  uint64_t node_id = ROOT_NODE_ID;
  size_t i;
  for (i = 0; i < query.size(); ++i) {
    const Node node = trie_->nodes_[node_id];
    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      if ((node.key_size() >= min) && (node.key_size() <= query.size()) &&
          key.equals_to(query.prefix(node.key_size()), node.key_size(), i)) {
        nodes_.push_back(node);
      }
      break;
    }

    if ((i >= min) &&
        (trie_->nodes_[node_id].child() == TERMINAL_LABEL)) {
      const Node leaf_node = trie_->nodes_[node.offset() ^ TERMINAL_LABEL];
      if (leaf_node.is_leaf()) {
        nodes_.push_back(leaf_node);
      }
    }

    node_id = node.offset() ^ query[i];
    if (trie_->nodes_[node_id].label() != query[i]) {
      break;
    }
  }

  if (i == query.size()) {
    const Node node = trie_->nodes_[node_id];
    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      if ((node.key_size() >= min) && (node.key_size() <= query.size())) {
        nodes_.push_back(node);
      }
    } else if (trie_->nodes_[node_id].child() == TERMINAL_LABEL) {
      const Node leaf_node = trie_->nodes_[node.offset() ^ TERMINAL_LABEL];
      if (leaf_node.is_leaf()) {
        nodes_.push_back(leaf_node);
      }
    }
  }

  if (offset >= static_cast<int64_t>(nodes_.size())) {
    current_ = end_ = 0;
  } else if (flags & MAP_CURSOR_DESCENDING) {
    current_ = nodes_.size() - offset - 1;
    end_ = -1;
    if (limit < (current_ - end_)) {
      end_ = current_ - limit;
    }
    step_ = -1;
  } else {
    current_ = offset;
    end_ = nodes_.size();
    if (limit < (end_ - current_)) {
      end_ = current_ + limit;
    }
    step_ = 1;
  }
} catch (...) {
  // TODO: Catch only std::vector's exception.
  GRNXX_ERROR() << "std::vector::push_back() failed";
  GRNXX_THROW();
}

}  // namespace large
}  // namespace da
}  // namespace map
}  // namespace grnxx
