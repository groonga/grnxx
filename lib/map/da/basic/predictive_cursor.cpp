#include "map/da/basic/predictive_cursor.hpp"

#include "exception.hpp"
#include "logger.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace basic {
namespace {

constexpr uint64_t IS_ROOT_FLAG    = uint64_t(1) << 63;
constexpr uint64_t POST_ORDER_FLAG = uint64_t(1) << 63;

}  // namespace

PredictiveCursor::~PredictiveCursor() {}

PredictiveCursor *PredictiveCursor::open(Trie *trie, MapCursorFlags flags,
                                         const Slice &min,
                                         int64_t offset, int64_t limit) {
  std::unique_ptr<PredictiveCursor> cursor(new (std::nothrow) PredictiveCursor);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::da::basic::PredictiveCursor failed";
    GRNXX_THROW();
  }
  cursor->open_cursor(trie, flags, min, offset, limit);
  return cursor.release();
}

bool PredictiveCursor::next() {
  if (limit_ == 0) {
    return false;
  }

  if (flags_ & MAP_CURSOR_DESCENDING) {
    return descending_next();
  } else {
    return ascending_next();
  }
}

PredictiveCursor::PredictiveCursor()
 : MapCursor(),
   trie_(),
   node_ids_(),
   min_size_(),
   offset_(),
   limit_(),
   flags_() {}

void PredictiveCursor::open_cursor(Trie *trie, MapCursorFlags flags,
                                   const Slice &min,
                                   int64_t offset, int64_t limit) try {
  trie_ = trie;

  min_size_ = min.size();
  if (flags & MAP_CURSOR_EXCEPT_MIN) {
    ++min_size_;
  }

  uint64_t node_id = ROOT_NODE_ID;
  for (size_t i = 0; i < min.size(); ++i) {
    const Node node = trie_->nodes_[node_id];
    if (node.is_leaf()) {
      if (offset <= 0) {
        const Key &key = trie_->get_key(node.key_pos());
        if ((key.size() >= min_size_) &&
            (key.slice().subslice(i, min.size() - i) ==
             min.subslice(i, min.size() - i))) {
          if (~flags & MAP_CURSOR_DESCENDING) {
            node_id |= IS_ROOT_FLAG;
          }
          node_ids_.push_back(node_id);
        }
      }
      return;
    }

    node_id = node.offset() ^ min[i];
    if (trie_->nodes_[node_id].label() != min[i]) {
      return;
    }
  }

  if (~flags & MAP_CURSOR_DESCENDING) {
    node_id |= IS_ROOT_FLAG;
  }
  node_ids_.push_back(node_id);

  offset_ = offset;
  limit_ = (limit >= 0) ? limit : std::numeric_limits<int64_t>::max();
  flags_ = flags;
} catch (...) {
  // TODO: Catch only std::vector's exception.
  GRNXX_ERROR() << "std::vector::push_back() failed";
  GRNXX_THROW();
}

bool PredictiveCursor::ascending_next() try {
  while (!node_ids_.empty()) {
    const bool is_root = node_ids_.back() & IS_ROOT_FLAG;
    const uint64_t node_id = node_ids_.back() & ~IS_ROOT_FLAG;
    node_ids_.pop_back();

    const Node node = trie_->nodes_[node_id];
    if (!is_root && (node.sibling() != INVALID_LABEL)) {
      node_ids_.push_back(node_id ^ node.label() ^ node.sibling());
    }

    if (node.is_leaf()) {
      const Key &key = trie_->get_key(node.key_pos());
      if (key.size() >= min_size_) {
        if (offset_ > 0) {
          --offset_;
        } else if (limit_ != 0) {
          key_id_ = key.id();
          key_ = key.slice();
          --limit_;
          return true;
        }
      }
    } else if (node.child() != INVALID_LABEL) {
      node_ids_.push_back(node.offset() ^ node.child());
    }
  }
  return false;
} catch (...) {
  // TODO: Catch only std::vector's exception.
  GRNXX_ERROR() << "std::vector::push_back() failed";
  GRNXX_THROW();
}

bool PredictiveCursor::descending_next() try {
  while (!node_ids_.empty()) {
    const bool post_order = node_ids_.back() & POST_ORDER_FLAG;
    const uint64_t node_id = node_ids_.back() & ~POST_ORDER_FLAG;

    const Node node = trie_->nodes_[node_id];
    if (post_order) {
      node_ids_.pop_back();
      if (node.is_leaf()) {
        const Key &key = trie_->get_key(node.key_pos());
        if (key.size() >= min_size_) {
          if (offset_ > 0) {
            --offset_;
          } else if (limit_ != 0) {
            key_id_ = key.id();
            key_ = key.slice();
            --limit_;
            return true;
          }
        }
      }
    } else {
      node_ids_.back() |= POST_ORDER_FLAG;
      uint64_t label = trie_->nodes_[node_id].child();
      while (label != INVALID_LABEL) {
        node_ids_.push_back(node.offset() ^ label);
        label = trie_->nodes_[node.offset() ^ label].sibling();
      }
    }
  }
  return false;
} catch (...) {
  // TODO: Catch only std::vector's exception.
  GRNXX_ERROR() << "std::vector::push_back() failed";
  GRNXX_THROW();
}

}  // namespace basic
}  // namespace da
}  // namespace map
}  // namespace grnxx
