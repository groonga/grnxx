#include "map/da/basic/id_cursor.hpp"

#include "exception.hpp"
#include "logger.hpp"

namespace grnxx {
namespace map {
namespace da {
namespace basic {

IDCursor::~IDCursor() {}

IDCursor *IDCursor::open(Trie *trie, MapCursorFlags flags,
                         int64_t min, int64_t max,
                         int64_t offset, int64_t limit) {
  std::unique_ptr<IDCursor> cursor(new (std::nothrow) IDCursor);
  if (!cursor) {
    GRNXX_ERROR() << "new grnxx::map::da::basic::IDCursor failed";
    GRNXX_THROW();
  }
  cursor->open_cursor(trie, flags, min, max, offset, limit);
  return cursor.release();
}

bool IDCursor::next() {
  if (limit_ <= 0) {
    return false;
  }
  while (current_ != end_) {
    const Entry entry = trie_->entries_[current_];
    if (entry) {
      key_id_ = current_;
      key_ = trie_->get_key(entry.key_pos()).slice();
      current_ += step_;
      --limit_;
      return true;
    }
    current_ += step_;
  }
  return false;
}

IDCursor::IDCursor()
 : MapCursor(),
   trie_(),
   current_(),
   end_(),
   step_(),
   limit_() {}

void IDCursor::open_cursor(Trie *trie, MapCursorFlags flags,
                           int64_t min, int64_t max,
                           int64_t offset, int64_t limit) {
  trie_ = trie;

  if (min < 0) {
    min = 0;
  } else if (flags & MAP_CURSOR_EXCEPT_MIN) {
    ++min;
  }

  if ((max < 0) || (max > trie->header_->max_key_id)) {
    max = trie->header_->max_key_id;
  } else if (flags & MAP_CURSOR_EXCEPT_MAX) {
    --max;
  }

  if (flags & MAP_CURSOR_DESCENDING) {
    current_ = max;
    end_ = min - 1;
    step_ = -1;
  } else {
    current_ = min;
    end_ = max + 1;
    step_ = 1;
  }

  while ((current_ != end_) && (offset > 0)) {
    if (trie->entries_[current_]) {
      --offset;
    }
    current_ += step_;
  }
  limit_ = (limit >= 0) ? limit : std::numeric_limits<int64_t>::max();
}

}  // namespace basic
}  // namespace da
}  // namespace map
}  // namespace grnxx
