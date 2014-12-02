#include "grnxx/impl/index.hpp"

#include <map>
#include <set>

#include "grnxx/impl/column.hpp"
#include "grnxx/impl/cursor.hpp"

namespace grnxx {
namespace impl {
namespace index {

// TODO: These are test implementations.

// -- EmptyCursor --

// Helper function to create an empty cursor.
std::unique_ptr<Cursor> create_empty_cursor() try {
  return std::unique_ptr<Cursor>(new EmptyCursor);
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// -- ExactMatchCursor --

template <typename T>
class ExactMatchCursor : public Cursor {
 public:
  using Iterator = T;

  ExactMatchCursor(Iterator begin, Iterator end, size_t offset, size_t limit)
      : Cursor(),
        it_(begin),
        end_(end),
        offset_(offset),
        limit_(limit) {}
  ~ExactMatchCursor() = default;

  size_t read(ArrayRef<Record> records);

 private:
  Iterator it_;
  Iterator end_;
  size_t offset_;
  size_t limit_;
};

template <typename T>
size_t ExactMatchCursor<T>::read(ArrayRef<Record> records) {
  size_t max_count = records.size();
  if (max_count > limit_) {
    max_count = limit_;
  }
  if (max_count <= 0) {
    return 0;
  }
  size_t count = 0;
  while ((offset_ > 0) && (it_ != end_)) {
    ++it_;
    --offset_;
  }
  while ((count < max_count) && (it_ != end_)) {
    records[count] = Record(*it_, Float(0.0));
    ++count;
    ++it_;
  }
  limit_ -= count;
  return count;
}

// Helper function to create a cursor for exact match search.
template <typename T>
std::unique_ptr<Cursor> create_exact_match_cursor(T begin,
                                                  T end,
                                                  size_t offset,
                                                  size_t limit) try {
  return std::unique_ptr<Cursor>(
      new ExactMatchCursor<T>(begin, end, offset, limit));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// TODO: It's not clear that a reverse cursor should return row IDs in
//       reverse order or not.
//
// Helper function to create a reverse cursor for exact match search.
template <typename T>
std::unique_ptr<Cursor> create_reverse_exact_match_cursor(T begin,
                                                          T end,
                                                          size_t offset,
                                                          size_t limit) try {
  using ReverseIterator = std::reverse_iterator<T>;
  return std::unique_ptr<Cursor>(
      new ExactMatchCursor<ReverseIterator>(ReverseIterator(end),
                                            ReverseIterator(begin),
                                            offset,
                                            limit));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// -- ExactMatchCursor --

template <typename T>
class RangeCursor : public Cursor {
 public:
  using MapIterator = T;
  using SetIterator = typename MapIterator::value_type::second_type::iterator;

  RangeCursor(MapIterator begin,
              MapIterator end,
              size_t offset,
              size_t limit)
      : Cursor(),
        map_it_(begin),
        map_end_(end),
        set_it_(),
        set_end_(),
        offset_(offset),
        limit_(limit) {
    if (map_it_ != map_end_) {
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
  }
  ~RangeCursor() = default;

  size_t read(ArrayRef<Record> records);

 private:
  MapIterator map_it_;
  MapIterator map_end_;
  SetIterator set_it_;
  SetIterator set_end_;
  size_t offset_;
  size_t limit_;
};

template <typename T>
size_t RangeCursor<T>::read(ArrayRef<Record> records) {
  size_t max_count = records.size();
  if (max_count > limit_) {
    max_count = limit_;
  }
  if (max_count <= 0) {
    return 0;
  }
  size_t count = 0;
  while ((count < max_count) && (map_it_ != map_end_)) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        break;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
      if (set_it_ == set_end_) {
        continue;
      }
    }
    if (offset_ > 0) {
      --offset_;
    } else {
      records[count] = Record(*set_it_, Float(0.0));
      ++count;
    }
    ++set_it_;
  }
  limit_ -= count;
  return count;
}

// Helper function to create a range cursor.
template <typename T>
std::unique_ptr<Cursor> create_range_cursor(T begin,
                                            T end,
                                            size_t offset,
                                            size_t limit) try {
  return std::unique_ptr<Cursor>(
      new RangeCursor<T>(begin, end, offset, limit));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// TODO: It's not clear that a reverse cursor should return row IDs in
//       reverse order or not.
//
// Helper function to create a reverse range cursor.
template <typename T>
std::unique_ptr<Cursor> create_reverse_range_cursor(T begin,
                                                    T end,
                                                    size_t offset,
                                                    size_t limit) try {
  using ReverseIterator = std::reverse_iterator<T>;
  return std::unique_ptr<Cursor>(
      new RangeCursor<ReverseIterator>(ReverseIterator(end),
                                       ReverseIterator(begin),
                                       offset,
                                       limit));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// -- TreeIndex --

template <typename T> class TreeIndex;

// -- TreeIndex<Int> --

template <>
class TreeIndex<Int> : public Index {
 public:
  struct Less {
    bool operator()(Int lhs, Int rhs) const {
      return lhs.raw() < rhs.raw();
    }
  };

  using Value = Int;
  using Set = std::set<Int, Less>;
  using Map = std::map<Value, Set, Less>;

  TreeIndex(ColumnBase *column,
            const String &name,
            const IndexOptions &options);
  ~TreeIndex() = default;

  IndexType type() const {
    return TREE_INDEX;
  }

  void insert(Int row_id, const Datum &value);
  void remove(Int row_id, const Datum &value);

  std::unique_ptr<Cursor> find(const Datum &datum,
                               const CursorOptions &options) const;
  std::unique_ptr<Cursor> find_in_range(const IndexRange &range,
                                        const CursorOptions &options) const;

 private:
  mutable Map map_;
};

TreeIndex<Int>::TreeIndex(ColumnBase *column,
                          const String &name,
                          const IndexOptions &options)
    : Index(column, name),
      map_() {
  auto cursor = column->table()->create_cursor();
  auto typed_column = static_cast<Column<Int> *>(column);
  Array<Record> records;
  Array<Value> values;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    if (count == 0) {
      break;
    }
    values.resize(records.size());
    typed_column->read(records, values.ref());
    for (size_t i = 0; i < count; ++i) {
      insert(records[i].row_id, values[i]);
    }
    records.clear();
  }
}

void TreeIndex<Int>::insert(Int row_id, const Datum &value) {
  auto result = map_[value.as_int()].insert(row_id);
  if (!result.second) {
    throw "Entry already exists";  // TODO
  }
}

void TreeIndex<Int>::remove(Int row_id, const Datum &value) {
  auto map_it = map_.find(value.as_int());
  if (map_it == map_.end()) {
    throw "Entry not found";  // TODO
  }
  auto set_it = map_it->second.find(row_id);
  if (set_it == map_it->second.end()) {
    throw "Entry not found";  // TODO
  }
  map_it->second.erase(set_it);
  if (map_it->second.size() == 0) {
    map_.erase(map_it);
  }
}

std::unique_ptr<Cursor> TreeIndex<Int>::find(
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != INT_DATA) {
    throw "Data type conflict";  // TODO
  }
  auto map_it = map_.find(datum.as_int());
  if (map_it == map_.end()) {
    return create_empty_cursor();
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    } else {
      return create_reverse_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    }
  }
}

std::unique_ptr<Cursor> TreeIndex<Int>::find_in_range(
    const IndexRange &range,
    const CursorOptions &options) const {
  Int lower_bound_value = Int::min();
  // TODO: Datum should provide is_na()?
  if (range.lower_bound().value.type() != NA_DATA) {
    // TODO: Typecast will be supported in future?
    if (range.lower_bound().value.type() != INT_DATA) {
      throw "Data type conflict";  // TODO
    }
    if (!range.lower_bound().value.as_int().is_na()) {
      lower_bound_value = range.lower_bound().value.as_int();
      if (range.lower_bound().type == EXCLUSIVE_END_POINT) {
        if (lower_bound_value.is_max()) {
          return create_empty_cursor();
        }
        ++lower_bound_value;
      }
    }
  }

  Int upper_bound_value = Int::max();
  if (range.upper_bound().value.type() != NA_DATA) {
    if (range.upper_bound().value.type() != INT_DATA) {
      throw "Data type conflict";  // TODO
    }
    if (!range.upper_bound().value.as_int().is_na()) {
      upper_bound_value = range.upper_bound().value.as_int();
      if (range.upper_bound().type == EXCLUSIVE_END_POINT) {
        if (upper_bound_value.is_min()) {
          return create_empty_cursor();
        }
        --upper_bound_value;
      }
    }
  }

  if ((lower_bound_value > upper_bound_value).is_true()) {
    return create_empty_cursor();
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = map_.upper_bound(upper_bound_value);
  if (options.order_type == CURSOR_REGULAR_ORDER) {
    return create_range_cursor(
        begin, end, options.offset, options.limit);
  } else {
    return create_reverse_range_cursor(
        begin, end, options.offset, options.limit);
  }
}

// -- TreeIndex<Float> --

template <>
class TreeIndex<Float> : public Index {
 public:
  struct Less {
    bool operator()(Int lhs, Int rhs) const {
      return lhs.raw() < rhs.raw();
    }
    bool operator()(Float lhs, Float rhs) const {
      return lhs.raw() < rhs.raw();
    }
  };

  using Value = Float;
  using Set = std::set<Int, Less>;
  using Map = std::map<Value, Set, Less>;

  TreeIndex(ColumnBase *column,
            const String &name,
            const IndexOptions &options);
  ~TreeIndex() = default;

  IndexType type() const {
    return TREE_INDEX;
  }

  void insert(Int row_id, const Datum &value);
  void remove(Int row_id, const Datum &value);

  std::unique_ptr<Cursor> find(const Datum &datum,
                               const CursorOptions &options) const;
  std::unique_ptr<Cursor> find_in_range(const IndexRange &range,
                                        const CursorOptions &options) const;

 private:
  mutable Map map_;
};

TreeIndex<Float>::TreeIndex(ColumnBase *column,
                            const String &name,
                            const IndexOptions &options)
    : Index(column, name),
      map_() {
  auto cursor = column->table()->create_cursor();
  auto typed_column = static_cast<Column<Float> *>(column);
  Array<Record> records;
  Array<Value> values;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    if (count == 0) {
      break;
    }
    values.resize(records.size());
    typed_column->read(records, values.ref());
    for (size_t i = 0; i < count; ++i) {
      insert(records[i].row_id, values[i]);
    }
    records.clear();
  }
}

void TreeIndex<Float>::insert(Int row_id, const Datum &value) {
  auto result = map_[value.as_float()].insert(row_id);
  if (!result.second) {
    throw "Entry already exists";  // TODO
  }
}

void TreeIndex<Float>::remove(Int row_id, const Datum &value) {
  auto map_it = map_.find(value.as_float());
  if (map_it == map_.end()) {
    throw "Entry not found";  // TODO
  }
  auto set_it = map_it->second.find(row_id);
  if (set_it == map_it->second.end()) {
    throw "Entry not found";  // TODO
  }
  map_it->second.erase(set_it);
  if (map_it->second.size() == 0) {
    map_.erase(map_it);
  }
}

std::unique_ptr<Cursor> TreeIndex<Float>::find(
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != FLOAT_DATA) {
    throw "Data type conflict";  // TODO
  }
  auto map_it = map_.find(datum.as_float());
  if (map_it == map_.end()) {
    return create_empty_cursor();
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    } else {
      return create_reverse_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    }
  }
}

std::unique_ptr<Cursor> TreeIndex<Float>::find_in_range(
    const IndexRange &range,
    const CursorOptions &options) const {
  Float lower_bound_value = Float::min();
  // TODO: Datum should provide is_na()?
  if (range.lower_bound().value.type() != NA_DATA) {
    // TODO: Typecast will be supported in future?
    if (range.lower_bound().value.type() != FLOAT_DATA) {
      throw "Data type conflict";  // TODO
    }
    if (!range.lower_bound().value.as_float().is_na()) {
      lower_bound_value = range.lower_bound().value.as_float();
      if (range.lower_bound().type == EXCLUSIVE_END_POINT) {
        if (lower_bound_value.raw() == Float::raw_infinity()) {
          return create_empty_cursor();
        } else if (lower_bound_value.raw() == -Float::raw_infinity()) {
          lower_bound_value = Float::min();
        } else if (lower_bound_value.is_max()) {
          lower_bound_value = Float::infinity();
        } else {
          lower_bound_value = lower_bound_value.next_toward(Float::max());
        }
      }
    }
  }

  Float upper_bound_value = Float::max();
  if (range.upper_bound().value.type() != NA_DATA) {
    if (range.upper_bound().value.type() != FLOAT_DATA) {
      throw "Data type conflict";  // TODO
    }
    if (!range.upper_bound().value.as_float().is_na()) {
      upper_bound_value = range.upper_bound().value.as_float();
      if (range.upper_bound().type == EXCLUSIVE_END_POINT) {
        if (upper_bound_value.raw() == -Float::raw_infinity()) {
          return create_empty_cursor();
        } else if (upper_bound_value.raw() == Float::raw_infinity()) {
          upper_bound_value = Float::max();
        } else if (upper_bound_value.is_min()) {
          upper_bound_value = -Float::infinity();
        } else {
          upper_bound_value = upper_bound_value.next_toward(Float::min());
        }
      }
    }
  }

  if ((upper_bound_value < lower_bound_value).is_true()) {
    return create_empty_cursor();
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = map_.upper_bound(upper_bound_value);
  if (options.order_type == CURSOR_REGULAR_ORDER) {
    return create_range_cursor(
        begin, end, options.offset, options.limit);
  } else {
    return create_reverse_range_cursor(
        begin, end, options.offset, options.limit);
  }
}

// -- TreeIndex<Text> --

template <>
class TreeIndex<Text> : public Index {
 public:
  struct Less {
    bool operator()(Int lhs, Int rhs) const {
      return lhs.raw() < rhs.raw();
    }
  };

  using Value = Text;
  using Set = std::set<Int, Less>;
  using Map = std::map<String, Set>;

  TreeIndex(ColumnBase *column,
            const String &name,
            const IndexOptions &options);
  ~TreeIndex() = default;

  IndexType type() const {
    return TREE_INDEX;
  }

  void insert(Int row_id, const Datum &value);
  void remove(Int row_id, const Datum &value);

  std::unique_ptr<Cursor> find(const Datum &datum,
                               const CursorOptions &options) const;
  std::unique_ptr<Cursor> find_in_range(const IndexRange &range,
                                        const CursorOptions &options) const;

 private:
  mutable Map map_;
};

TreeIndex<Text>::TreeIndex(ColumnBase *column,
                           const String &name,
                           const IndexOptions &options)
    : Index(column, name),
      map_() {
  auto cursor = column->table()->create_cursor();
  auto typed_column = static_cast<Column<Text> *>(column);
  Array<Record> records;
  Array<Value> values;
  for ( ; ; ) {
    size_t count = cursor->read(1024, &records);
    if (count == 0) {
      break;
    }
    values.resize(records.size());
    typed_column->read(records, values.ref());
    for (size_t i = 0; i < count; ++i) {
      insert(records[i].row_id, values[i]);
    }
    records.clear();
  }
}

void TreeIndex<Text>::insert(Int row_id, const Datum &value) {
  Text text = value.as_text();
  String string;
  string.assign(text.raw_data(), text.raw_size());
  auto result = map_[std::move(string)].insert(row_id);
  if (!result.second) {
    throw "Entry already exists";  // TODO
  }
}

void TreeIndex<Text>::remove(Int row_id, const Datum &value) {
  Text text = value.as_text();
  String string(text.raw_data(), text.raw_size());
  auto map_it = map_.find(string);
  if (map_it == map_.end()) {
    throw "Entry not found";  // TODO
  }
  auto set_it = map_it->second.find(row_id);
  if (set_it == map_it->second.end()) {
    throw "Entry not found";  // TODO
  }
  map_it->second.erase(set_it);
  if (map_it->second.size() == 0) {
    map_.erase(map_it);
  }
}

std::unique_ptr<Cursor> TreeIndex<Text>::find(
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != TEXT_DATA) {
    throw "Data type conflict";  // TODO
  }
  Text text = datum.as_text();
  String string(text.raw_data(), text.raw_size());
  auto map_it = map_.find(string);
  if (map_it == map_.end()) {
    return create_empty_cursor();
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    } else {
      return create_reverse_exact_match_cursor(
          set_begin, set_end, options.offset, options.limit);
    }
  }
}

std::unique_ptr<Cursor> TreeIndex<Text>::find_in_range(
    const IndexRange &range,
    const CursorOptions &options) const {
  String lower_bound_value;
//  // TODO: Datum should provide is_na()?
  if (range.lower_bound().value.type() != NA_DATA) {
//    // TODO: Typecast will be supported in future?
    if (range.lower_bound().value.type() != TEXT_DATA) {
      throw "Data type conflict";  // TODO
    }
    Text text = range.lower_bound().value.as_text();
    if (!text.is_na()) {
      lower_bound_value = String(text.raw_data(), text.raw_size());
      if (range.lower_bound().type == EXCLUSIVE_END_POINT) {
        lower_bound_value.append('\0');
      }
    }
  }

  String upper_bound_value;
  if (range.upper_bound().value.type() != NA_DATA) {
    if (range.upper_bound().value.type() != TEXT_DATA) {
      throw "Data type conflict";  // TODO
    }
    Text text = range.upper_bound().value.as_text();
    if (!text.is_na()) {
      upper_bound_value = String(text.raw_data(), text.raw_size());
      if (range.upper_bound().type == INCLUSIVE_END_POINT) {
        upper_bound_value.append('\0');
      } else if (upper_bound_value.is_empty()) {
        return create_empty_cursor();
      }
    }
  }

  if (!upper_bound_value.is_empty()) {
    if (lower_bound_value >= upper_bound_value) {
      return create_empty_cursor();
    }
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = upper_bound_value.is_empty() ?
      map_.end() : map_.lower_bound(upper_bound_value);
  if (options.order_type == CURSOR_REGULAR_ORDER) {
    return create_range_cursor(
        begin, end, options.offset, options.limit);
  } else {
    return create_reverse_range_cursor(
        begin, end, options.offset, options.limit);
  }
}

}  // namespace index

using namespace index;

Index::Index(ColumnBase *column, const String &name)
    : column_(column),
      name_(name.clone()) {}

Index::~Index() {}

ColumnInterface *Index::column() const {
  return column_;
}

bool Index::contains(const Datum &value) const {
  return !find_one(value).is_na();
}

Int Index::find_one(const Datum &value) const {
  // TODO: This function should not fail, so Cursor should not be used.
  auto cursor = find(value);
  Array<Record> records;
  auto count = cursor->read(1, &records);
  if (count == 0) {
    Int::na();
  }
  return records[0].row_id;
}

std::unique_ptr<Cursor> Index::find(
    const Datum &value,
    const CursorOptions &options) const {
  throw "Not supported yet";  // TODO
}

std::unique_ptr<Cursor> Index::find_in_range(
    const IndexRange &range,
    const CursorOptions &options) const {
  throw "Not supported yet";  // TODO
}

std::unique_ptr<Cursor> Index::find_starts_with(
    const EndPoint &prefix,
    const CursorOptions &options) const {
  throw "Not supported yet";  // TODO
}

std::unique_ptr<Cursor> Index::find_prefixes(
    const Datum &datum,
    const CursorOptions &options) const {
  throw "Not supported yet";  // TODO
}

Index *Index::create(ColumnBase *column,
                     const String &name,
                     IndexType type,
                     const IndexOptions &options) try {
  if (type != TREE_INDEX) {
    throw "Not supoprted yet";  // TODO
  }
  switch (column->data_type()) {
    case BOOL_DATA: {
      throw "Not supported yet";  // TODO
    }
    case INT_DATA: {
      return new TreeIndex<Int>(column, name, options);
    }
    case FLOAT_DATA: {
      return new TreeIndex<Float>(column, name, options);
    }
    case GEO_POINT_DATA: {
      throw "Not supported yet";  // TODO
    }
    case TEXT_DATA: {
      return new TreeIndex<Text>(column, name, options);
    }
    case BOOL_VECTOR_DATA:
    case INT_VECTOR_DATA:
    case FLOAT_VECTOR_DATA:
    case GEO_POINT_VECTOR_DATA:
    case TEXT_VECTOR_DATA:
    default: {
      throw "Not supported yet";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void Index::rename(const String &new_name) {
  name_.assign(new_name);
}

bool Index::is_removable() const {
  return true;
}

}  // namespace impl
}  // namespace grnxx
