#include "grnxx/impl/index.hpp"

#include <map>
#include <set>

#include "grnxx/impl/column.hpp"
#include "grnxx/impl/cursor.hpp"

namespace grnxx {
namespace impl {
namespace index {

// TODO: These are test implementations.

// -- IteratorCursor --

template <typename T>
class IteratorCursor : public Cursor {
 public:
  using Iterator = T;

  IteratorCursor(Iterator begin, Iterator end, size_t offset, size_t limit)
      : Cursor(),
        it_(begin),
        end_(end),
        offset_(offset),
        limit_(limit) {}
  ~IteratorCursor() = default;

  size_t read(ArrayRef<Record> records);

 private:
  Iterator it_;
  Iterator end_;
  size_t offset_;
  size_t limit_;
};

template <typename T>
size_t IteratorCursor<T>::read(ArrayRef<Record> records) {
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

// Helper function to create an iterator cursor.
template <typename T>
std::unique_ptr<Cursor> create_iterator_cursor(T begin,
                                               T end,
                                               size_t offset,
                                               size_t limit) {
  return std::unique_ptr<Cursor>(
      new IteratorCursor<T>(begin, end, offset, limit));
}

// TODO: It's not clear that a reverse cursor should return row IDs in
//       reverse order or not.
//
// Helper function to create a reverse iterator cursor.
template <typename T>
std::unique_ptr<Cursor> create_reverse_iterator_cursor(T begin,
                                                       T end,
                                                       size_t offset,
                                                       size_t limit) {
  using ReverseIterator = std::reverse_iterator<T>;
  return std::unique_ptr<Cursor>(
      new IteratorCursor<ReverseIterator>(ReverseIterator(end),
                                          ReverseIterator(begin),
                                          offset,
                                          limit));
}

template <typename T> class TreeIndex;

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
    return std::unique_ptr<Cursor>(new EmptyCursor);
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_iterator_cursor(set_begin,
                                    set_end,
                                    options.offset,
                                    options.limit);
    } else {
      return create_reverse_iterator_cursor(set_begin,
                                            set_end,
                                            options.offset,
                                            options.limit);
    }
  }
}

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
    return std::unique_ptr<Cursor>(new EmptyCursor);
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_iterator_cursor(set_begin,
                                    set_end,
                                    options.offset,
                                    options.limit);
    } else {
      return create_reverse_iterator_cursor(set_begin,
                                            set_end,
                                            options.offset,
                                            options.limit);
    }
  }
}

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
    return std::unique_ptr<Cursor>(new EmptyCursor);
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == CURSOR_REGULAR_ORDER) {
      return create_iterator_cursor(set_begin,
                                    set_end,
                                    options.offset,
                                    options.limit);
    } else {
      return create_reverse_iterator_cursor(set_begin,
                                            set_end,
                                            options.offset,
                                            options.limit);
    }
  }
}

//std::unique_ptr<Cursor> TreeIndex<Text>::find(
//    const Datum &datum,
//    const CursorOptions &options) const;

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
