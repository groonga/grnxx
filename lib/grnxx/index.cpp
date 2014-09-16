#include "grnxx/index.hpp"

#include "grnxx/column.hpp"
#include "grnxx/column_impl.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/table.hpp"
#include "grnxx/tree_index.hpp"

namespace grnxx {

// -- Index --

Index::~Index() {}

unique_ptr<Cursor> Index::create_cursor(
    Error *error,
    const Datum &,
    const CursorOptions &) const {
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

unique_ptr<Cursor> Index::create_cursor(
    Error *error,
    const IndexRange &,
    const CursorOptions &) const {
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

Index::Index()
    : column_(nullptr),
      name_(),
      type_() {}

bool Index::initialize_base(Error *error,
                            Column *column,
                            String name,
                            IndexType type,
                            const IndexOptions &) {
  column_ = column;
  if (!name_.assign(error, name)) {
    return false;
  }
  type_ = type;
  return true;
}

unique_ptr<Index> Index::create(Error *error,
                                Column *column,
                                String name,
                                IndexType type,
                                const IndexOptions &options) {
  if (type != TREE_INDEX) {
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
    return nullptr;
  }
  switch (column->data_type()) {
    case BOOL_DATA: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
      return nullptr;
    }
    case INT_DATA: {
      return TreeIndex<Int>::create(error, column, name, options);
    }
    case FLOAT_DATA:
    case GEO_POINT_DATA:
    case TEXT_DATA:
    case BOOL_VECTOR_DATA:
    case INT_VECTOR_DATA:
    case FLOAT_VECTOR_DATA:
    case GEO_POINT_VECTOR_DATA:
    case TEXT_VECTOR_DATA:
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
      return nullptr;
    }
  }
}

bool Index::rename(Error *error, String new_name) {
  return name_.assign(error, new_name);
}

bool Index::is_removable() {
  // TODO: Not supported yet.
  return true;
}

// -- EmptyCursor --

class EmptyCursor : public Cursor {
 public:
  EmptyCursor(const Table *table) : Cursor(table) {}

  Int read(Error *error, Int max_count, Array<Record> *records);
};

Int EmptyCursor::read(Error *, Int, Array<Record> *) {
  return 0;
}

// Create an empty cursor.
unique_ptr<Cursor> create_empty_cursor(Error *error, const Table *table) {
  unique_ptr<Cursor> cursor(new (nothrow) EmptyCursor(table));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

// -- IteratorCursor --

template <typename T>
class IteratorCursor : public Cursor {
 public:
  using Iterator = T;

  IteratorCursor(const Table *table,
                 Iterator begin,
                 Iterator end,
                 Int offset,
                 Int limit)
      : Cursor(table),
        begin_(begin),
        it_(begin),
        end_(end),
        offset_(offset),
        limit_(limit),
        offset_left_(offset),
        limit_left_(limit) {}
  ~IteratorCursor() {}

  Int read(Error *error, Int max_count, Array<Record> *records);

 private:
  Iterator begin_;
  Iterator it_;
  Iterator end_;
  Int offset_;
  Int limit_;
  Int offset_left_;
  Int limit_left_;
};

template <typename T>
Int IteratorCursor<T>::read(Error *error,
                            Int max_count,
                            Array<Record> *records) {
  if (max_count > limit_left_) {
    max_count = limit_left_;
  }
  if (max_count <= 0) {
    return 0;
  }
  Int count = 0;
  while ((offset_left_ > 0) && (it_ != end_)) {
    ++it_;
    --offset_left_;
  }
  while ((count < max_count) && (it_ != end_)) {
    if (!records->push_back(error, Record(*it_, 0.0))) {
      return -1;
    }
    ++count;
    ++it_;
  }
  limit_left_ -= count;
  return count;
}

// Helper function to create an iterator cursor.
template <typename T>
unique_ptr<Cursor> create_iterator_cursor(Error *error,
                                          const Table *table,
                                          T begin,
                                          T end,
                                          Int offset,
                                          Int limit) {
  unique_ptr<Cursor> cursor(
      new (nothrow) IteratorCursor<T>(table, begin, end, offset, limit));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

// TODO: It's not clear that a reverse cursor should return row IDs in
//       reverse order or not.
//
// Helper function to create a reverse iterator cursor.
template <typename T>
unique_ptr<Cursor> create_reverse_iterator_cursor(Error *error,
                                                  const Table *table,
                                                  T begin,
                                                  T end,
                                                  Int offset,
                                                  Int limit) {
  using ReverseIterator = std::reverse_iterator<T>;
  unique_ptr<Cursor> cursor(
      new (nothrow) IteratorCursor<ReverseIterator>(table,
                                                    ReverseIterator(end),
                                                    ReverseIterator(begin),
                                                    offset,
                                                    limit));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

// -- DualIteratorCursor --

template <typename T>
class MapSetCursor : public Cursor {
 public:
  using MapIterator = T;
  using SetIterator = typename MapIterator::value_type::second_type::iterator;

  MapSetCursor(const Table *table,
               MapIterator begin,
               MapIterator end,
               Int offset,
               Int limit)
      : Cursor(table),
        map_begin_(begin),
        map_it_(begin),
        map_end_(end),
        set_it_(),
        set_end_(),
        offset_(offset),
        limit_(limit),
        offset_left_(offset),
        limit_left_(limit) {
    if (map_it_ != map_end_) {
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
  }
  ~MapSetCursor() {}

  Int read(Error *error, Int max_count, Array<Record> *records);

 private:
  MapIterator map_begin_;
  MapIterator map_it_;
  MapIterator map_end_;
  SetIterator set_begin_;
  SetIterator set_it_;
  SetIterator set_end_;
  Int offset_;
  Int limit_;
  Int offset_left_;
  Int limit_left_;
};

template <typename T>
Int MapSetCursor<T>::read(Error *error,
                          Int max_count,
                          Array<Record> *records) {
  if (max_count > limit_left_) {
    max_count = limit_left_;
  }
  if (max_count <= 0) {
    return 0;
  }
  Int count = 0;
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

    if (offset_left_ > 0) {
      --offset_left_;
    } else {
      if (!records->push_back(error, Record(*set_it_, 0.0))) {
        return -1;
      }
      ++count;
    }
    ++set_it_;
  }
  limit_left_ -= count;
  return count;
}

// Helper function to create a map set cursor.
template <typename T>
unique_ptr<Cursor> create_map_set_cursor(Error *error,
                                         const Table *table,
                                         T begin,
                                         T end,
                                         Int offset,
                                         Int limit) {
  unique_ptr<Cursor> cursor(
      new (nothrow) MapSetCursor<T>(table, begin, end, offset, limit));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

// TODO: It's not clear that a reverse cursor should return row IDs in
//       reverse order or not.
//
// Helper function to create a reverse map set cursor.
template <typename T>
unique_ptr<Cursor> create_reverse_map_set_cursor(Error *error,
                                                  const Table *table,
                                                  T begin,
                                                  T end,
                                                  Int offset,
                                                  Int limit) {
  using ReverseIterator = std::reverse_iterator<T>;
  unique_ptr<Cursor> cursor(
      new (nothrow) MapSetCursor<ReverseIterator>(table,
                                                  ReverseIterator(end),
                                                  ReverseIterator(begin),
                                                  offset,
                                                  limit));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

// -- TreeIndex<Int> --

unique_ptr<TreeIndex<Int>> TreeIndex<Int>::create(
    Error *error,
    Column *column,
    String name,
    const IndexOptions &options) {
  unique_ptr<TreeIndex> index(new (nothrow) TreeIndex);
  if (!index) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!index->initialize_base(error, column, name, TREE_INDEX, options)) {
    return nullptr;
  }
  auto cursor = column->table()->create_cursor(error);
  if (!cursor) {
    return nullptr;
  }
  auto typed_column = static_cast<ColumnImpl<Int> *>(column);
  Array<Record> records;
  for ( ; ; ) {
    Int count = cursor->read(error, 1024, &records);
    if (count < 0) {
      return nullptr;
    } else if (count == 0) {
      break;
    }
    for (Int i = 0; i < count; ++i) {
      Int row_id = records.get_row_id(i);
      if (!index->insert(error, row_id, typed_column->get(row_id))) {
        return nullptr;
      }
    }
    records.clear();
  }
  return index;
}

TreeIndex<Int>::~TreeIndex() {}

unique_ptr<Cursor> TreeIndex<Int>::create_cursor(
    Error *error,
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != INT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
    return nullptr;
  }

  auto map_it = map_.find(datum.force_int());
  if (map_it == map_.end()) {
    return create_empty_cursor(error, column_->table());
  } else {
    auto set_begin = map_it->second.begin();
    auto set_end = map_it->second.end();
    if (options.order_type == REGULAR_ORDER) {
      return create_iterator_cursor(error,
                                    column_->table(),
                                    set_begin,
                                    set_end,
                                    options.offset,
                                    options.limit);
    } else {
      return create_reverse_iterator_cursor(error,
                                            column_->table(),
                                            set_begin,
                                            set_end,
                                            options.offset,
                                            options.limit);
    }
  }
}

unique_ptr<Cursor> TreeIndex<Int>::create_cursor(
    Error *error,
    const IndexRange &range,
    const CursorOptions &options) const {
  Int lower_bound_value = numeric_limits<Int>::min();
  if (range.has_lower_bound()) {
    const EndPoint &lower_bound = range.lower_bound();
    if (lower_bound.value.type() != INT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    lower_bound_value = lower_bound.value.force_int();
    if (lower_bound.type == EXCLUSIVE_END_POINT) {
      if (lower_bound_value == numeric_limits<Int>::max()) {
        // TODO: Invalid range.
        return nullptr;
      }
      ++lower_bound_value;
    }
  }

  Int upper_bound_value = numeric_limits<Int>::max();
  if (range.has_upper_bound()) {
    const EndPoint &upper_bound = range.upper_bound();
    if (upper_bound.value.type() != INT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    upper_bound_value = upper_bound.value.force_int();
    if (upper_bound.type == EXCLUSIVE_END_POINT) {
      if (upper_bound_value == numeric_limits<Int>::min()) {
        // TODO: Invalid range.
        return nullptr;
      }
      --upper_bound_value;
    }
  }

  if (lower_bound_value > upper_bound_value) {
    // TODO: Invalid range.
    return nullptr;
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = map_.upper_bound(upper_bound_value);
  if (options.order_type == REGULAR_ORDER) {
    return create_map_set_cursor(error,
                                 column_->table(),
                                 begin,
                                 end,
                                 options.offset,
                                 options.limit);
  } else {
    return create_reverse_map_set_cursor(error,
                                         column_->table(),
                                         begin,
                                         end,
                                         options.offset,
                                         options.limit);
  }
}

bool TreeIndex<Int>::insert(Error *, Int row_id, const Datum &value) {
  auto result = map_[value.force_int()].insert(row_id);
  if (!result.second) {
    // Already exists.
    return false;
  }
  return true;
}

bool TreeIndex<Int>::remove(Error *, Int row_id, const Datum &value) {
  auto map_it = map_.find(value.force_int());
  if (map_it == map_.end()) {
    return false;
  }
  auto set_it = map_it->second.find(row_id);
  if (set_it == map_it->second.end()) {
    return false;
  }
  map_it->second.erase(set_it);
  if (map_it->second.size() == 0) {
    map_.erase(map_it);
  }
  return true;
}

}  // namespace grnxx
