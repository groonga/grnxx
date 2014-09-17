#include "grnxx/index.hpp"

#include "grnxx/column.hpp"
#include "grnxx/column_impl.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/table.hpp"
#include "grnxx/tree_index.hpp"

namespace grnxx {

// -- Index --

Index::~Index() {}

unique_ptr<Cursor> Index::find(
    Error *error,
    const Datum &,
    const CursorOptions &) const {
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

unique_ptr<Cursor> Index::find_in_range(
    Error *error,
    const IndexRange &,
    const CursorOptions &) const {
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

unique_ptr<Cursor> Index::find_starts_with(
    Error *error,
    const EndPoint &,
    const CursorOptions &) const {
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
  return nullptr;
}

unique_ptr<Cursor> Index::find_prefixes(
    Error *error,
    const Datum &,
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
      return TreeIndex<Bool>::create(error, column, name, options);
    }
    case INT_DATA: {
      return TreeIndex<Int>::create(error, column, name, options);
    }
    case FLOAT_DATA: {
      return TreeIndex<Float>::create(error, column, name, options);
    }
    case GEO_POINT_DATA: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supoprted yet");
      return nullptr;
    }
    case TEXT_DATA: {
      return TreeIndex<Text>::create(error, column, name, options);
    }
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

// -- ArrayCursor --

class ArrayCursor : public Cursor {
 public:
  using Set = std::set<Int>;
  using Map = std::map<std::string, Set>;

  ArrayCursor(const Table *table,
              Array<Map::iterator> &&array,
              Int offset,
              Int limit)
      : Cursor(table),
        array_(std::move(array)),
        pos_(0),
        it_(),
        end_(),
        offset_(offset),
        limit_(limit),
        offset_left_(offset),
        limit_left_(limit) {
    if (array_.size() != 0) {
      it_ = array_[0]->second.begin();
      end_ = array_[0]->second.end();
    }
  }
  ~ArrayCursor() {}

  Int read(Error *error, Int max_count, Array<Record> *records);

 private:
  Array<Map::iterator> array_;
  Int pos_;
  Set::iterator it_;
  Set::iterator end_;
  Int offset_;
  Int limit_;
  Int offset_left_;
  Int limit_left_;
};

Int ArrayCursor::read(Error *error,
                      Int max_count,
                      Array<Record> *records) {
  if (max_count > limit_left_) {
    max_count = limit_left_;
  }
  if (max_count <= 0) {
    return 0;
  }
  Int count = 0;
  while ((count < max_count) && (pos_ < array_.size())) {
    if (it_ == end_) {
      ++pos_;
      if (pos_ >= array_.size()) {
        break;
      }
      it_ = array_[pos_]->second.begin();
      end_ = array_[pos_]->second.end();
      if (it_ == end_) {
        continue;
      }
    }

    if (offset_left_ > 0) {
      --offset_left_;
    } else {
      if (!records->push_back(error, Record(*it_, 0.0))) {
        return -1;
      }
      ++count;
    }
    ++it_;
  }
  limit_left_ -= count;
  return count;
}

// -- TreeIndex<Bool> --

unique_ptr<TreeIndex<Bool>> TreeIndex<Bool>::create(
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
  auto typed_column = static_cast<ColumnImpl<Bool> *>(column);
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

TreeIndex<Bool>::~TreeIndex() {}

unique_ptr<Cursor> TreeIndex<Bool>::find(
    Error *error,
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != BOOL_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
    return nullptr;
  }

  auto map_it = map_.find(datum.force_bool());
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

unique_ptr<Cursor> TreeIndex<Bool>::find_in_range(
    Error *error,
    const IndexRange &range,
    const CursorOptions &options) const {
  Bool lower_bound_value = false;
  if (range.has_lower_bound()) {
    const EndPoint &lower_bound = range.lower_bound();
    if (lower_bound.value.type() != BOOL_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    lower_bound_value = lower_bound.value.force_bool();
    if (lower_bound.type == EXCLUSIVE_END_POINT) {
      if (lower_bound_value) {
        return create_empty_cursor(error, column_->table());
      }
      lower_bound_value = true;
    }
  }

  Bool upper_bound_value = true;
  if (range.has_upper_bound()) {
    const EndPoint &upper_bound = range.upper_bound();
    if (upper_bound.value.type() != BOOL_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    upper_bound_value = upper_bound.value.force_bool();
    if (upper_bound.type == EXCLUSIVE_END_POINT) {
      if (!upper_bound_value) {
        return create_empty_cursor(error, column_->table());
      }
      upper_bound_value = false;
    }
  }

  if (lower_bound_value > upper_bound_value) {
    return create_empty_cursor(error, column_->table());
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

bool TreeIndex<Bool>::insert(Error *, Int row_id, const Datum &value) {
  auto result = map_[value.force_bool()].insert(row_id);
  if (!result.second) {
    // Already exists.
    return false;
  }
  return true;
}

bool TreeIndex<Bool>::remove(Error *, Int row_id, const Datum &value) {
  auto map_it = map_.find(value.force_bool());
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

unique_ptr<Cursor> TreeIndex<Int>::find(
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

unique_ptr<Cursor> TreeIndex<Int>::find_in_range(
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
        return create_empty_cursor(error, column_->table());
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
        return create_empty_cursor(error, column_->table());
      }
      --upper_bound_value;
    }
  }

  if (lower_bound_value > upper_bound_value) {
    return create_empty_cursor(error, column_->table());
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

// -- TreeIndex<Float> --

unique_ptr<TreeIndex<Float>> TreeIndex<Float>::create(
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
  auto typed_column = static_cast<ColumnImpl<Float> *>(column);
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

TreeIndex<Float>::~TreeIndex() {}

unique_ptr<Cursor> TreeIndex<Float>::find(
    Error *error,
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != FLOAT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
    return nullptr;
  }

  auto map_it = map_.find(datum.force_float());
  if (map_it == map_.end()) {
    return create_empty_cursor(error, column_->table());
  }
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

unique_ptr<Cursor> TreeIndex<Float>::find_in_range(
    Error *error,
    const IndexRange &range,
    const CursorOptions &options) const {
  Float lower_bound_value = numeric_limits<Float>::min();
  if (range.has_lower_bound()) {
    const EndPoint &lower_bound = range.lower_bound();
    if (lower_bound.value.type() != FLOAT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    lower_bound_value = lower_bound.value.force_float();
    if (lower_bound.type == EXCLUSIVE_END_POINT) {
      if (lower_bound_value == -numeric_limits<Float>::infinity()) {
        lower_bound_value = numeric_limits<Float>::min();
      } else if (lower_bound_value == numeric_limits<Float>::max()) {
        lower_bound_value = numeric_limits<Float>::infinity();
      } else if (std::isnan(lower_bound_value)) {
        return create_empty_cursor(error, column_->table());
      } else {
        lower_bound_value = std::nextafter(lower_bound_value,
                                           numeric_limits<Float>::max());
      }
    }
  }

  Float upper_bound_value = numeric_limits<Float>::max();
  if (range.has_upper_bound()) {
    const EndPoint &upper_bound = range.upper_bound();
    if (upper_bound.value.type() != FLOAT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    upper_bound_value = upper_bound.value.force_float();
    if (upper_bound.type == EXCLUSIVE_END_POINT) {
      if (upper_bound_value == -numeric_limits<Float>::infinity()) {
        return create_empty_cursor(error, column_->table());
      } else if (upper_bound_value == numeric_limits<Float>::min()) {
        upper_bound_value = -numeric_limits<Float>::infinity();
      } else if (std::isnan(upper_bound_value)) {
        upper_bound_value = numeric_limits<Float>::infinity();
      } else {
        upper_bound_value = std::nextafter(upper_bound_value,
                                           numeric_limits<Float>::min());
      }
    }
  }

  if (Less()(upper_bound_value, lower_bound_value)) {
    return create_empty_cursor(error, column_->table());
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

bool TreeIndex<Float>::insert(Error *, Int row_id, const Datum &value) {
  auto result = map_[value.force_float()].insert(row_id);
  if (!result.second) {
    // Already exists.
    return false;
  }
  return true;
}

bool TreeIndex<Float>::remove(Error *, Int row_id, const Datum &value) {
  auto map_it = map_.find(value.force_float());
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

// -- TreeIndex<Text> --

unique_ptr<TreeIndex<Text>> TreeIndex<Text>::create(
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
  auto typed_column = static_cast<ColumnImpl<Text> *>(column);
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

TreeIndex<Text>::~TreeIndex() {}

unique_ptr<Cursor> TreeIndex<Text>::find(
    Error *error,
    const Datum &datum,
    const CursorOptions &options) const {
  if (datum.type() != TEXT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
    return nullptr;
  }

  Text text = datum.force_text();
  std::string string(text.data(), text.size());
  auto map_it = map_.find(string);
  if (map_it == map_.end()) {
    return create_empty_cursor(error, column_->table());
  }
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

unique_ptr<Cursor> TreeIndex<Text>::find_in_range(
    Error *error,
    const IndexRange &range,
    const CursorOptions &options) const {
  std::string lower_bound_value = "";
  if (range.has_lower_bound()) {
    const EndPoint &lower_bound = range.lower_bound();
    if (lower_bound.value.type() != TEXT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    Text text = lower_bound.value.force_text();
    lower_bound_value.assign(text.data(), text.size());
    if (lower_bound.type == EXCLUSIVE_END_POINT) {
      lower_bound_value += '\0';
    }
  }

  std::string upper_bound_value = "";
  if (range.has_upper_bound()) {
    const EndPoint &upper_bound = range.upper_bound();
    if (upper_bound.value.type() != TEXT_DATA) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
      return nullptr;
    }
    Text text = upper_bound.value.force_text();
    upper_bound_value.assign(text.data(), text.size());
    if (upper_bound.type == INCLUSIVE_END_POINT) {
      upper_bound_value += '\0';
    } else if (upper_bound_value.empty()) {
      return create_empty_cursor(error, column_->table());
    }
  }

  if (range.has_upper_bound()) {
    if (lower_bound_value >= upper_bound_value) {
      return create_empty_cursor(error, column_->table());
    }
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = range.has_upper_bound() ?
      map_.lower_bound(upper_bound_value) : map_.end();
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

unique_ptr<Cursor> TreeIndex<Text>::find_starts_with(
    Error *error,
    const EndPoint &prefix,
    const CursorOptions &options) const {
  std::string lower_bound_value = "";
  if (prefix.value.type() != TEXT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Data type conflict");
    return nullptr;
  }
  Text prefix_text = prefix.value.force_text();
  lower_bound_value.assign(prefix_text.data(), prefix_text.size());

  std::string upper_bound_value = lower_bound_value;
  while (!upper_bound_value.empty() &&
         (static_cast<unsigned char>(upper_bound_value.back()) == 0xFF)) {
    upper_bound_value.pop_back();
  }
  if (!upper_bound_value.empty()) {
    ++upper_bound_value.back();
  }

  if (prefix.type == EXCLUSIVE_END_POINT) {
    lower_bound_value += '\0';
  }

  auto begin = map_.lower_bound(lower_bound_value);
  auto end = !upper_bound_value.empty() ?
      map_.lower_bound(upper_bound_value) : map_.end();
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

unique_ptr<Cursor> TreeIndex<Text>::find_prefixes(
    Error *error,
    const Datum &prefix,
    const CursorOptions &options) const {
  Text prefix_text = prefix.force_text();
  std::string value(prefix_text.data(), prefix_text.size());
  Array<Map::iterator> array;
  for (std::size_t i = 0; i <= value.size(); ++i) {
    auto it = map_.find(value.substr(0, i));
    if (it != map_.end()) {
      if (!array.push_back(error, it)) {
        return nullptr;
      }
    }
  }
  if (options.order_type == REVERSE_ORDER) {
    for (Int i = 0; i < (array.size() / 2); ++i) {
      Map::iterator temp = array[i];
      array[i] = array[array.size() - i - 1];
      array[array.size() - i - 1] = temp;
    }
  }
  unique_ptr<Cursor> cursor(
      new (nothrow) ArrayCursor(column_->table(), std::move(array),
                                options.offset, options.limit));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

bool TreeIndex<Text>::insert(Error *, Int row_id, const Datum &value) {
  Text text = value.force_text();
  std::string string(text.data(), text.size());
  auto result = map_[string].insert(row_id);
  if (!result.second) {
    // Already exists.
    return false;
  }
  return true;
}

bool TreeIndex<Text>::remove(Error *, Int row_id, const Datum &value) {
  Text text = value.force_text();
  std::string string(text.data(), text.size());
  auto map_it = map_.find(string);
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
