#include "grnxx/index.hpp"

#include "grnxx/column.hpp"
#include "grnxx/column_impl.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/table.hpp"
#include "grnxx/tree_index.hpp"

namespace grnxx {

// -- Index --

Index::~Index() {}

unique_ptr<Cursor> Index::create_cursor(
    Error *error,
    const CursorOptions &options) const {
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
                            const IndexOptions &options) {
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

// -- TreeIndexCursor --

template <typename T> class TreeIndexCursor;

template <>
class TreeIndexCursor<Int> : public Cursor {
 public:
  using Value = Int;
  using Set = std::set<Int>;
  using Map = std::map<Value, Set>;

  TreeIndexCursor(const Table *table, Map::iterator begin, Map::iterator end)
      : Cursor(table),
        map_it_(begin),
        map_end_(end),
        set_it_(),
        set_end_() {
    if (begin != end) {
      set_it_ = begin->second.begin();
      set_end_ = begin->second.end();
    }
  }
  ~TreeIndexCursor() {}

  Int read(Error *error, Int max_count, Array<Record> *records);

 private:
  Map::iterator map_it_;
  Map::iterator map_end_;
  Set::iterator set_it_;
  Set::iterator set_end_;
};

Int TreeIndexCursor<Int>::read(Error *error,
                               Int max_count,
                               Array<Record> *records) {
  if (map_it_ == map_end_) {
    return 0;
  }
  Int count = 0;
  for ( ; count < max_count; ++count) {
    if (set_it_ == set_end_) {
      ++map_it_;
      if (map_it_ == map_end_) {
        break;
      }
      set_it_ = map_it_->second.begin();
      set_end_ = map_it_->second.end();
    }
    if (!records->push_back(error, Record(*set_it_, 0.0))) {
      return -1;
    }
    ++set_it_;
  }
  return count;
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
    const CursorOptions &options) const {
  unique_ptr<Cursor> cursor(
      new (nothrow) TreeIndexCursor<Int>(column_->table(),
                                         map_.begin(), map_.end()));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return cursor;
}

bool TreeIndex<Int>::insert(Error *error, Int row_id, const Datum &value) {
  auto result = map_[value.force_int()].insert(row_id);
  if (!result.second) {
    // Already exists.
    return false;
  }
  return true;
}

bool TreeIndex<Int>::remove(Error *error, Int row_id, const Datum &value) {
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
