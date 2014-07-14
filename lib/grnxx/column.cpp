#include "grnxx/column.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

Column::~Column() {}

Index *Column::create_index(Error *error,
                            String name,
                            IndexType index_type,
                            const IndexOptions &index_options) {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return nullptr;
}

bool Column::remove_index(Error *error, String name) {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

bool Column::rename_index(Error *error,
                          String name,
                          String new_name) {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

bool Column::reorder_index(Error *error,
                           String name,
                           String prev_name) {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

Index *Column::find_index(Error *error, String name) const {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return nullptr;
}

bool Column::set(Error *error, Int row_id, const Datum &datum) {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

bool Column::get(Error *error, Int row_id, Datum *datum) const {
  // TODO: Index is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

unique_ptr<Cursor> Column::create_cursor(
    Error *error,
    const CursorOptions &options) const {
  // TODO: Cursor is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return nullptr;
}

unique_ptr<Column> Column::create(Error *error,
                                  Table *table,
                                  String name,
                                  DataType data_type,
                                  const ColumnOptions &options) {
  switch (data_type) {
    case BOOL_DATA: {
      return ColumnImpl<Bool>::create(error, table, name, options);
    }
    case INT_DATA: {
      return ColumnImpl<Int>::create(error, table, name, options);
    }
    case FLOAT_DATA: {
      return ColumnImpl<Float>::create(error, table, name, options);
    }
    case TIME_DATA: {
      return ColumnImpl<Time>::create(error, table, name, options);
    }
    case GEO_POINT_DATA: {
      return ColumnImpl<GeoPoint>::create(error, table, name, options);
    }
//    case TEXT_DATA: {
//      return ColumnImpl<Text>::create(error, table, name, options);
//    }
    default: {
      // TODO: Other data types are not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
      return nullptr;
    }
  }
}

Column::Column()
    : table_(nullptr),
      name_(),
      data_type_(INVALID_DATA),
      ref_table_(nullptr),
      has_key_attribute_(false) {}

bool Column::initialize_base(Error *error,
                             Table *table,
                             String name,
                             DataType data_type,
                             const ColumnOptions &options) {
  table_ = table;
  if (!name_.assign(error, name)) {
    return false;
  }
  data_type_ = data_type;
  return true;
}

bool Column::rename(Error *error, String new_name) {
  return name_.assign(error, new_name);
}

bool Column::is_removable() {
  // TODO
  return true;
}

bool Column::set_initial_key(Error *error, Int row_id, const Datum &key) {
  // TODO
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

bool Column::set_default_value(Error *error, Int row_id) {
  // TODO
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

void Column::unset(Int row_id) {
}

// -- ColumnImpl --

template <typename T>
bool ColumnImpl<T>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TypeTraits<T>::data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  // Note that a Bool object does not have its own address.
  T value;
  datum.force(&value);
  values_[row_id] = value;
  return true;
}

template <typename T>
bool ColumnImpl<T>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

template <typename T>
unique_ptr<ColumnImpl<T>> ColumnImpl<T>::create(Error *error,
                                                Table *table,
                                                String name,
                                                const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               TypeTraits<T>::data_type(), options)) {
    return nullptr;
  }
  try {
    column->values_.resize(table->max_row_id() + 1,
                           TypeTraits<T>::default_value());
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return column;
}

template <typename T>
ColumnImpl<T>::~ColumnImpl() {}

template <typename T>
bool ColumnImpl<T>::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    try {
      values_.resize(row_id + 1, TypeTraits<T>::default_value());
      return true;
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
  }
  values_[row_id] = TypeTraits<T>::default_value();
  return true;
}

template <typename T>
void ColumnImpl<T>::unset(Int row_id) {
  values_[row_id] = TypeTraits<T>::default_value();
}

template <typename T>
ColumnImpl<T>::ColumnImpl() : Column(), values_() {}

}  // namespace grnxx
