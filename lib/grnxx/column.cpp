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
      return BoolColumn::create(error, table, name, options);
    }
//    case INT_DATA: {
//      return ColumnImpl<Int>::create(error, table, name, options);
//    }
//    case FLOAT_DATA: {
//      return ColumnImpl<Float>::create(error, table, name, options);
//    }
//    case TIME_DATA: {
//      return ColumnImpl<Time>::create(error, table, name, options);
//    }
//    case GEO_POINT_DATA: {
//      return ColumnImpl<GeoPoint>::create(error, table, name, options);
//    }
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

// -- BoolColumn --

bool BoolColumn::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != BOOL_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  values_[row_id] = static_cast<Bool>(datum);
  return true;
}

bool BoolColumn::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

unique_ptr<BoolColumn> BoolColumn::create(Error *error,
                                          Table *table,
                                          String name,
                                          const ColumnOptions &options) {
  unique_ptr<BoolColumn> column(new (nothrow) BoolColumn);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, BOOL_DATA, options)) {
    return nullptr;
  }
  try {
    column->values_.resize(table->max_row_id() + 1, false);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return column;
}

BoolColumn::~BoolColumn() {}

bool BoolColumn::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    try {
      values_.resize(row_id + 1, false);
      return true;
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
  }
  values_[row_id] = false;
  return true;
}

void BoolColumn::unset(Int row_id) {
  values_[row_id] = false;
}

BoolColumn::BoolColumn() : Column(), values_() {}

// -- IntColumn --

bool IntColumn::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != INT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  values_[row_id] = static_cast<Int>(datum);
  return true;
}

bool IntColumn::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

unique_ptr<IntColumn> IntColumn::create(Error *error,
                                        Table *table,
                                        String name,
                                        const ColumnOptions &options) {
  unique_ptr<IntColumn> column(new (nothrow) IntColumn);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, INT_DATA, options)) {
    return nullptr;
  }
  try {
    column->values_.resize(table->max_row_id() + 1, 0);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return column;
}

IntColumn::~IntColumn() {}

bool IntColumn::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    try {
      values_.resize(row_id + 1, 0);
      return true;
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
  }
  values_[row_id] = 0;
  return true;
}

void IntColumn::unset(Int row_id) {
  values_[row_id] = 0;
}

IntColumn::IntColumn() : Column(), values_() {}

}  // namespace grnxx
