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
    case GEO_POINT_DATA: {
      return ColumnImpl<GeoPoint>::create(error, table, name, options);
    }
    case TEXT_DATA: {
      return ColumnImpl<Text>::create(error, table, name, options);
    }
    case BOOL_VECTOR_DATA: {
      return ColumnImpl<Vector<Bool>>::create(error, table, name, options);
    }
    case INT_VECTOR_DATA: {
      return ColumnImpl<Vector<Int>>::create(error, table, name, options);
    }
    case FLOAT_VECTOR_DATA: {
      return ColumnImpl<Vector<Float>>::create(error, table, name, options);
    }
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
  // TODO: Reference column is not supported yet.
  return true;
}

bool Column::set_initial_key(Error *error, Int row_id, const Datum &key) {
  // TODO: Key column is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
  return false;
}

// -- ColumnImpl<T> --

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
  values_.set(row_id, value);
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
  if (!column->values_.resize(error, table->max_row_id() + 1,
                              TypeTraits<T>::default_value())) {
    return nullptr;
  }
  return column;
}

template <typename T>
ColumnImpl<T>::~ColumnImpl() {}

template <typename T>
bool ColumnImpl<T>::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1, TypeTraits<T>::default_value())) {
      return false;
    }
  }
  values_.set(row_id, TypeTraits<T>::default_value());
  return true;
}

template <typename T>
void ColumnImpl<T>::unset(Int row_id) {
  values_.set(row_id, TypeTraits<T>::default_value());
}

template <typename T>
ColumnImpl<T>::ColumnImpl() : Column(), values_() {}

// -- ColumnImpl<Text> --

bool ColumnImpl<Text>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TEXT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Text value = datum.force_text();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    std::memcpy(&bodies_[offset], value.data(), value.size());
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long text is stored in front of the body.
    if ((offset % sizeof(Int)) != 0) {
      offset += sizeof(Int) - (offset % sizeof(Int));
    }
    if (!bodies_.resize(error, offset + sizeof(Int) + value.size())) {
      return false;
    }
    *reinterpret_cast<Int *>(&bodies_[offset]) = value.size();
    std::memcpy(&bodies_[offset + sizeof(Int)], value.data(), value.size());
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Text>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Text>> ColumnImpl<Text>::create(
    Error *error,
    Table *table,
    String name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, TEXT_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Text>::~ColumnImpl() {}

bool ColumnImpl<Text>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Text>::unset(Int row_id) {
  headers_[row_id] = 0;
}

ColumnImpl<Text>::ColumnImpl() : Column(), headers_(), bodies_() {}

// -- ColumnImpl<Vector<Int>> --

bool ColumnImpl<Vector<Int>>::set(Error *error, Int row_id,
                                  const Datum &datum) {
  if (datum.type() != INT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Int> value = datum.force_int_vector();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long vector is stored in front of the body.
    if (!bodies_.resize(error, offset + 1 + value.size())) {
      return false;
    }
    bodies_[offset] = value.size();
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + 1 + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Vector<Int>>::get(Error *error, Int row_id,
                                  Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<Int>>> ColumnImpl<Vector<Int>>::create(
    Error *error,
    Table *table,
    String name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, INT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Vector<Int>>::~ColumnImpl() {}

bool ColumnImpl<Vector<Int>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Vector<Int>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

ColumnImpl<Vector<Int>>::ColumnImpl() : Column(), headers_(), bodies_() {}

// -- ColumnImpl<Vector<Float>> --

bool ColumnImpl<Vector<Float>>::set(Error *error, Int row_id,
                                    const Datum &datum) {
  if (datum.type() != FLOAT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Float> value = datum.force_float_vector();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long vector is stored in front of the body.
    if (!bodies_.resize(error, offset + 1 + value.size())) {
      return false;
    }
    Int size_for_copy = value.size();
    std::memcpy(&bodies_[offset], &size_for_copy, sizeof(Int));
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + 1 + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Vector<Float>>::get(Error *error, Int row_id,
                                    Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<Float>>> ColumnImpl<Vector<Float>>::create(
    Error *error,
    Table *table,
    String name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               FLOAT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Vector<Float>>::~ColumnImpl() {}

bool ColumnImpl<Vector<Float>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Vector<Float>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

ColumnImpl<Vector<Float>>::ColumnImpl() : Column(), headers_(), bodies_() {}

}  // namespace grnxx
