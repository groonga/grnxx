#include "grnxx/impl/column/column_vector_float.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {


bool Column<Vector<Float>>::set(Error *error, Int row_id, const Datum &datum) {
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

bool Column<Vector<Float>>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<Column<Vector<Float>>> Column<Vector<Float>>::create(
    Error *error,
    Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
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

Column<Vector<Float>>::~Column() {}

bool Column<Vector<Float>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void Column<Vector<Float>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

Column<Vector<Float>>::Column()
    : ColumnBase(),
      headers_(),
      bodies_() {}

}  // namespace impl
}  // namespace grnxx
