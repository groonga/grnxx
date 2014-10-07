#include "grnxx/impl/column/column_vector_text.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

bool Column<Vector<Text>>::set(Error *error, Int row_id,
                               const Datum &datum) {
  if (datum.type() != TEXT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Text> value = datum.force_text_vector();
  if (value.size() == 0) {
    headers_[row_id] = Header{ 0, 0 };
    return true;
  }
  Int text_headers_offset = text_headers_.size();
  if (!text_headers_.resize(error, text_headers_offset + value.size())) {
    return false;
  }
  Int total_size = 0;
  for (Int i = 0; i < value.size(); ++i) {
    total_size += value[i].size();
  }
  Int bodies_offset = bodies_.size();
  if (!bodies_.resize(error, bodies_offset + total_size)) {
    return false;
  }
  headers_[row_id] = Header{ text_headers_offset, value.size() };
  for (Int i = 0; i < value.size(); ++i) {
    text_headers_[text_headers_offset + i].offset = bodies_offset;
    text_headers_[text_headers_offset + i].size = value[i].size();
    std::memcpy(&bodies_[bodies_offset], value[i].data(), value[i].size());
    bodies_offset += value[i].size();
  }
  return true;
}

bool Column<Vector<Text>>::get(Error *error, Int row_id,
                               Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<Column<Vector<Text>>> Column<Vector<Text>>::create(
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
                               TEXT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1,
                               Header{ 0, 0 })) {
    return nullptr;
  }
  return column;
}

Column<Vector<Text>>::~Column() {}

bool Column<Vector<Text>>::set_default_value(Error *error,
                                                     Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = Header{ 0, 0 };
  return true;
}

void Column<Vector<Text>>::unset(Int row_id) {
  headers_[row_id] = Header{ 0, 0 };
}

Column<Vector<Text>>::Column()
    : ColumnBase(),
      headers_(),
      text_headers_(),
      bodies_() {}

}  // namespace impl
}  // namespace grnxx
