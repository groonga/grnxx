#include "grnxx/impl/column/column_vector_int.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

bool Column<Vector<Int>>::set(Error *error, Int row_id,
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
  if (ref_table_) {
    for (Int i = 0; i < value.size(); ++i) {
      if (!ref_table_->test_row(error, value[i])) {
        return false;
      }
    }
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

bool Column<Vector<Int>>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<Column<Vector<Int>>> Column<Vector<Int>>::create(
    Error *error,
    Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
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
  if (column->ref_table()) {
    if (!column->ref_table_->append_referrer_column(error, column.get())) {
      return nullptr;
    }
  }
  return column;
}

Column<Vector<Int>>::~Column() {}

bool Column<Vector<Int>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void Column<Vector<Int>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

void Column<Vector<Int>>::clear_references(Int row_id) {
  auto cursor = table_->create_cursor(nullptr);
  if (!cursor) {
    // Error.
    return;
  }
  Array<Record> records;
  for ( ; ; ) {
    auto result = cursor->read(nullptr, 1024, &records);
    if (!result.is_ok) {
      // Error.
      return;
    } else if (result.count == 0) {
      return;
    }
    for (Int i = 0; i < records.size(); ++i) {
      Int value_row_id = records.get_row_id(i);
      Int value_size = static_cast<Int>(headers_[value_row_id] & 0xFFFF);
      if (value_size == 0) {
        continue;
      }
      Int value_offset = static_cast<Int>(headers_[value_row_id] >> 16);
      if (value_size >= 0xFFFF) {
        value_size = bodies_[value_offset];
        ++value_offset;
      }
      Int count = 0;
      for (Int j = 0; j < value_size; ++j) {
        if (bodies_[value_offset + j] != row_id) {
          bodies_[value_offset + count] = bodies_[value_offset + j];
          ++count;
        }
      }
      if (count < value_size) {
        if (count == 0) {
          headers_[value_row_id] = 0;
        } else if (count < 0xFFFF) {
          headers_[value_row_id] = count | (value_offset << 16);
        } else {
          bodies_[value_offset - 1] = count;
        }
      }
    }
    records.clear();
  }
}

Column<Vector<Int>>::Column() : ColumnBase(), headers_(), bodies_() {}

}  // namespace impl
}  // namespace grnxx
