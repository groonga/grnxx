#include "grnxx/impl/column/scalar/bool.hpp"

#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

Column<Bool>::Column(Table *table,
                     const String &name,
                     const ColumnOptions &)
    : ColumnBase(table, name, BOOL_DATA),
      values_() {}

Column<Bool>::~Column() {}

void Column<Bool>::set(Int row_id, const Datum &datum) {
  Bool value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Bool::na());
  }
  values_[value_id] = value;
  // TODO: Update indexes if exist.
}

void Column<Bool>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    *datum = Bool::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Bool>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Bool value = parse_datum(datum);
  for (size_t i = 0; i < values_.size(); ++i) {
    if (values_[i].value() == value.value()) {
      return true;
    }
  }
  return false;
}

Int Column<Bool>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Bool value = parse_datum(datum);
  for (size_t i = 0; i < values_.size(); ++i) {
    if (values_[i] == value) {
      return Int(i);
    }
  }
  return Int::na();
}

void Column<Bool>::unset(Int row_id) {
  size_t value_id = row_id.value();
  values_[value_id] = Bool::na();
  // TODO: Update indexes if exist.
}

void Column<Bool>::read(ArrayCRef<Record> records,
                        ArrayRef<Bool> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

Bool Column<Bool>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Bool::na();
    }
    case BOOL_DATA: {
      return datum.as_bool();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
