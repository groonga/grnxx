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
  Bool new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  Bool old_value = get(row_id);
  if (old_value == new_value) {
    return;
  }
  if (!old_value.is_na()) {
    // TODO: Remove the old value from indexes.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, old_value);
//    }
  }
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Bool::na());
  }
  // TODO: Insert the new value into indexes.
//  for (size_t i = 0; i < num_indexes(); ++i) try {
//    indexes_[i]->insert(row_id, datum)) {
//  } catch (...) {
//    for (size_t j = 0; j < i; ++i) {
//      indexes_[j]->remove(row_id, datum);
//    }
//    throw;
//  }
  values_[value_id] = new_value;
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
  if (value.is_na()) {
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].is_na() && table_->_test_row(i)) {
        return true;
      }
    }
  } else {
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].value() == value.value()) {
        return true;
      }
    }
  }
  return false;
}

Int Column<Bool>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Bool value = parse_datum(datum);
  if (value.is_na()) {
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].is_na() && table_->_test_row(i)) {
        return Int(i);
      }
    }
  } else {
    for (size_t i = 0; i < values_.size(); ++i) {
      if (values_[i].value() == value.value()) {
        return Int(i);
      }
    }
  }
  return Int::na();
}

void Column<Bool>::unset(Int row_id) {
  Bool value = get(row_id);
  if (!value.is_na()) {
    values_[row_id.value()] = Bool::na();
    // TODO: Update indexes if exist.
  }
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
