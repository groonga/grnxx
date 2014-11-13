#include "grnxx/impl/column/scalar/float.hpp"

#include "grnxx/impl/table.hpp"
//#include "grnxx/index.hpp"

namespace grnxx {
namespace impl {

Column<Float>::Column(Table *table,
                      const String &name,
                      const ColumnOptions &)
    : ColumnBase(table, name, FLOAT_DATA),
      values_() {}

Column<Float>::~Column() {}

void Column<Float>::set(Int row_id, const Datum &datum) {
  Float new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  Float old_value = get(row_id);
  if (old_value.value() == new_value.value()) {
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
    values_.resize(value_id + 1, Float::na());
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

void Column<Float>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    *datum = Float::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Float>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Float value = parse_datum(datum);
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

Int Column<Float>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Float value = parse_datum(datum);
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

void Column<Float>::unset(Int row_id) {
  Float value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    values_[row_id.value()] = Float::na();
  }
}

void Column<Float>::read(ArrayCRef<Record> records,
                         ArrayRef<Float> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

Float Column<Float>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Float::na();
    }
    case FLOAT_DATA: {
      return datum.as_float();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
