#include "grnxx/impl/column/scalar/bool.hpp"

#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

Column<Bool>::Column(Table *table,
                     const String &name,
                     const ColumnOptions &)
    : ColumnBase(table, name, GRNXX_BOOL),
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
  if (old_value.match(new_value)) {
    return;
  }
  if (!old_value.is_na()) {
    // TODO: Remove the old value from indexes.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, old_value);
//    }
  }
  size_t value_id = row_id.raw();
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
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    *datum = Bool::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Bool>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  return !scan(parse_datum(datum)).is_na();
}

Int Column<Bool>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  return scan(parse_datum(datum));
}

void Column<Bool>::unset(Int row_id) {
  Bool value = get(row_id);
  if (!value.is_na()) {
    values_[row_id.raw()] = Bool::na();
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

Int Column<Bool>::scan(Bool value) const {
  if (table_->max_row_id().is_na()) {
    return Int::na();
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  size_t valid_size =
      (values_.size() < table_size) ? values_.size() : table_size;
  if (value.is_na()) {
    if (values_.size() < table_size) {
      return table_->max_row_id();
    }
    bool is_full = table_->is_full();
    for (size_t i = 0; i < valid_size; ++i) {
      if (values_[i].is_na() && (is_full || table_->_test_row(i))) {
        return Int(i);
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      if (values_[i].match(value)) {
        return Int(i);
      }
    }
  }
  return Int::na();
}

Bool Column<Bool>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case GRNXX_NA: {
      return Bool::na();
    }
    case GRNXX_BOOL: {
      return datum.as_bool();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
