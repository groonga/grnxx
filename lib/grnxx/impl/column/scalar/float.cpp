#include "grnxx/impl/column/scalar/float.hpp"

#include "grnxx/impl/table.hpp"
#include "grnxx/impl/index.hpp"

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
  if (old_value.match(new_value)) {
    return;
  }
  if (!old_value.is_na()) {
    // Remove the old value from indexes.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, old_value);
    }
  }
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Float::na());
  }
  // Insert the new value into indexes.
  for (size_t i = 0; i < num_indexes(); ++i) try {
    indexes_[i]->insert(row_id, datum);
  } catch (...) {
    for (size_t j = 0; j < i; ++i) {
      indexes_[j]->remove(row_id, datum);
    }
    throw;
  }
  values_[value_id] = new_value;
}

void Column<Float>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    *datum = Float::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Float>::contains(const Datum &datum) const {
  // TODO: Choose the best index.
  Float value = parse_datum(datum);
  if (!indexes_.is_empty()) {
    if (value.is_na()) {
      return table_->num_rows() != indexes_[0]->num_entries();
    }
    return indexes_[0]->contains(datum);
  }
  return !scan(value).is_na();
}

Int Column<Float>::find_one(const Datum &datum) const {
  // TODO: Choose the best index.
  Float value = parse_datum(datum);
  if (!value.is_na() && !indexes_.is_empty()) {
    return indexes_[0]->find_one(datum);
  }
  return scan(value);
}

void Column<Float>::unset(Int row_id) {
  Float value = get(row_id);
  if (!value.is_na()) {
    // Update indexes if exist.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, value);
    }
    values_[row_id.raw()] = Float::na();
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

Int Column<Float>::scan(Float value) const {
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
    for (size_t i = 0; i < valid_size; ++i) {
      if (values_[i].is_na() && table_->_test_row(i)) {
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
