#include "grnxx/impl/column/scalar/geo_point.hpp"

#include "grnxx/impl/table.hpp"
//#include "grnxx/index.hpp"

namespace grnxx {
namespace impl {

Column<GeoPoint>::Column(Table *table,
                      const String &name,
                      const ColumnOptions &)
    : ColumnBase(table, name, GRNXX_GEO_POINT),
      values_() {}

Column<GeoPoint>::~Column() {}

void Column<GeoPoint>::set(Int row_id, const Datum &datum) {
  GeoPoint new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  GeoPoint old_value = get(row_id);
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
    values_.resize(value_id + 1, GeoPoint::na());
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

void Column<GeoPoint>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    *datum = GeoPoint::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<GeoPoint>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  return !scan(parse_datum(datum)).is_na();
}

Int Column<GeoPoint>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  return scan(parse_datum(datum));
}

void Column<GeoPoint>::unset(Int row_id) {
  GeoPoint value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    values_[row_id.raw()] = GeoPoint::na();
  }
}

void Column<GeoPoint>::read(ArrayCRef<Record> records,
                            ArrayRef<GeoPoint> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

Int Column<GeoPoint>::scan(GeoPoint value) const {
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

GeoPoint Column<GeoPoint>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case GRNXX_NA: {
      return GeoPoint::na();
    }
    case GRNXX_GEO_POINT: {
      return datum.as_geo_point();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
