#include "grnxx/impl/column/vector/float.hpp"

#include <cstring>

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

namespace grnxx {
namespace impl {

Column<Vector<Float>>::Column(Table *table,
                              const String &name,
                              const ColumnOptions &)
    : ColumnBase(table, name, FLOAT_VECTOR_DATA),
      headers_(),
      bodies_() {}

Column<Vector<Float>>::~Column() {}

void Column<Vector<Float>>::set(Int row_id, const Datum &datum) {
  Vector<Float> new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  Vector<Float> old_value = get(row_id);
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
  if (value_id >= headers_.size()) {
    headers_.resize(value_id + 1, na_header());
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
  // TODO: Error handling.
  size_t offset = bodies_.size();
  size_t size = new_value.raw_size();
  uint64_t header;
  if (size < 0xFFFF) {
    bodies_.resize(offset + size);
    std::memcpy(&bodies_[offset], new_value.raw_data(), sizeof(Float) * size);
    header = (offset << 16) | size;
  } else {
    // The size of a long vector is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)],
                new_value.raw_data(), sizeof(Float) * size);
    header = (offset << 16) | 0xFFFF;
  }
  headers_[value_id] = header;
}

void Column<Vector<Float>>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    *datum = Vector<Float>::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Vector<Float>>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  return !scan(parse_datum(datum)).is_na();
}

Int Column<Vector<Float>>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  return scan(parse_datum(datum));
}

void Column<Vector<Float>>::unset(Int row_id) {
  Vector<Float> value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    headers_[row_id.raw()] = na_header();
  }
}

void Column<Vector<Float>>::read(ArrayCRef<Record> records,
                                 ArrayRef<Vector<Float>> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

Int Column<Vector<Float>>::scan(const Vector<Float> &value) const {
  if (table_->max_row_id().is_na()) {
    return Int::na();
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  size_t valid_size =
      (headers_.size() < table_size) ? headers_.size() : table_size;
  if (value.is_na()) {
    if (headers_.size() < table_size) {
      return table_->max_row_id();
    }
    bool is_full = table_->is_full();
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i] == na_header() && (is_full || table_->_test_row(i))) {
        return Int(i);
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      // TODO: Improve this (get() checks the range of its argument).
      if (get(Int(i)).match(value)) {
        return Int(i);
      }
    }
  }
  return Int::na();
}

size_t Column<Vector<Float>>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  if (table_size < headers_.size()) {
    return table_size;
  }
  return headers_.size();
}

Vector<Float> Column<Vector<Float>>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Vector<Float>::na();
    }
    case FLOAT_VECTOR_DATA: {
      return datum.as_float_vector();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
