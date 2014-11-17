#include "grnxx/impl/column/vector/bool.hpp"

#include <cstring>

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

namespace grnxx {
namespace impl {

Column<Vector<Bool>>::Column(Table *table,
                             const String &name,
                             const ColumnOptions &)
    : ColumnBase(table, name, BOOL_VECTOR_DATA),
      headers_(),
      bodies_() {}

Column<Vector<Bool>>::~Column() {}

void Column<Vector<Bool>>::set(Int row_id, const Datum &datum) {
  Vector<Bool> new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  Vector<Bool> old_value = get(row_id);
  if ((old_value == new_value).is_true()) {
    return;
  }
  if (!old_value.is_na()) {
    // TODO: Remove the old value from indexes.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, old_value);
//    }
  }
  size_t value_id = row_id.value();
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
  size_t size = new_value.size().value();
  uint64_t header;
  if (size < 0xFFFF) {
    bodies_.resize(offset + size);
    std::memcpy(&bodies_[offset], new_value.data(), size);
    header = (offset << 16) | size;
  } else {
    // The size of a long vector is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)], new_value.data(), size);
    header = (offset << 16) | 0xFFFF;
  }
  headers_[value_id] = header;
}

void Column<Vector<Bool>>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.value();
  if (value_id >= headers_.size()) {
    *datum = Vector<Bool>::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Vector<Bool>>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Bool> value = parse_datum(datum);
  if (value.is_na()) {
    for (size_t i = 0; i < headers_.size(); ++i) {
      if (headers_[i] == na_header()) {
        return true;
      }
    }
  } else {
    for (size_t i = 0; i < headers_.size(); ++i) {
      // TODO: Improve this.
      if ((get(Int(i)) == value).is_true()) {
        return true;
      }
    }
  }
  return false;
}

Int Column<Vector<Bool>>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Bool> value = parse_datum(datum);
  if (value.is_na()) {
    for (size_t i = 0; i < headers_.size(); ++i) {
      if (headers_[i] == na_header()) {
        return Int(i);
      }
    }
  } else {
    for (size_t i = 0; i < headers_.size(); ++i) {
      // TODO: Improve this.
      if ((get(Int(i)) == value).is_true()) {
        return Int(i);
      }
    }
  }
  return Int::na();
}

void Column<Vector<Bool>>::unset(Int row_id) {
  Vector<Bool> value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    headers_[row_id.value()] = na_header();
  }
}

void Column<Vector<Bool>>::read(ArrayCRef<Record> records,
                                ArrayRef<Vector<Bool>> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

Vector<Bool> Column<Vector<Bool>>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Vector<Bool>::na();
    }
    case BOOL_VECTOR_DATA: {
      return datum.as_bool_vector();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
