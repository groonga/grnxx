#include "grnxx/impl/column/vector/int.hpp"

#include <cstring>

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

namespace grnxx {
namespace impl {

Column<Vector<Int>>::Column(Table *table,
                            const String &name,
                            const ColumnOptions &options)
    : ColumnBase(table, name, INT_VECTOR_DATA),
      headers_(),
      bodies_() {
  if (!options.reference_table_name.is_empty()) {
    reference_table_ = table->_db()->find_table(options.reference_table_name);
    if (!reference_table_) {
      throw "Table not found";  // TODO
    }
  }
}

Column<Vector<Int>>::~Column() {}

void Column<Vector<Int>>::set(Int row_id, const Datum &datum) {
  Vector<Int> new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  if (reference_table_) {
    size_t new_value_size = new_value.raw_size();
    for (size_t i = 0; i < new_value_size; ++i) {
      if (!reference_table_->test_row(new_value[i])) {
        throw "Invalid reference";  // TODO
      }
    }
  }
  Vector<Int> old_value = get(row_id);
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
    std::memcpy(&bodies_[offset], new_value.data(), sizeof(Int) * size);
    header = (offset << 16) | size;
  } else {
    // The size of a long vector is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)],
                new_value.data(), sizeof(Int) * size);
    header = (offset << 16) | 0xFFFF;
  }
  headers_[value_id] = header;
}

void Column<Vector<Int>>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    *datum = Vector<Int>::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Vector<Int>>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Int> value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i] == na_header()) {
        return true;
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      // TODO: Improve this.
      if (get(Int(i)).match(value)) {
        return true;
      }
    }
  }
  return false;
}

Int Column<Vector<Int>>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Int> value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i] == na_header()) {
        return Int(i);
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      // TODO: Improve this.
      if (get(Int(i)).match(value)) {
        return Int(i);
      }
    }
  }
  return Int::na();
}

void Column<Vector<Int>>::unset(Int row_id) {
  Vector<Int> value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    headers_[row_id.raw()] = na_header();
  }
}

void Column<Vector<Int>>::read(ArrayCRef<Record> records,
                               ArrayRef<Vector<Int>> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

size_t Column<Vector<Int>>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  if (table_size < headers_.size()) {
    return table_size;
  }
  return headers_.size();
}

Vector<Int> Column<Vector<Int>>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Vector<Int>::na();
    }
    case INT_VECTOR_DATA: {
      return datum.as_int_vector();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

//void Column<Vector<Int>>::clear_references(Int row_id) {
//  auto cursor = table_->create_cursor(nullptr);
//  if (!cursor) {
//    // Error.
//    return;
//  }
//  Array<Record> records;
//  for ( ; ; ) {
//    auto result = cursor->read(nullptr, 1024, &records);
//    if (!result.is_ok) {
//      // Error.
//      return;
//    } else if (result.count == 0) {
//      return;
//    }
//    for (Int i = 0; i < records.size(); ++i) {
//      Int value_row_id = records.get_row_id(i);
//      Int value_size = static_cast<Int>(headers_[value_row_id] & 0xFFFF);
//      if (value_size == 0) {
//        continue;
//      }
//      Int value_offset = static_cast<Int>(headers_[value_row_id] >> 16);
//      if (value_size >= 0xFFFF) {
//        value_size = bodies_[value_offset];
//        ++value_offset;
//      }
//      Int count = 0;
//      for (Int j = 0; j < value_size; ++j) {
//        if (bodies_[value_offset + j] != row_id) {
//          bodies_[value_offset + count] = bodies_[value_offset + j];
//          ++count;
//        }
//      }
//      if (count < value_size) {
//        if (count == 0) {
//          headers_[value_row_id] = 0;
//        } else if (count < 0xFFFF) {
//          headers_[value_row_id] = count | (value_offset << 16);
//        } else {
//          bodies_[value_offset - 1] = count;
//        }
//      }
//    }
//    records.clear();
//  }
//}

}  // namespace impl
}  // namespace grnxx
