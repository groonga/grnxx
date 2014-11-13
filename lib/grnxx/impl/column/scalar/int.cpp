#include "grnxx/impl/column/scalar/int.hpp"

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

//#include <unordered_set>

namespace grnxx {
namespace impl {

Column<Int>::Column(Table *table,
                    const String &name,
                    const ColumnOptions &options)
    : ColumnBase(table, name, INT_DATA),
      values_() {
  if (!options.reference_table_name.is_empty()) {
    reference_table_ = table->_db()->find_table(options.reference_table_name);
    if (!reference_table_) {
      throw "Table not found";  // TODO
    }
  }
}

Column<Int>::~Column() {}

void Column<Int>::set(Int row_id, const Datum &datum) {
  Int new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (is_key_) {
    if (new_value.is_na()) {
      throw "N/A key";  // TODO
    }
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  if (reference_table_) {
    if (!reference_table_->test_row(new_value)) {
      throw "Invalid reference";  // TODO
    }
  }
  Int old_value = get(row_id);
  if (old_value.value() == new_value.value()) {
    return;
  }
  if (is_key_ && contains(datum)) {
    throw "Key already exists";  // TODO
  }
  if (!old_value.is_na()) {
    // TODO: Remove the old value from indexes.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, old_value);
//    }
  }
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Int::na());
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

void Column<Int>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    *datum = Int::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Int>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Int value = parse_datum(datum);
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

Int Column<Int>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Int value = parse_datum(datum);
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

//  // TODO: Cursor should not be used because it takes time.
//  //       Also, cursor operations can fail due to memory allocation.
//  Int value = datum.force_int();
//  if (indexes_.size() != 0) {
//    return indexes_[0]->find_one(datum);
//  } else {
//    // TODO: A full scan takes time.
//    //       An index should be required for a key column.

//    // TODO: Functor-based inline callback may be better in this case,
//    //       because it does not require memory allocation.

//    // Scan the column to find "value".
//    auto cursor = table_->create_cursor(nullptr);
//    if (!cursor) {
//      return NULL_ROW_ID;
//    }
//    Array<Record> records;
//    for ( ; ; ) {
//      auto result = cursor->read(nullptr, 1024, &records);
//      if (!result.is_ok || result.count == 0) {
//        return NULL_ROW_ID;
//      }
//      for (Int i = 0; i < result.count; ++i) {
//        if (values_[records.get_row_id(i)] == value) {
//          return records.get_row_id(i);
//        }
//      }
//      records.clear();
//    }
//  }
//  return NULL_ROW_ID;
}

void Column<Int>::set_key_attribute() {
  if (is_key_) {
    throw "Key column";  // TODO
  }
  throw "Not supported yet";  // TODO

//  // TODO: An index should be used if possible.
//  try {
//    std::unordered_set<Int> set;
//    // TODO: Functor-based inline callback may be better in this case,
//    //       because it does not require memory allocation.
//    auto cursor = table_->create_cursor(nullptr);
//    if (!cursor) {
//      return false;
//    }
//    Array<Record> records;
//    for ( ; ; ) {
//      auto result = cursor->read(nullptr, 1024, &records);
//      if (!result.is_ok) {
//        return false;
//      } else {
//        break;
//      }
//      for (Int i = 0; i < result.count; ++i) {
//        if (!set.insert(values_[records.get_row_id(i)]).second) {
//          GRNXX_ERROR_SET(error, INVALID_OPERATION, "Key duplicate");
//          return false;
//        }
//      }
//      records.clear();
//    }
//  } catch (...) {
//    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
//    return false;
//  }
//  has_key_attribute_ = true;
//  return true;
}

void Column<Int>::unset_key_attribute() {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  is_key_ = true;
}

void Column<Int>::set_key(Int row_id, const Datum &key) {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  if (contains(key)) {
    throw "Key already exists";  // TODO
  }
  size_t value_id = row_id.value();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Int::na());
  }
  // TODO: N/A is not available.
  Int value = parse_datum(key);
  // TODO: Update indexes if exist.
//  for (size_t i = 0; i < num_indexes(); ++i) try {
//    indexes_[i]->insert(row_id, value);
//  } catch (...) {
//    for (size_t j = 0; j < i; ++j) {
//      indexes_[j]->remove(row_id, value);
//    }
//    throw;
//  }
  values_[value_id] = value;
}

void Column<Int>::unset(Int row_id) {
  Int value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    values_[row_id.value()] = Int::na();
  }
}

void Column<Int>::read(ArrayCRef<Record> records, ArrayRef<Int> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

//void Column<Int>::clear_references(Int row_id) {
//  // TODO: Cursor should not be used to avoid errors.
//  if (indexes_.size() != 0) {
//    auto cursor = indexes_[0]->find(nullptr, Int(0));
//    if (!cursor) {
//      // Error.
//      return;
//    }
//    Array<Record> records;
//    for ( ; ; ) {
//      auto result = cursor->read(nullptr, 1024, &records);
//      if (!result.is_ok) {
//        // Error.
//        return;
//      } else if (result.count == 0) {
//        return;
//      }
//      for (Int i = 0; i < records.size(); ++i) {
//        set(nullptr, row_id, NULL_ROW_ID);
//      }
//      records.clear();
//    }
//  } else {
//    auto cursor = table_->create_cursor(nullptr);
//    if (!cursor) {
//      // Error.
//      return;
//    }
//    Array<Record> records;
//    for ( ; ; ) {
//      auto result = cursor->read(nullptr, 1024, &records);
//      if (!result.is_ok) {
//        // Error.
//        return;
//      } else if (result.count == 0) {
//        return;
//      }
//      for (Int i = 0; i < records.size(); ++i) {
//        if (values_[records.get_row_id(i)] == row_id) {
//          values_[records.get_row_id(i)] = NULL_ROW_ID;
//        }
//      }
//      records.clear();
//    }
//  }
//}

Int Column<Int>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Int::na();
    }
    case INT_DATA: {
      return datum.as_int();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
