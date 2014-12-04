#include "grnxx/impl/column/scalar/int.hpp"

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/impl/index.hpp"

#include <unordered_set>

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
  if (old_value.match(new_value)) {
    return;
  }
  if (is_key_ && contains(datum)) {
    throw "Key already exists";  // TODO
  }
  if (!old_value.is_na()) {
    // Remove the old value from indexes.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, old_value);
    }
  }
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Int::na());
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

void Column<Int>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    *datum = Int::na();
  } else {
    *datum = values_[value_id];
  }
}

bool Column<Int>::contains(const Datum &datum) const {
  // TODO: Choose the best index.
  if (!indexes_.is_empty()) {
    return indexes_[0]->contains(datum);
  }
  Int value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (values_[i].is_na() && table_->_test_row(i)) {
        return true;
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      if (values_[i].match(value)) {
        return true;
      }
    }
  }
  return false;
}

Int Column<Int>::find_one(const Datum &datum) const {
  // TODO: Choose the best index.
  if (!indexes_.is_empty()) {
    return indexes_[0]->find_one(datum);
  }
  Int value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
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

void Column<Int>::set_key_attribute() {
  if (is_key_) {
    throw "Key column";  // TODO
  }
  if (reference_table_ == table_) {
    throw "Self reference";  // TODO
  }

  // TODO: An index should be used if available.
  std::unordered_set<int64_t> set;
  size_t valid_size = get_valid_size();
  for (size_t i = 0; i < valid_size; ++i) try {
    if (!values_[i].is_na()) {
      if (!set.insert(values_[i].raw()).second) {
        throw "Key duplicate";  // TODO
      }
    }
  } catch (const std::bad_alloc &) {
    throw "Memory allocation failed";  // TODO
  }
  is_key_ = true;
}

void Column<Int>::unset_key_attribute() {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  is_key_ = false;
}

void Column<Int>::set_key(Int row_id, const Datum &key) {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  if (contains(key)) {
    throw "Key already exists";  // TODO
  }
  size_t value_id = row_id.raw();
  if (value_id >= values_.size()) {
    values_.resize(value_id + 1, Int::na());
  }
  Int value = parse_datum(key);
  // Update indexes if exist.
  for (size_t i = 0; i < num_indexes(); ++i) try {
    indexes_[i]->insert(row_id, value);
  } catch (...) {
    for (size_t j = 0; j < i; ++j) {
      indexes_[j]->remove(row_id, value);
    }
    throw;
  }
  values_[value_id] = value;
}

void Column<Int>::unset(Int row_id) {
  Int value = get(row_id);
  if (!value.is_na()) {
    // Update indexes if exist.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, value);
    }
    values_[row_id.raw()] = Int::na();
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

size_t Column<Int>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  if (table_size < values_.size()) {
    return table_size;
  }
  return values_.size();
}

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
