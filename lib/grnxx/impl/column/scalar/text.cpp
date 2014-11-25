#include "grnxx/impl/column/scalar/text.hpp"

#include <cstring>
#include <set>

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

namespace grnxx {
namespace impl {

Column<Text>::Column(Table *table,
                     const String &name,
                     const ColumnOptions &)
    : ColumnBase(table, name, TEXT_DATA),
      headers_(),
      bodies_() {}

Column<Text>::~Column() {}

void Column<Text>::set(Int row_id, const Datum &datum) {
  Text new_value = parse_datum(datum);
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
  Text old_value = get(row_id);
  if (old_value.match(new_value)) {
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
    // The size of a long text is stored in front of the body.
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

//bool Column<Text>::set(Error *error, Int row_id, const Datum &datum) {
//  if (datum.type() != TEXT_DATA) {
//    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
//    return false;
//  }
//  if (!table_->test_row(error, row_id)) {
//    return false;
//  }
//  Text old_value = get(row_id);
//  Text new_value = datum.force_text();
//  if (new_value != old_value) {
//    if (has_key_attribute_ && contains(datum)) {
//      GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
//      return false;
//    }
//    for (Int i = 0; i < num_indexes(); ++i) {
//      if (!indexes_[i]->insert(error, row_id, datum)) {
//        for (Int j = 0; j < i; ++i) {
//          indexes_[j]->remove(nullptr, row_id, datum);
//        }
//        return false;
//      }
//    }
//    Int offset = bodies_.size();
//    UInt new_header;
//    if (new_value.size() < 0xFFFF) {
//      if (!bodies_.resize(error, offset + new_value.size())) {
//        return false;
//      }
//      std::memcpy(&bodies_[offset], new_value.data(), new_value.size());
//      new_header = (offset << 16) | new_value.size();
//    } else {
//      // The size of a long text is stored in front of the body.
//      if ((offset % sizeof(Int)) != 0) {
//        offset += sizeof(Int) - (offset % sizeof(Int));
//      }
//      if (!bodies_.resize(error, offset + sizeof(Int) + new_value.size())) {
//        return false;
//      }
//      *reinterpret_cast<Int *>(&bodies_[offset]) = new_value.size();
//      std::memcpy(&bodies_[offset + sizeof(Int)],
//                  new_value.data(), new_value.size());
//      new_header = (offset << 16) | 0xFFFF;
//    }
//    for (Int i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(nullptr, row_id, old_value);
//    }
//    headers_[row_id] = new_header;
//  }
//  return true;
//}

void Column<Text>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.value();
  if (value_id >= headers_.size()) {
    *datum = Text::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Text>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Text value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i] == na_header()) {
        return true;
      }
    }
  } else {
    for (size_t i = 0; i < valid_size; ++i) {
      // TODO: Improve this (get() checks the range of its argument).
      if (get(Int(i)).match(value)) {
        return true;
      }
    }
  }
  return false;
}

Int Column<Text>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Text value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i] == na_header()) {
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

//Int Column<Text>::find_one(const Datum &datum) const {
//  // TODO: Cursor should not be used because it takes time.
//  // Also, cursor operations can fail due to memory allocation.
//  Text value = datum.force_text();
//  if (indexes_.size() != 0) {
//    auto cursor = indexes_[0]->find(nullptr, value);
//    Array<Record> records;
//    auto result = cursor->read(nullptr, 1, &records);
//    if (!result.is_ok || (result.count == 0)) {
//      return NULL_ROW_ID;
//    }
//    return true;
//  } else {
//    // TODO: A full scan takes time.
//    // An index should be required for a key column.

//    // TODO: Functor-based inline callback may be better in this case,
//    // because it does not require memory allocation.

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
//        if (get(records.get_row_id(i)) == value) {
//          return records.get_row_id(i);
//        }
//      }
//      records.clear();
//    }
//  }
//  return NULL_ROW_ID;
//}

void Column<Text>::set_key_attribute() {
  if (is_key_) {
    throw "Key column";  // TODO
  }
  // TODO: An index should be used if available.
  std::set<String> set;
  size_t size = headers_.size();
  if (table_->max_row_id().is_na()) {
    size = 0;
  } else if (static_cast<size_t>(table_->max_row_id().value()) < size) {
    size = static_cast<size_t>(table_->max_row_id().value()) + 1;
  }
  for (size_t i = 0; i < size; ++i) try {
    Text value = get(grnxx::Int(i));
    if (!value.is_na()) {
      if (!set.insert(String(value.data(), value.size().value())).second) {
        throw "Key duplicate";  // TODO
      }
    }
  } catch (const std::bad_alloc &) {
    throw "Memory allocation failed";  // TODO
  }
  is_key_ = true;
}

//bool Column<Text>::set_key_attribute(Error *error) {
//  if (has_key_attribute_) {
//    GRNXX_ERROR_SET(error, INVALID_OPERATION,
//                    "This column is a key column");
//    return false;
//  }
//  // TODO: An index should be used if possible.
//  try {
//    std::set<Text> set;
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
//        if (!set.insert(get(records.get_row_id(i))).second) {
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
//}

void Column<Text>::unset_key_attribute() {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  is_key_ = false;
}

void Column<Text>::set_key(Int row_id, const Datum &key) {
  if (!is_key_) {
    throw "Not key column";  // TODO
  }
  if (contains(key)) {
    throw "Key already exists";  // TODO
  }
  size_t value_id = row_id.value();
  if (value_id >= headers_.size()) {
    headers_.resize(value_id + 1, na_header());
  }
  // TODO: N/A is not available.
  Text value = parse_datum(key);
  // TODO: Update indexes if exist.
//  for (size_t i = 0; i < num_indexes(); ++i) try {
//    indexes_[i]->insert(row_id, value);
//  } catch (...) {
//    for (size_t j = 0; j < i; ++j) {
//      indexes_[j]->remove(row_id, value);
//    }
//    throw;
//  }
  // TODO: Error handling.
  size_t offset = bodies_.size();
  size_t size = value.size().value();
  uint64_t header;
  if (size < 0xFFFF) {
    bodies_.resize(offset + size);
    std::memcpy(&bodies_[offset], value.data(), size);
    header = (offset << 16) | size;
  } else {
    // The size of a long text is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)], value.data(), size);
    header = (offset << 16) | 0xFFFF;
  }
  headers_[value_id] = header;
}

//bool Column<Text>::set_initial_key(Error *error,
//    Int row_id,
//    const Datum &key) {
//  if (!has_key_attribute_) {
//    GRNXX_ERROR_SET(error, INVALID_OPERATION,
//                    "This column is not a key column");
//    return false;
//  }
//  if (has_key_attribute_ && contains(key)) {
//    GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
//    return false;
//  }
//  if (row_id >= headers_.size()) {
//    if (!headers_.resize(error, row_id + 1, 0)) {
//      return false;
//    }
//  }
//  Text value = key.force_text();
//  for (Int i = 0; i < num_indexes(); ++i) {
//    if (!indexes_[i]->insert(error, row_id, value)) {
//      for (Int j = 0; j < i; ++j) {
//        indexes_[j]->remove(nullptr, row_id, value);
//      }
//      return false;
//    }
//  }
//  Int offset = bodies_.size();
//  UInt header;
//  if (value.size() < 0xFFFF) {
//    if (!bodies_.resize(error, offset + value.size())) {
//      return false;
//    }
//    std::memcpy(&bodies_[offset], value.data(), value.size());
//    header = (offset << 16) | value.size();
//  } else {
//    // The size of a long text is stored in front of the body.
//    if ((offset % sizeof(Int)) != 0) {
//      offset += sizeof(Int) - (offset % sizeof(Int));
//    }
//    if (!bodies_.resize(error, offset + sizeof(Int) + value.size())) {
//      return false;
//    }
//    *reinterpret_cast<Int *>(&bodies_[offset]) = value.size();
//    std::memcpy(&bodies_[offset + sizeof(Int)], value.data(), value.size());
//    header = (offset << 16) | 0xFFFF;
//  }
//  headers_[row_id] = header;
//  return true;
//}

void Column<Text>::unset(Int row_id) {
  Text value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    headers_[row_id.value()] = na_header();
  }
}

void Column<Text>::read(ArrayCRef<Record> records,
                        ArrayRef<Text> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

size_t Column<Text>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().value() + 1;
  if (table_size < headers_.size()) {
    return table_size;
  }
  return headers_.size();
}

Text Column<Text>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Text::na();
    }
    case TEXT_DATA: {
      return datum.as_text();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
