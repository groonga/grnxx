#include "grnxx/impl/column/scalar/text.hpp"

#include <cstring>
#include <set>

#include "grnxx/impl/table.hpp"
#include "grnxx/impl/index.hpp"

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
    // Remove the old value from indexes.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, old_value);
    }
  }
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    headers_.resize(value_id + 1, na_header());
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
  // TODO: Error handling.
  size_t offset = bodies_.size();
  size_t size = new_value.raw_size();
  uint64_t header;
  if (size < 0xFFFF) {
    bodies_.resize(offset + size);
    std::memcpy(&bodies_[offset], new_value.raw_data(), size);
    header = (offset << 16) | size;
  } else {
    // The size of a long text is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)],
                new_value.raw_data(), size);
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
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    *datum = Text::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Text>::contains(const Datum &datum) const {
  // TODO: Choose the best index.
  Text value = parse_datum(datum);
  if (!indexes_.is_empty()) {
    if (value.is_na()) {
      return table_->num_rows() != indexes_[0]->num_entries();
    }
    return indexes_[0]->contains(datum);
  }
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
  // TODO: Choose the best index.
  if (!indexes_.is_empty()) {
    return indexes_[0]->find_one(datum);
  }
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

void Column<Text>::set_key_attribute() {
  if (is_key_) {
    throw "Key column";  // TODO
  }

  if (!indexes_.is_empty()) {
    if (contains(grnxx::Text::na())) {
      throw "N/A exist";  // TODO
    }
    // TODO: Choose the best index.
    if (!indexes_[0]->test_uniqueness()) {
      throw "Key duplicate";  // TODO
    }
  } else {
    std::set<String> set;
    size_t valid_size = get_valid_size();
    for (size_t i = 0; i < valid_size; ++i) try {
      Text value = get(Int(i));
      if (value.is_na()) {
        if (table_->test_row(grnxx::Int(i))) {
          throw "N/A exist";  // TODO
        }
      } else {
        if (!set.insert(String(value.raw_data(), value.raw_size())).second) {
          throw "Key duplicate";  // TODO
        }
      }
    } catch (const std::bad_alloc &) {
      throw "Memory allocation failed";  // TODO
    }
  }
  is_key_ = true;
}

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
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    headers_.resize(value_id + 1, na_header());
  }
  Text value = parse_datum(key);
  // Update indexes if exist.
  for (size_t i = 0; i < num_indexes(); ++i) try {
    indexes_[i]->insert(row_id, value);
  } catch (...) {
    for (size_t j = 0; j < i; ++j) {
      indexes_[j]->remove(row_id, value);
    }
    throw;
  }
  // TODO: Error handling.
  size_t offset = bodies_.size();
  size_t size = value.raw_size();
  uint64_t header;
  if (size < 0xFFFF) {
    bodies_.resize(offset + size);
    std::memcpy(&bodies_[offset], value.raw_data(), size);
    header = (offset << 16) | size;
  } else {
    // The size of a long text is stored in front of the body.
    if ((offset % sizeof(uint64_t)) != 0) {
      offset += sizeof(uint64_t) - (offset % sizeof(uint64_t));
    }
    bodies_.resize(offset + sizeof(uint64_t) + size);
    *reinterpret_cast<uint64_t *>(&bodies_[offset]) = size;
    std::memcpy(&bodies_[offset + sizeof(uint64_t)], value.raw_data(), size);
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
    // Update indexes if exist.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, value);
    }
    headers_[row_id.raw()] = na_header();
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
  size_t table_size = table_->max_row_id().raw() + 1;
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
