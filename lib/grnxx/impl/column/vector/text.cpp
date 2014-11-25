#include "grnxx/impl/column/vector/text.hpp"

#include <cstring>

#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
//#include "grnxx/impl/index.hpp"

namespace grnxx {
namespace impl {

Column<Vector<Text>>::Column(Table *table,
                             const String &name,
                             const ColumnOptions &)
    : ColumnBase(table, name, TEXT_VECTOR_DATA),
      headers_(),
      bodies_() {}

Column<Vector<Text>>::~Column() {}

void Column<Vector<Text>>::set(Int row_id, const Datum &datum) {
  Vector<Text> new_value = parse_datum(datum);
  if (!table_->test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  if (new_value.is_na()) {
    unset(row_id);
    return;
  }
  Vector<Text> old_value = get(row_id);
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
  size_t new_value_size = new_value.size().raw();
  size_t text_headers_offset = text_headers_.size();
  text_headers_.resize(text_headers_offset + new_value_size);
  size_t total_size = 0;
  for (size_t i = 0; i < new_value_size; ++i) {
    if (!new_value[i].is_na()) {
      total_size += new_value[i].raw_size();
    }
  }
  size_t bodies_offset = bodies_.size();
  bodies_.resize(bodies_offset + total_size);
  headers_[value_id] = Header{ text_headers_offset, new_value.size() };
  for (size_t i = 0; i < new_value_size; ++i) {
    text_headers_[text_headers_offset + i].offset = bodies_offset;
    text_headers_[text_headers_offset + i].size = new_value[i].size();
    if (!new_value[i].is_na()) {
      std::memcpy(&bodies_[bodies_offset],
                  new_value[i].data(), new_value[i].raw_size());
      bodies_offset += new_value[i].raw_size();
    }
  }
}

void Column<Vector<Text>>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  if (value_id >= headers_.size()) {
    *datum = Vector<Text>::na();
  } else {
    // TODO
    *datum = get(row_id);
  }
}

bool Column<Vector<Text>>::contains(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Text> value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i].size.is_na()) {
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

Int Column<Vector<Text>>::find_one(const Datum &datum) const {
  // TODO: Use an index if exists.
  Vector<Text> value = parse_datum(datum);
  size_t valid_size = get_valid_size();
  if (value.is_na()) {
    for (size_t i = 0; i < valid_size; ++i) {
      if (headers_[i].size.is_na()) {
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

void Column<Vector<Text>>::unset(Int row_id) {
  Vector<Text> value = get(row_id);
  if (!value.is_na()) {
    // TODO: Update indexes if exist.
//    for (size_t i = 0; i < num_indexes(); ++i) {
//      indexes_[i]->remove(row_id, value);
//    }
    headers_[row_id.raw()] = na_header();
  }
}

void Column<Vector<Text>>::read(ArrayCRef<Record> records,
                                ArrayRef<Vector<Text>> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  for (size_t i = 0; i < records.size(); ++i) {
    values.set(i, get(records[i].row_id));
  }
}

size_t Column<Vector<Text>>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  if (table_size < headers_.size()) {
    return table_size;
  }
  return headers_.size();
}

Vector<Text> Column<Vector<Text>>::parse_datum(const Datum &datum) {
  switch (datum.type()) {
    case NA_DATA: {
      return Vector<Text>::na();
    }
    case TEXT_VECTOR_DATA: {
      return datum.as_text_vector();
    }
    default: {
      throw "Wrong data type";  // TODO
    }
  }
}

}  // namespace impl
}  // namespace grnxx
