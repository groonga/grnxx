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
      value_size_(8),
      buffer_(nullptr),
      size_(0),
      capacity_(0) {
  if (!options.reference_table_name.is_empty()) {
    reference_table_ = table->_db()->find_table(options.reference_table_name);
    if (!reference_table_) {
      throw "Table not found";  // TODO
    }
  }
}

Column<Int>::~Column() {
  std::free(buffer_);
}

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
  reserve(value_id + 1, new_value);
  // Insert the new value into indexes.
  for (size_t i = 0; i < num_indexes(); ++i) try {
    indexes_[i]->insert(row_id, datum);
  } catch (...) {
    for (size_t j = 0; j < i; ++i) {
      indexes_[j]->remove(row_id, datum);
    }
    throw;
  }
  switch (value_size_) {
    case 8: {
      values_8_[value_id] = static_cast<int8_t>(new_value.raw());
      break;
    }
    case 16: {
      values_16_[value_id] = static_cast<int16_t>(new_value.raw());
      break;
    }
    case 32: {
      values_32_[value_id] = static_cast<int32_t>(new_value.raw());
      break;
    }
    default: {
      values_64_[value_id] = new_value;
      break;
    }
  }
}

void Column<Int>::get(Int row_id, Datum *datum) const {
  size_t value_id = row_id.raw();
  *datum = (value_id < size_) ? _get(value_id) : Int::na();
}

bool Column<Int>::contains(const Datum &datum) const {
  // TODO: Choose the best index.
  Int value = parse_datum(datum);
  if (!indexes_.is_empty()) {
    if (value.is_na()) {
      return table_->num_rows() != indexes_[0]->num_entries();
    }
    return indexes_[0]->contains(datum);
  }
  return !scan(value).is_na();
}

Int Column<Int>::find_one(const Datum &datum) const {
  // TODO: Choose the best index.
  Int value = parse_datum(datum);
  if (!value.is_na() && !indexes_.is_empty()) {
    return indexes_[0]->find_one(datum);
  }
  return scan(parse_datum(datum));
}

void Column<Int>::set_key_attribute() {
  if (is_key_) {
    throw "Key column";  // TODO
  }
  if (reference_table_ == table_) {
    throw "Self reference";  // TODO
  }

  if (!indexes_.is_empty()) {
    if (contains(grnxx::Int::na())) {
      throw "N/A exist";  // TODO
    }
    // TODO: Choose the best index.
    if (!indexes_[0]->test_uniqueness()) {
      throw "Key duplicate";  // TODO
    }
  } else {
    std::unordered_set<int64_t> set;
    size_t valid_size = get_valid_size();
    for (size_t i = 0; i < valid_size; ++i) try {
      // TODO: Improve this loop.
      Int value = _get(i);
      if (value.is_na()) {
        if (table_->test_row(grnxx::Int(i))) {
          throw "N/A exist";  // TODO
        }
      } else {
        if (!set.insert(value.raw()).second) {
          throw "Key duplicate";  // TODO
        }
      }
    } catch (const std::bad_alloc &) {
      throw "Memory allocation failed";  // TODO
    }
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
  Int value = parse_datum(key);
  reserve(value_id + 1, value);
  // Update indexes if exist.
  for (size_t i = 0; i < num_indexes(); ++i) try {
    indexes_[i]->insert(row_id, value);
  } catch (...) {
    for (size_t j = 0; j < i; ++j) {
      indexes_[j]->remove(row_id, value);
    }
    throw;
  }
  switch (value_size_) {
    case 8: {
      values_8_[value_id] = static_cast<int8_t>(value.raw());
      break;
    }
    case 16: {
      values_16_[value_id] = static_cast<int16_t>(value.raw());
      break;
    }
    case 32: {
      values_32_[value_id] = static_cast<int32_t>(value.raw());
      break;
    }
    default: {
      values_64_[value_id] = value;
      break;
    }
  }
}

void Column<Int>::unset(Int row_id) {
  Int value = get(row_id);
  if (!value.is_na()) {
    // Update indexes if exist.
    for (size_t i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(row_id, value);
    }
    switch (value_size_) {
      case 8: {
        values_8_[row_id.raw()] = na_value_8();
        break;
      }
      case 16: {
        values_16_[row_id.raw()] = na_value_16();
        break;
      }
      case 32: {
        values_32_[row_id.raw()] = na_value_32();
        break;
      }
      default: {
        values_64_[row_id.raw()] = Int::na();
        break;
      }
    }
  }
}

void Column<Int>::read(ArrayCRef<Record> records, ArrayRef<Int> values) const {
  if (records.size() != values.size()) {
    throw "Data size conflict";  // TODO
  }
  switch (value_size_) {
    case 8: {
      for (size_t i = 0; i < records.size(); ++i) {
        size_t value_id = records[i].row_id.raw();
        values[i] = ((value_id < size_) &&
                     (values_8_[value_id] != na_value_8())) ?
                    Int(values_8_[value_id]) : Int::na();
      }
      break;
    }
    case 16: {
      for (size_t i = 0; i < records.size(); ++i) {
        size_t value_id = records[i].row_id.raw();
        values[i] = ((value_id < size_) &&
                     (values_16_[value_id] != na_value_16())) ?
                    Int(values_16_[value_id]) : Int::na();
      }
      break;
    }
    case 32: {
      for (size_t i = 0; i < records.size(); ++i) {
        size_t value_id = records[i].row_id.raw();
        values[i] = ((value_id < size_) &&
                     (values_32_[value_id] != na_value_32())) ?
                    Int(values_32_[value_id]) : Int::na();
      }
      break;
    }
    default: {
      for (size_t i = 0; i < records.size(); ++i) {
        size_t value_id = records[i].row_id.raw();
        values[i] = (value_id < size_) ? values_64_[value_id] : Int::na();
      }
      break;
    }
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

Int Column<Int>::scan(Int value) const {
  if (table_->max_row_id().is_na()) {
    return Int::na();
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  size_t valid_size = (size_ < table_size) ? size_ : table_size;
  if (value.is_na()) {
    if (size_ < table_size) {
      return table_->max_row_id();
    }
    bool is_full = table_->is_full();
    switch (value_size_) {
      case 8: {
        for (size_t i = 0; i < valid_size; ++i) {
          if ((values_8_[i] == na_value_8()) &&
              (is_full || table_->_test_row(i))) {
            return Int(i);
          }
        }
        break;
      }
      case 16: {
        for (size_t i = 0; i < valid_size; ++i) {
          if ((values_16_[i] == na_value_16()) &&
              (is_full || table_->_test_row(i))) {
            return Int(i);
          }
        }
        break;
      }
      case 32: {
        for (size_t i = 0; i < valid_size; ++i) {
          if ((values_32_[i] == na_value_32()) &&
              (is_full || table_->_test_row(i))) {
            return Int(i);
          }
        }
        break;
      }
      default: {
        for (size_t i = 0; i < valid_size; ++i) {
          if (values_64_[i].is_na() && (is_full || table_->_test_row(i))) {
            return Int(i);
          }
        }
        break;
      }
    }
  } else {
    switch (value_size_) {
      case 8: {
        for (size_t i = 0; i < valid_size; ++i) {
          if (values_8_[i] == value.raw()) {
            return Int(i);
          }
        }
        break;
      }
      case 16: {
        for (size_t i = 0; i < valid_size; ++i) {
          if (values_16_[i] == value.raw()) {
            return Int(i);
          }
        }
        break;
      }
      case 32: {
        for (size_t i = 0; i < valid_size; ++i) {
          if (values_32_[i] == value.raw()) {
            return Int(i);
          }
        }
        break;
      }
      default: {
        for (size_t i = 0; i < valid_size; ++i) {
          if (values_64_[i].match(value)) {
            return Int(i);
          }
        }
        break;
      }
    }
  }
  return Int::na();
}

size_t Column<Int>::get_valid_size() const {
  if (table_->max_row_id().is_na()) {
    return 0;
  }
  size_t table_size = table_->max_row_id().raw() + 1;
  return (table_size < size_) ? table_size : size_;
}

void Column<Int>::reserve(size_t size, Int value) {
  if (!value.is_na()) {
    int64_t raw = value.raw();
    switch (value_size_) {
      case 8: {
        if ((raw >= min_value_8()) && (raw <= max_value_8())) {
          // 8 -> 8.
          reserve_with_same_value_size(size);
        } else if ((raw >= min_value_16()) && (raw <= max_value_16())) {
          // 8 -> 16.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          int16_t *new_values = static_cast<int16_t *>(
              std::malloc(sizeof(int16_t) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_8_[i] != na_value_8()) ?
                            values_8_[i] : na_value_16();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = na_value_16();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 16;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        } else if ((raw >= min_value_32()) && (raw <= max_value_32())) {
          // 8 -> 32.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          int32_t *new_values = static_cast<int32_t *>(
              std::malloc(sizeof(int32_t) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_8_[i] != na_value_8()) ?
                            values_8_[i] : na_value_32();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = na_value_32();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 32;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        } else {
          // 8 -> 64.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          Int *new_values = static_cast<Int *>(
              std::malloc(sizeof(Int) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_8_[i] != na_value_8()) ?
                            Int(values_8_[i]) : Int::na();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = Int::na();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 64;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        }
        break;
      }
      case 16: {
        if ((raw >= min_value_16()) && (raw <= max_value_16())) {
          // 16 -> 16.
          reserve_with_same_value_size(size);
        } else if ((raw >= min_value_32()) && (raw <= max_value_32())) {
          // 16 -> 32.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          int32_t *new_values = static_cast<int32_t *>(
              std::malloc(sizeof(int32_t) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_16_[i] != na_value_16()) ?
                            values_16_[i] : na_value_32();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = na_value_32();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 32;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        } else {
          // 16 -> 64.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          Int *new_values = static_cast<Int *>(
              std::malloc(sizeof(Int) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_16_[i] != na_value_16()) ?
                            Int(values_16_[i]) : Int::na();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = Int::na();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 64;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        }
        break;
      }
      case 32: {
        if ((raw >= min_value_32()) && (raw <= max_value_32())) {
          // 32 -> 32.
          reserve_with_same_value_size(size);
        } else {
          // 32 -> 64.
          size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
          while (new_capacity < size) {
            new_capacity *= 2;
          }
          Int *new_values = static_cast<Int *>(
              std::malloc(sizeof(Int) * new_capacity));
          if (!new_values) {
            throw "Memory allocation failed";
          }
          for (size_t i = 0; i < size_; ++i) {
            new_values[i] = (values_32_[i] != na_value_32()) ?
                            Int(values_32_[i]) : Int::na();
          }
          for (size_t i = size_; i < size; ++i) {
            new_values[i] = Int::na();
          }
          std::free(buffer_);
          buffer_ = new_values;
          value_size_ = 64;
          if (size > size_) {
            size_ = size;
          }
          capacity_ = new_capacity;
        }
        break;
      }
      default: {
        // 64 -> 64.
        reserve_with_same_value_size(size);
        break;
      }
    }
  }
}

void Column<Int>::reserve_with_same_value_size(size_t size) {
  if (size > capacity_) {
    size_t new_capacity = (capacity_ != 0) ? capacity_ : 1;
    while (new_capacity < size) {
      new_capacity *= 2;
    }
    void *new_buffer =
        std::realloc(buffer_, (value_size_ / 8) * new_capacity);
    if (!new_buffer) {
      throw "Memory allocation failed";
    }
    buffer_ = new_buffer;
    capacity_ = new_capacity;
  }
  if (size > size_) {
    switch (value_size_) {
      case 8: {
        for (size_t i = size_; i < size; ++i) {
          values_8_[i] = na_value_8();
        }
        break;
      }
      case 16: {
        for (size_t i = size_; i < size; ++i) {
          values_16_[i] = na_value_16();
        }
        break;
      }
      case 32: {
        for (size_t i = size_; i < size; ++i) {
          values_32_[i] = na_value_32();
        }
        break;
      }
      default: {
        for (size_t i = size_; i < size; ++i) {
          values_64_[i] = Int::na();
        }
        break;
      }
    }
    size_ = size;
  }
}

void Column<Int>::reserve_with_different_value_size(size_t size,
                                                    size_t value_size) {
  // TODO
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
