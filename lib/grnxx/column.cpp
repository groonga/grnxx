#include "grnxx/column.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/index.hpp"

#include <set>
#include <unordered_set>

namespace grnxx {

// -- ColumnImpl<T> --

template <typename T>
bool ColumnImpl<T>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TypeTraits<T>::data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  // Note that a Bool object does not have its own address.
  T old_value = get(row_id);
  T new_value;
  datum.force(&new_value);
  // TODO: Note that NaN != NaN.
  if (new_value != old_value) {
    for (Int i = 0; i < num_indexes(); ++i) {
      if (!indexes_[i]->insert(error, row_id, datum)) {
        for (Int j = 0; j < i; ++i) {
          indexes_[j]->remove(nullptr, row_id, datum);
        }
        return false;
      }
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(nullptr, row_id, old_value);
    }
  }
  values_.set(row_id, new_value);
  return true;
}

template <typename T>
bool ColumnImpl<T>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

template <typename T>
unique_ptr<ColumnImpl<T>> ColumnImpl<T>::create(Error *error,
                                                impl::Table *table,
                                                const StringCRef &name,
                                                const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               TypeTraits<T>::data_type(), options)) {
    return nullptr;
  }
  if (!column->values_.resize(error, table->max_row_id() + 1,
                              TypeTraits<T>::default_value())) {
    return nullptr;
  }
  return column;
}

template <typename T>
ColumnImpl<T>::~ColumnImpl() {}

template <typename T>
bool ColumnImpl<T>::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1, TypeTraits<T>::default_value())) {
      return false;
    }
  }
  T value = TypeTraits<T>::default_value();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  values_.set(row_id, value);
  return true;
}

template <typename T>
void ColumnImpl<T>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  values_.set(row_id, TypeTraits<T>::default_value());
}

template <typename T>
ColumnImpl<T>::ColumnImpl() : ColumnBase(), values_() {}

template class ColumnImpl<Bool>;
template class ColumnImpl<Float>;
template class ColumnImpl<GeoPoint>;
template class ColumnImpl<Vector<Bool>>;

// -- ColumnImpl<Int> --

bool ColumnImpl<Int>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != INT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  if (ref_table_) {
    if (!ref_table_->test_row(error, datum.force_int())) {
      return false;
    }
  }
  Int old_value = get(row_id);
  Int new_value = datum.force_int();
  if (new_value != old_value) {
    if (has_key_attribute_ && contains(datum)) {
      GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
      return false;
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      if (!indexes_[i]->insert(error, row_id, datum)) {
        for (Int j = 0; j < i; ++i) {
          indexes_[j]->remove(nullptr, row_id, datum);
        }
        return false;
      }
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(nullptr, row_id, old_value);
    }
  }
  values_.set(row_id, new_value);
  return true;
}

bool ColumnImpl<Int>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

unique_ptr<ColumnImpl<Int>> ColumnImpl<Int>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, INT_DATA, options)) {
    return nullptr;
  }
  if (!column->values_.resize(error, table->max_row_id() + 1,
                              TypeTraits<Int>::default_value())) {
    return nullptr;
  }
  if (column->ref_table()) {
    if (!column->ref_table_->append_referrer_column(error, column.get())) {
      return nullptr;
    }
  }
  return column;
}

ColumnImpl<Int>::~ColumnImpl() {}

bool ColumnImpl<Int>::set_key_attribute(Error *error) {
  if (has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is a key column");
    return false;
  }
  // TODO: An index should be used if possible.
  try {
    std::unordered_set<Int> set;
    // TODO: Functor-based inline callback may be better in this case,
    //       because it does not require memory allocation.
    auto cursor = table_->create_cursor(nullptr);
    if (!cursor) {
      return false;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok) {
        return false;
      } else {
        break;
      }
      for (Int i = 0; i < result.count; ++i) {
        if (!set.insert(values_[records.get_row_id(i)]).second) {
          GRNXX_ERROR_SET(error, INVALID_OPERATION, "Key duplicate");
          return false;
        }
      }
      records.clear();
    }
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  has_key_attribute_ = true;
  return true;
}

bool ColumnImpl<Int>::unset_key_attribute(Error *error) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  has_key_attribute_ = false;
  return true;
}

bool ColumnImpl<Int>::set_initial_key(Error *error,
                                      Int row_id,
                                      const Datum &key) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  if (has_key_attribute_ && contains(key)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
    return false;
  }
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1, TypeTraits<Int>::default_value())) {
      return false;
    }
  }
  Int value = key.force_int();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  values_.set(row_id, value);
  return true;
}

bool ColumnImpl<Int>::set_default_value(Error *error, Int row_id) {
  if (has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is a key column");
    return false;
  }
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1, TypeTraits<Int>::default_value())) {
      return false;
    }
  }
  Int value = TypeTraits<Int>::default_value();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  values_.set(row_id, value);
  return true;
}

void ColumnImpl<Int>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  values_.set(row_id, TypeTraits<Int>::default_value());
}

Int ColumnImpl<Int>::find_one(const Datum &datum) const {
  // TODO: Cursor should not be used because it takes time.
  //       Also, cursor operations can fail due to memory allocation.
  Int value = datum.force_int();
  if (indexes_.size() != 0) {
    return indexes_[0]->find_one(datum);
  } else {
    // TODO: A full scan takes time.
    //       An index should be required for a key column.

    // TODO: Functor-based inline callback may be better in this case,
    //       because it does not require memory allocation.

    // Scan the column to find "value".
    auto cursor = table_->create_cursor(nullptr);
    if (!cursor) {
      return NULL_ROW_ID;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok || result.count == 0) {
        return NULL_ROW_ID;
      }
      for (Int i = 0; i < result.count; ++i) {
        if (values_[records.get_row_id(i)] == value) {
          return records.get_row_id(i);
        }
      }
      records.clear();
    }
  }
  return NULL_ROW_ID;
}

void ColumnImpl<Int>::clear_references(Int row_id) {
  // TODO: Cursor should not be used to avoid errors.
  if (indexes_.size() != 0) {
    auto cursor = indexes_[0]->find(nullptr, grnxx::Int(0));
    if (!cursor) {
      // Error.
      return;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok) {
        // Error.
        return;
      } else if (result.count == 0) {
        return;
      }
      for (Int i = 0; i < records.size(); ++i) {
        set(nullptr, row_id, grnxx::NULL_ROW_ID);
      }
      records.clear();
    }
  } else {
    auto cursor = table_->create_cursor(nullptr);
    if (!cursor) {
      // Error.
      return;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok) {
        // Error.
        return;
      } else if (result.count == 0) {
        return;
      }
      for (Int i = 0; i < records.size(); ++i) {
        if (values_[records.get_row_id(i)] == row_id) {
          values_[records.get_row_id(i)] = NULL_ROW_ID;
        }
      }
      records.clear();
    }
  }
}

ColumnImpl<Int>::ColumnImpl() : ColumnBase(), values_() {}

// -- ColumnImpl<Text> --

bool ColumnImpl<Text>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TEXT_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Text old_value = get(row_id);
  Text new_value = datum.force_text();
  if (new_value != old_value) {
    if (has_key_attribute_ && contains(datum)) {
      GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
      return false;
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      if (!indexes_[i]->insert(error, row_id, datum)) {
        for (Int j = 0; j < i; ++i) {
          indexes_[j]->remove(nullptr, row_id, datum);
        }
        return false;
      }
    }
    Int offset = bodies_.size();
    UInt new_header;
    if (new_value.size() < 0xFFFF) {
      if (!bodies_.resize(error, offset + new_value.size())) {
        return false;
      }
      std::memcpy(&bodies_[offset], new_value.data(), new_value.size());
      new_header = (offset << 16) | new_value.size();
    } else {
      // The size of a long text is stored in front of the body.
      if ((offset % sizeof(Int)) != 0) {
        offset += sizeof(Int) - (offset % sizeof(Int));
      }
      if (!bodies_.resize(error, offset + sizeof(Int) + new_value.size())) {
        return false;
      }
      *reinterpret_cast<Int *>(&bodies_[offset]) = new_value.size();
      std::memcpy(&bodies_[offset + sizeof(Int)],
                  new_value.data(), new_value.size());
      new_header = (offset << 16) | 0xFFFF;
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(nullptr, row_id, old_value);
    }
    headers_[row_id] = new_header;
  }
  return true;
}

bool ColumnImpl<Text>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Text>> ColumnImpl<Text>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, TEXT_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Text>::~ColumnImpl() {}

bool ColumnImpl<Text>::set_key_attribute(Error *error) {
  if (has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is a key column");
    return false;
  }
  // TODO: An index should be used if possible.
  try {
    std::set<Text> set;
    // TODO: Functor-based inline callback may be better in this case,
    //       because it does not require memory allocation.
    auto cursor = table_->create_cursor(nullptr);
    if (!cursor) {
      return false;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok) {
        return false;
      } else {
        break;
      }
      for (Int i = 0; i < result.count; ++i) {
        if (!set.insert(get(records.get_row_id(i))).second) {
          GRNXX_ERROR_SET(error, INVALID_OPERATION, "Key duplicate");
          return false;
        }
      }
      records.clear();
    }
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  has_key_attribute_ = true;
  return true;
}

bool ColumnImpl<Text>::unset_key_attribute(Error *error) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  has_key_attribute_ = false;
  return true;
}

bool ColumnImpl<Text>::set_initial_key(Error *error,
    Int row_id,
    const Datum &key) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  if (has_key_attribute_ && contains(key)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key duplicate");
    return false;
  }
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1, 0)) {
      return false;
    }
  }
  Text value = key.force_text();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  Int offset = bodies_.size();
  UInt header;
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    std::memcpy(&bodies_[offset], value.data(), value.size());
    header = (offset << 16) | value.size();
  } else {
    // The size of a long text is stored in front of the body.
    if ((offset % sizeof(Int)) != 0) {
      offset += sizeof(Int) - (offset % sizeof(Int));
    }
    if (!bodies_.resize(error, offset + sizeof(Int) + value.size())) {
      return false;
    }
    *reinterpret_cast<Int *>(&bodies_[offset]) = value.size();
    std::memcpy(&bodies_[offset + sizeof(Int)], value.data(), value.size());
    header = (offset << 16) | 0xFFFF;
  }
  headers_[row_id] = header;
  return true;
}

bool ColumnImpl<Text>::set_default_value(Error *error, Int row_id) {
  if (has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is a key column");
    return false;
  }
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  Text value = TypeTraits<Text>::default_value();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Text>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  headers_[row_id] = 0;
}

Int ColumnImpl<Text>::find_one(const Datum &datum) const {
  // TODO: Cursor should not be used because it takes time.
  // Also, cursor operations can fail due to memory allocation.
  Text value = datum.force_text();
  if (indexes_.size() != 0) {
    auto cursor = indexes_[0]->find(nullptr, value);
    Array<Record> records;
    auto result = cursor->read(nullptr, 1, &records);
    if (!result.is_ok || (result.count == 0)) {
      return NULL_ROW_ID;
    }
    return true;
  } else {
    // TODO: A full scan takes time.
    // An index should be required for a key column.

    // TODO: Functor-based inline callback may be better in this case,
    // because it does not require memory allocation.

    // Scan the column to find "value".
    auto cursor = table_->create_cursor(nullptr);
    if (!cursor) {
      return NULL_ROW_ID;
    }
    Array<Record> records;
    for ( ; ; ) {
      auto result = cursor->read(nullptr, 1024, &records);
      if (!result.is_ok || result.count == 0) {
        return NULL_ROW_ID;
      }
      for (Int i = 0; i < result.count; ++i) {
        if (get(records.get_row_id(i)) == value) {
          return records.get_row_id(i);
        }
      }
      records.clear();
    }
  }
  return NULL_ROW_ID;
}

ColumnImpl<Text>::ColumnImpl() : ColumnBase(), headers_(), bodies_() {}

// -- ColumnImpl<Vector<Int>> --

bool ColumnImpl<Vector<Int>>::set(Error *error, Int row_id,
                                  const Datum &datum) {
  if (datum.type() != INT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Int> value = datum.force_int_vector();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  if (ref_table_) {
    for (Int i = 0; i < value.size(); ++i) {
      if (!ref_table_->test_row(error, value[i])) {
        return false;
      }
    }
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long vector is stored in front of the body.
    if (!bodies_.resize(error, offset + 1 + value.size())) {
      return false;
    }
    bodies_[offset] = value.size();
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + 1 + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Vector<Int>>::get(Error *error, Int row_id,
                                  Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<Int>>> ColumnImpl<Vector<Int>>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name, INT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  if (column->ref_table()) {
    if (!column->ref_table_->append_referrer_column(error, column.get())) {
      return nullptr;
    }
  }
  return column;
}

ColumnImpl<Vector<Int>>::~ColumnImpl() {}

bool ColumnImpl<Vector<Int>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Vector<Int>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

void ColumnImpl<Vector<Int>>::clear_references(Int row_id) {
  auto cursor = table_->create_cursor(nullptr);
  if (!cursor) {
    // Error.
    return;
  }
  Array<Record> records;
  for ( ; ; ) {
    auto result = cursor->read(nullptr, 1024, &records);
    if (!result.is_ok) {
      // Error.
      return;
    } else if (result.count == 0) {
      return;
    }
    for (Int i = 0; i < records.size(); ++i) {
      Int value_row_id = records.get_row_id(i);
      Int value_size = static_cast<Int>(headers_[value_row_id] & 0xFFFF);
      if (value_size == 0) {
        continue;
      }
      Int value_offset = static_cast<Int>(headers_[value_row_id] >> 16);
      if (value_size >= 0xFFFF) {
        value_size = bodies_[value_offset];
        ++value_offset;
      }
      Int count = 0;
      for (Int j = 0; j < value_size; ++j) {
        if (bodies_[value_offset + j] != row_id) {
          bodies_[value_offset + count] = bodies_[value_offset + j];
          ++count;
        }
      }
      if (count < value_size) {
        if (count == 0) {
          headers_[value_row_id] = 0;
        } else if (count < 0xFFFF) {
          headers_[value_row_id] = count | (value_offset << 16);
        } else {
          bodies_[value_offset - 1] = count;
        }
      }
    }
    records.clear();
  }
}

ColumnImpl<Vector<Int>>::ColumnImpl() : ColumnBase(), headers_(), bodies_() {}

// -- ColumnImpl<Vector<Float>> --

bool ColumnImpl<Vector<Float>>::set(Error *error, Int row_id,
                                    const Datum &datum) {
  if (datum.type() != FLOAT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Float> value = datum.force_float_vector();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long vector is stored in front of the body.
    if (!bodies_.resize(error, offset + 1 + value.size())) {
      return false;
    }
    Int size_for_copy = value.size();
    std::memcpy(&bodies_[offset], &size_for_copy, sizeof(Int));
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + 1 + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Vector<Float>>::get(Error *error, Int row_id,
                                    Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<Float>>> ColumnImpl<Vector<Float>>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               FLOAT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Vector<Float>>::~ColumnImpl() {}

bool ColumnImpl<Vector<Float>>::set_default_value(Error *error, Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Vector<Float>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

ColumnImpl<Vector<Float>>::ColumnImpl()
    : ColumnBase(),
      headers_(),
      bodies_() {}

// -- ColumnImpl<Vector<GeoPoint>> --

bool ColumnImpl<Vector<GeoPoint>>::set(Error *error, Int row_id,
                                       const Datum &datum) {
  if (datum.type() != GEO_POINT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<GeoPoint> value = datum.force_geo_point_vector();
  if (value.size() == 0) {
    headers_[row_id] = 0;
    return true;
  }
  Int offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    if (!bodies_.resize(error, offset + value.size())) {
      return false;
    }
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // The size of a long vector is stored in front of the body.
    if (!bodies_.resize(error, offset + 1 + value.size())) {
      return false;
    }
    Int size_for_copy = value.size();
    std::memcpy(&bodies_[offset], &size_for_copy, sizeof(Int));
    for (Int i = 0; i < value.size(); ++i) {
      bodies_[offset + 1 + i] = value[i];
    }
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  return true;
}

bool ColumnImpl<Vector<GeoPoint>>::get(Error *error, Int row_id,
                                       Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<GeoPoint>>> ColumnImpl<Vector<GeoPoint>>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               GEO_POINT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1, 0)) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Vector<GeoPoint>>::~ColumnImpl() {}

bool ColumnImpl<Vector<GeoPoint>>::set_default_value(Error *error,
                                                     Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = 0;
  return true;
}

void ColumnImpl<Vector<GeoPoint>>::unset(Int row_id) {
  headers_[row_id] = 0;
}

ColumnImpl<Vector<GeoPoint>>::ColumnImpl()
    : ColumnBase(),
      headers_(),
      bodies_() {}

// -- ColumnImpl<Vector<Text>> --

bool ColumnImpl<Vector<Text>>::set(Error *error, Int row_id,
                                   const Datum &datum) {
  if (datum.type() != TEXT_VECTOR_DATA) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Vector<Text> value = datum.force_text_vector();
  if (value.size() == 0) {
    headers_[row_id] = Header{ 0, 0 };
    return true;
  }
  Int text_headers_offset = text_headers_.size();
  if (!text_headers_.resize(error, text_headers_offset + value.size())) {
    return false;
  }
  Int total_size = 0;
  for (Int i = 0; i < value.size(); ++i) {
    total_size += value[i].size();
  }
  Int bodies_offset = bodies_.size();
  if (!bodies_.resize(error, bodies_offset + total_size)) {
    return false;
  }
  headers_[row_id] = Header{ text_headers_offset, value.size() };
  for (Int i = 0; i < value.size(); ++i) {
    text_headers_[text_headers_offset + i].offset = bodies_offset;
    text_headers_[text_headers_offset + i].size = value[i].size();
    std::memcpy(&bodies_[bodies_offset], value[i].data(), value[i].size());
    bodies_offset += value[i].size();
  }
  return true;
}

bool ColumnImpl<Vector<Text>>::get(Error *error, Int row_id,
                                   Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<ColumnImpl<Vector<Text>>> ColumnImpl<Vector<Text>>::create(
    Error *error,
    impl::Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<ColumnImpl> column(new (nothrow) ColumnImpl);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               TEXT_VECTOR_DATA, options)) {
    return nullptr;
  }
  if (!column->headers_.resize(error, table->max_row_id() + 1,
                               Header{ 0, 0 })) {
    return nullptr;
  }
  return column;
}

ColumnImpl<Vector<Text>>::~ColumnImpl() {}

bool ColumnImpl<Vector<Text>>::set_default_value(Error *error,
                                                     Int row_id) {
  if (row_id >= headers_.size()) {
    if (!headers_.resize(error, row_id + 1)) {
      return false;
    }
  }
  headers_[row_id] = Header{ 0, 0 };
  return true;
}

void ColumnImpl<Vector<Text>>::unset(Int row_id) {
  headers_[row_id] = Header{ 0, 0 };
}

ColumnImpl<Vector<Text>>::ColumnImpl()
    : ColumnBase(),
      headers_(),
      text_headers_(),
      bodies_() {}

}  // namespace grnxx
