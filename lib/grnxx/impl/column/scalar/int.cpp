#include "grnxx/impl/column/column_int.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/index.hpp"

#include <unordered_set>

namespace grnxx {
namespace impl {

bool Column<Int>::set(Error *error, Int row_id, const Datum &datum) {
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

bool Column<Int>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

unique_ptr<Column<Int>> Column<Int>::create(
    Error *error,
    Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
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

Column<Int>::~Column() {}

bool Column<Int>::set_key_attribute(Error *error) {
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

bool Column<Int>::unset_key_attribute(Error *error) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  has_key_attribute_ = false;
  return true;
}

bool Column<Int>::set_initial_key(Error *error,
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

bool Column<Int>::set_default_value(Error *error, Int row_id) {
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

void Column<Int>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  values_.set(row_id, TypeTraits<Int>::default_value());
}

Int Column<Int>::find_one(const Datum &datum) const {
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

void Column<Int>::clear_references(Int row_id) {
  // TODO: Cursor should not be used to avoid errors.
  if (indexes_.size() != 0) {
    auto cursor = indexes_[0]->find(nullptr, Int(0));
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
        set(nullptr, row_id, NULL_ROW_ID);
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

Column<Int>::Column() : ColumnBase(), values_() {}

}  // namespace impl
}  // namespace grnxx
