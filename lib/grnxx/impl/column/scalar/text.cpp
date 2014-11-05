#include "grnxx/impl/column/column_text.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/index.hpp"

#include <set>

namespace grnxx {
namespace impl {

bool Column<Text>::set(Error *error, Int row_id, const Datum &datum) {
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

bool Column<Text>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = get(row_id);
  return true;
}

unique_ptr<Column<Text>> Column<Text>::create(
    Error *error,
    Table *table,
    const StringCRef &name,
    const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
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

Column<Text>::~Column() {}

bool Column<Text>::set_key_attribute(Error *error) {
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

bool Column<Text>::unset_key_attribute(Error *error) {
  if (!has_key_attribute_) {
    GRNXX_ERROR_SET(error, INVALID_OPERATION,
                    "This column is not a key column");
    return false;
  }
  has_key_attribute_ = false;
  return true;
}

bool Column<Text>::set_initial_key(Error *error,
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

bool Column<Text>::set_default_value(Error *error, Int row_id) {
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

void Column<Text>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  headers_[row_id] = 0;
}

Int Column<Text>::find_one(const Datum &datum) const {
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

Column<Text>::Column() : ColumnBase(), headers_(), bodies_() {}

}  // namespace impl
}  // namespace grnxx
