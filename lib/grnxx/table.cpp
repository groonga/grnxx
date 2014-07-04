#include "grnxx/table.hpp"

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"

namespace grnxx {

Table::~Table() {}

Column *Table::create_column(Error *error,
                             String name,
                             DataType data_type,
                             const ColumnOptions &options) {
  if (find_column(nullptr, name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Column already exists: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return nullptr;
  }
  try {
    columns_.reserve(columns_.size() + 1);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  unique_ptr<Column> new_column =
      Column::create(error, this, name, data_type, options);
  if (!new_column) {
    return nullptr;
  }
  columns_.push_back(std::move(new_column));
  return columns_.back().get();
}

bool Table::remove_column(Error *error, String name) {
  size_t column_id;
  if (!find_column_with_id(error, name, &column_id)) {
    return false;
  }
  if (!columns_[column_id]->is_removable()) {
    GRNXX_ERROR_SET(error, NOT_REMOVABLE,
                    "Column is not removable: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return false;
  }
  columns_.erase(columns_.begin() + column_id);
  return true;
}

bool Table::rename_column(Error *error,
                          String name,
                          String new_name) {
  size_t column_id;
  if (!find_column_with_id(error, name, &column_id)) {
    return false;
  }
  if (name == new_name) {
    return true;
  }
  if (find_column(nullptr, new_name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Column already exists: new_name = \"%.*s\"",
                    static_cast<int>(new_name.size()), new_name.data());
    return false;
  }
  return columns_[column_id]->rename(error, new_name);
}

bool Table::reorder_column(Error *error,
                           String name,
                           String prev_name) {
  size_t column_id;
  if (!find_column_with_id(error, name, &column_id)) {
    return false;
  }
  size_t new_column_id = 0;
  if (prev_name.size() != 0) {
    size_t prev_column_id;
    if (!find_column_with_id(error, prev_name, &prev_column_id)) {
      return false;
    }
    if (column_id <= prev_column_id) {
      new_column_id = prev_column_id;
    } else {
      new_column_id = prev_column_id + 1;
    }
  }
  for ( ; column_id < new_column_id; ++column_id) {
    std::swap(columns_[column_id], columns_[column_id + 1]);
  }
  for ( ; column_id > new_column_id; --column_id) {
    std::swap(columns_[column_id], columns_[column_id - 1]);
  }
  return true;
}

Column *Table::get_column(Error *error, size_t column_id) const {
  if (column_id >= num_columns()) {
    GRNXX_ERROR_SET(error, NOT_FOUND,
                    "Column not found: column_id = %" PRIi64, column_id);
    return nullptr;
  }
  return columns_[column_id].get();
}

Column *Table::find_column(Error *error, String name) const {
  for (size_t column_id = 0; column_id < num_columns(); ++column_id) {
    if (name == columns_[column_id]->name()) {
      return columns_[column_id].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Column not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

bool Table::set_key_column(Error *error, String name) {
  if (key_column_) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key column already exists");
    return false;
  }
  // TODO: Key column is not supported yet.
  return false;
}

bool Table::unset_key_column(Error *error) {
  if (!key_column_) {
    GRNXX_ERROR_SET(error, NOT_FOUND, "Key column not found");
    return false;
  }
  // TODO: Key column is not supported yet.
  return false;
}

bool Table::insert_row(Error *error,
                       Int request_row_id,
                       const Datum &key,
                       Int *result_row_id) {
  *result_row_id = NULL_ROW_ID;
  Int next_row_id;
  if (request_row_id != NULL_ROW_ID) {
    if (request_row_id > (max_row_id() + 1)) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT,
                      "Invalid argument: request_row_id = %" PRIi64,
                      request_row_id);
      return false;
    }
    if (get_bit(request_row_id)) {
      *result_row_id = request_row_id;
      GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                      "Row already exists: request_row_id = %" PRIi64,
                      request_row_id);
      return false;
    }
    next_row_id = request_row_id;
  } else {
    // TODO: Removed rows should be reused.
    next_row_id = max_row_id() + 1;
  }
  if (key_column()) {
    *result_row_id = find_row(nullptr, key);
    if (*result_row_id != NULL_ROW_ID) {
      GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key already exists");
      return false;
    }
  }
  if (!reserve_bit(error, next_row_id)) {
    return false;
  }
  if (key_column()) {
    if (!key_column_->set_initial_key(error, next_row_id, key)) {
      return false;
    }
  }
  // TODO: Fill other column values with the default values.
  if (next_row_id > max_row_id()) {
    for (size_t column_id = 0; column_id < num_columns(); ++column_id) {
      if (columns_[column_id].get() != key_column()) {
        if (!columns_[column_id]->set_default_value(error, next_row_id)) {
          // Rollback the insertion.
          if (key_column()) {
            key_column_->unset(next_row_id);
          }
          for (size_t i = 0; i < column_id; ++i) {
            if (columns_[i].get() != key_column()) {
              columns_[i]->unset(next_row_id);
            }
          }
          return false;
        }
      }
    }
    max_row_id_ = next_row_id;
  }
  set_bit(next_row_id);
  *result_row_id = next_row_id;
  return true;
}

bool Table::remove_row(Error *error, Int row_id) {
  if (!test_row(error, row_id)) {
    return false;
  }
  // TODO: Check removability and unset column values.
  for (size_t column_id = 0; column_id < num_columns(); ++column_id) {
    columns_[column_id]->unset(row_id);
  }
  unset_bit(row_id);
  return true;
}

bool Table::test_row(Error *error, Int row_id) const {
  if ((row_id < MIN_ROW_ID) || (row_id > max_row_id())) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid row ID");
    return false;
  }
  if (!get_bit(row_id)) {
    GRNXX_ERROR_SET(error, NOT_FOUND, "Removed row");
    return false;
  }
  return true;
}

Int Table::find_row(Error *error, const Datum &key) const {
  if (!key_column_) {
    GRNXX_ERROR_SET(error, NO_KEY_COLUMN, "No key column");
    return NULL_ROW_ID;
  }
  // TODO: Key column is not supported yet.
  return NULL_ROW_ID;
}

unique_ptr<Cursor> Table::create_cursor(
    Error *error,
    const CursorOptions &options) const {
  // TODO: Cursor is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return nullptr;
}

unique_ptr<Table> Table::create(Error *error,
                                DB *db,
                                String name,
                                const TableOptions &options) {
  std::unique_ptr<Table> table(new Table);
  table->db_ = db;
  if (!table->name_.assign(error, name)) {
    return nullptr;
  }
  try {
    // Disable NULL_ROW_ID.
    table->bitmap_.push_back(~uint64_t(0) ^ 1);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  // TODO: Apply options.
  return table;
}

bool Table::reserve_bit(Error *error, Int row_id) {
  if (static_cast<size_t>(row_id / 64) >= bitmap_.size()) {
    try {
      bitmap_.push_back(~uint64_t(0));
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
  }
  return true;
}

bool Table::rename(Error *error, String new_name) {
  return name_.assign(error, new_name);
}

bool Table::is_removable() {
  // TODO: Referenced table (except self-reference) is not removable.
  return true;
}

Table::Table()
    : db_(nullptr),
      name_(),
      columns_(),
      key_column_(nullptr),
      max_row_id_(MIN_ROW_ID - 1),
      bitmap_() {}

Column *Table::find_column_with_id(Error *error,
                                   String name,
                                   size_t *column_id) const {
  for (Int i = 0; i < num_columns(); ++i) {
    if (name == columns_[i]->name()) {
      if (column_id != nullptr) {
        *column_id = i;
      }
      return columns_[i].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Column not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

}  // namespace grnxx
