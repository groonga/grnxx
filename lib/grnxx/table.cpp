#include "grnxx/table.hpp"

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/db.hpp"
#include "grnxx/error.hpp"
#include "grnxx/record.hpp"

#include <iostream>  // For debug.

namespace grnxx {

// -- TableCursor --

class TableCursor : public Cursor {
 public:
  // -- Public API --

  ~TableCursor() {}

  Int read(Error *error, Int max_count, RecordSet *record_set);

  // -- Internal API --

  // Create a cursor.
  //
  // Returns a pointer to the cursor on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<TableCursor> create(Error *error,
                                        const Table *table,
                                        const CursorOptions &options);

  // Read records in regular order.
  Int regular_read(Error *error, Int max_count, RecordSet *record_set);

  // Read records in reverse order.
  Int reverse_read(Error *error, Int max_count, RecordSet *record_set);

 private:
  Int offset_left_;
  Int limit_left_;
  OrderType order_type_;
  Int next_row_id_;

  explicit TableCursor(const Table *table);
};

Int TableCursor::read(Error *error, Int max_count, RecordSet *record_set) {
  if (max_count <= 0) {
    return 0;
  }
  switch (order_type_) {
    case REGULAR_ORDER: {
      return regular_read(error, max_count, record_set);
    }
    case REVERSE_ORDER: {
      return reverse_read(error, max_count, record_set);
    }
    default: {
      GRNXX_ERROR_SET(error, BROKEN, "Broken cursor");
      return -1;
    }
  }
}

unique_ptr<TableCursor> TableCursor::create(Error *error,
                                            const Table *table,
                                            const CursorOptions &options) {
  unique_ptr<TableCursor> cursor(new (nothrow) TableCursor(table));
  if (!cursor) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  cursor->offset_left_ = options.offset;
  cursor->limit_left_ = options.limit;
  switch (options.order_type) {
    case REGULAR_ORDER: {
      cursor->order_type_ = REGULAR_ORDER;
      cursor->next_row_id_ = MIN_ROW_ID;
      break;
    }
    case REVERSE_ORDER: {
      cursor->order_type_ = REVERSE_ORDER;
      cursor->next_row_id_ = table->max_row_id();
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid order type");
      return nullptr;
    }
  }
  return cursor;
}

TableCursor::TableCursor(const Table *table)
    : Cursor(table),
      offset_left_(),
      limit_left_(),
      order_type_(),
      next_row_id_() {}

Int TableCursor::regular_read(Error *error,
                              Int max_count,
                              RecordSet *record_set) {
  // TODO: If possible, the buffer should be expanded outside the loop in order
  //       to remove size check and buffer expansion inside the loop.
  //       However, note that max_count can be extremely large for reading all
  //       the remaining records.
  Int count = 0;
  bool has_false_bit =
      table_->num_rows() != (table_->max_row_id() - MIN_ROW_ID + 1);
  if (!has_false_bit) {
    // There are no false bits in the bitmap and bit checks are not required.
    Int num_remaining_records = table_->max_row_id() - next_row_id_ + 1;
    if (offset_left_ > 0) {
      if (offset_left_ >= num_remaining_records) {
        next_row_id_ += num_remaining_records;
        offset_left_ -= num_remaining_records;
        return 0;
      }
      num_remaining_records -= offset_left_;
      next_row_id_ += offset_left_;
      offset_left_ = 0;
    }
    // Calculate the number of records to be read.
    count = max_count;
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (Int i = 0; i < count; ++i) {
      if (!record_set->append(error, Record(next_row_id_, 0.0))) {
        return -1;
      }
      ++next_row_id_;
      --limit_left_;
    }
  } else {
    // There exist false bits in the bitmap and bit checks are required.
    while (next_row_id_ <= table_->max_row_id()) {
      if (!table_->get_bit(next_row_id_)) {
        ++next_row_id_;
        continue;
      }
      if (offset_left_ > 0) {
        --offset_left_;
        ++next_row_id_;
      } else {
        if (!record_set->append(error, Record(next_row_id_, 0.0))) {
          return -1;
        }
        --limit_left_;
        ++count;
        ++next_row_id_;
        if ((limit_left_ <= 0) || (count >= max_count)) {
          break;
        }
      }
    }
  }
  return count;
}

Int TableCursor::reverse_read(Error *error,
                              Int max_count,
                              RecordSet *record_set) {
  // TODO: If possible, the buffer should be expanded outside the loop in order
  //       to remove size check and buffer expansion inside the loop.
  //       However, note that max_count can be extremely large for reading all
  //       the remaining records.
  Int count = 0;
  bool has_false_bit =
      table_->num_rows() != (table_->max_row_id() - MIN_ROW_ID + 1);
  if (!has_false_bit) {
    // There are no false bits in the bitmap and bit checks are not required.
    Int num_remaining_records = next_row_id_ - MIN_ROW_ID + 1;
    if (offset_left_ > 0) {
      if (offset_left_ >= num_remaining_records) {
        next_row_id_ -= num_remaining_records;
        offset_left_ -= num_remaining_records;
        return 0;
      }
      num_remaining_records -= offset_left_;
      next_row_id_ -= offset_left_;
      offset_left_ = 0;
    }
    // Calculate the number of records to be read.
    count = max_count;
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (Int i = 0; i < count; ++i) {
      if (!record_set->append(error, Record(next_row_id_, 0.0))) {
        return -1;
      }
      --next_row_id_;
      --limit_left_;
    }
  } else {
    while (next_row_id_ >= MIN_ROW_ID) {
      // There exist false bits in the bitmap and bit checks are required.
      if (!table_->get_bit(next_row_id_)) {
        --next_row_id_;
        continue;
      }
      if (offset_left_ > 0) {
        --offset_left_;
        --next_row_id_;
      } else {
        if (!record_set->append(error, Record(next_row_id_, 0.0))) {
          return -1;
        }
        --limit_left_;
        ++count;
        --next_row_id_;
        if ((limit_left_ <= 0) || (count >= max_count)) {
          break;
        }
      }
    }
  }
  return count;
}

// -- Table --

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
  ++num_rows_;
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
  --num_rows_;
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
  return TableCursor::create(error, this, options);
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
      num_rows_(0),
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
