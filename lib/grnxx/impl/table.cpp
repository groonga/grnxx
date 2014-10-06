#include "grnxx/impl/table.hpp"

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"

#include <iostream>  // For debug.

namespace grnxx {
namespace impl {

// -- TableCursor --

class TableCursor : public Cursor {
 public:
  // -- Public API --

  ~TableCursor() {}

  CursorResult read(Error *error, ArrayRef<Record> records);

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
  CursorResult regular_read(Error *error, ArrayRef<Record> records);

  // Read records in reverse order.
  CursorResult reverse_read(Error *error, ArrayRef<Record> records);

 private:
  const Table *table_;
  Int offset_left_;
  Int limit_left_;
  OrderType order_type_;
  Int next_row_id_;

  explicit TableCursor(const Table *table);
};

CursorResult TableCursor::read(Error *error, ArrayRef<Record> records) {
  if (records.size() <= 0) {
    return { true, 0 };
  }
  switch (order_type_) {
    case REGULAR_ORDER: {
      return regular_read(error, records);
    }
    case REVERSE_ORDER: {
      return reverse_read(error, records);
    }
    default: {
      GRNXX_ERROR_SET(error, BROKEN, "Broken cursor");
      return { false, 0 };
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
      table_(table),
      offset_left_(),
      limit_left_(),
      order_type_(),
      next_row_id_() {}

CursorResult TableCursor::regular_read(Error *, ArrayRef<Record> records) {
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
        return { true, 0 };
      }
      num_remaining_records -= offset_left_;
      next_row_id_ += offset_left_;
      offset_left_ = 0;
    }
    // Calculate the number of records to be read.
    count = records.size();
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (Int i = 0; i < count; ++i) {
      records.set(i, Record(next_row_id_, 0.0));
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
        records.set(count, Record(next_row_id_, 0.0));
        --limit_left_;
        ++count;
        ++next_row_id_;
        if ((limit_left_ <= 0) || (count >= records.size())) {
          break;
        }
      }
    }
  }
  return { true, count };
}

CursorResult TableCursor::reverse_read(Error *, ArrayRef<Record> records) {
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
        return { true, 0 };
      }
      num_remaining_records -= offset_left_;
      next_row_id_ -= offset_left_;
      offset_left_ = 0;
    }
    // Calculate the number of records to be read.
    count = records.size();
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (Int i = 0; i < count; ++i) {
      records.set(i, Record(next_row_id_, 0.0));
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
        records.set(count, Record(next_row_id_, 0.0));
        --limit_left_;
        ++count;
        --next_row_id_;
        if ((limit_left_ <= 0) || (count >= records.size())) {
          break;
        }
      }
    }
  }
  return { true, count };
}

// -- Table --

Table::Table()
    : grnxx::Table(),
      db_(nullptr),
      name_(),
      columns_(),
      referrer_columns_(),
      key_column_(nullptr),
      num_rows_(0),
      max_row_id_(MIN_ROW_ID - 1),
      bitmap_(),
      bitmap_indexes_() {}

Table::~Table() {}

grnxx::DB *Table::db() const {
  return db_;
}

Column *Table::create_column(Error *error,
                             const StringCRef &name,
                             DataType data_type,
                             const ColumnOptions &options) {
  if (find_column(nullptr, name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Column already exists: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return nullptr;
  }
  if (!columns_.reserve(error, columns_.size() + 1)) {
    return nullptr;
  }
  unique_ptr<Column> new_column =
      Column::create(error, this, name, data_type, options);
  if (!new_column) {
    return nullptr;
  }
  columns_.push_back(nullptr, std::move(new_column));
  return columns_.back().get();
}

bool Table::remove_column(Error *error, const StringCRef &name) {
  Int column_id;
  if (!find_column_with_id(error, name, &column_id)) {
    return false;
  }
  if (!columns_[column_id]->is_removable()) {
    GRNXX_ERROR_SET(error, NOT_REMOVABLE,
                    "Column is not removable: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return false;
  }
  Column *column = columns_[column_id].get();
  if (column == key_column_) {
    key_column_ = nullptr;
  }
  if (column->ref_table()) {
    column->_ref_table()->remove_referrer_column(nullptr, column);
  }
  columns_.erase(column_id);
  return true;
}

bool Table::rename_column(Error *error,
                          const StringCRef &name,
                          const StringCRef &new_name) {
  Int column_id;
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
                           const StringCRef &name,
                           const StringCRef &prev_name) {
  Int column_id;
  if (!find_column_with_id(error, name, &column_id)) {
    return false;
  }
  Int new_column_id = 0;
  if (prev_name.size() != 0) {
    Int prev_column_id;
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

Column *Table::find_column(Error *error, const StringCRef &name) const {
  for (Int column_id = 0; column_id < num_columns(); ++column_id) {
    if (name == columns_[column_id]->name()) {
      return columns_[column_id].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Column not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

bool Table::set_key_column(Error *error, const StringCRef &name) {
  if (key_column_) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS, "Key column already exists");
    return false;
  }
  Column *column = find_column(error, name);
  if (!column) {
    return false;
  }
  if (!column->set_key_attribute(error)) {
    return false;
  }
  key_column_ = column;
  return true;
}

bool Table::unset_key_column(Error *error) {
  if (!key_column_) {
    GRNXX_ERROR_SET(error, NOT_FOUND, "Key column not found");
    return false;
  }
  if (!key_column_->unset_key_attribute(error)) {
    return false;
  }
  key_column_ = nullptr;
  return true;
}

bool Table::insert_row(Error *error,
                       Int request_row_id,
                       const Datum &key,
                       Int *result_row_id) {
  *result_row_id = NULL_ROW_ID;
  Int next_row_id;
  if (request_row_id == NULL_ROW_ID) {
    next_row_id = find_zero_bit();
  } else if ((request_row_id < MIN_ROW_ID) || (request_row_id > MAX_ROW_ID)) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT,
                    "Invalid argument: request_row_id = %" PRIi64,
                    request_row_id);
    return false;
  } else if (request_row_id > max_row_id()) {
    next_row_id = request_row_id;
  } else {
    // Fails if the request row ID is used.
    if (get_bit(request_row_id)) {
      *result_row_id = request_row_id;
      GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                      "Row already exists: request_row_id = %" PRIi64,
                      request_row_id);
      return false;
    }
    next_row_id = request_row_id;
  }

  if (key_column()) {
    // Fails if the key already exists.
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

  if (next_row_id > max_row_id()) {
    // Fill non-key column values with the default values.
    for (Int column_id = 0; column_id < num_columns(); ++column_id) {
      if (columns_[column_id].get() != key_column()) {
        if (!columns_[column_id]->set_default_value(error, next_row_id)) {
          // Rollback the insertion.
          if (key_column()) {
            key_column_->unset(next_row_id);
          }
          for (Int i = 0; i < column_id; ++i) {
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
  // TODO: Check removability.
  for (Int i = 0; i < referrer_columns_.size(); ++i) {
    if (referrer_columns_[i]->has_key_attribute()) {
      GRNXX_ERROR_SET(error, INVALID_OPERATION,
                      "This table is referred to from a key column");
      return false;
    }
  }
  // Unset column values.
  for (Int column_id = 0; column_id < num_columns(); ++column_id) {
    columns_[column_id]->unset(row_id);
  }
  unset_bit(row_id);
  --num_rows_;
  if (num_rows_ == 0) {
    max_row_id_ = MIN_ROW_ID - 1;
  } else if (row_id == max_row_id()) {
    Int block_id = (max_row_id() - 1) / 64;
    while (block_id >= 0) {
      if (bitmap_[block_id] != 0) {
        break;
      }
      --block_id;
    }
    max_row_id_ = (block_id * 64) + 63 - ::__builtin_clzll(bitmap_[block_id]);
  }
  // Clear referrers.
  for (Int i = 0; i < referrer_columns_.size(); ++i) {
    referrer_columns_[i]->clear_references(row_id);
  }
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
  return key_column_->find_one(key);
}

unique_ptr<Cursor> Table::create_cursor(
    Error *error,
    const CursorOptions &options) const {
  return TableCursor::create(error, this, options);
}

unique_ptr<Table> Table::create(Error *error,
                                DB *db,
                                const StringCRef &name,
                                const TableOptions &) {
  unique_ptr<Table> table(new Table);
  table->db_ = db;
  if (!table->name_.assign(error, name)) {
    return nullptr;
  }
  // Initialize the bitmap.
  if (!table->reserve_bit(error, 0)) {
    return nullptr;
  }
  table->set_bit(0);
  // TODO: Apply options.
  return table;
}

bool Table::rename(Error *error, const StringCRef &new_name) {
  return name_.assign(error, new_name);
}

bool Table::is_removable() {
  // Referenced table (except self-reference) is not removable.
  for (Int i = 0; i < referrer_columns_.size(); ++i) {
    if (referrer_columns_[i]->table() != this) {
      return false;
    }
  }
  return true;
}

bool Table::append_referrer_column(Error *error, Column *column) {
  if (column->ref_table() != this) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong column");
    return false;
  }
  return referrer_columns_.push_back(error, column);
}

bool Table::remove_referrer_column(Error *error, Column *column) {
  for (Int i = 0; i < referrer_columns_.size(); ++i) {
    if (column == referrer_columns_[i]) {
      referrer_columns_.erase(i);
      return true;
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Column not found");
  return false;
}

void Table::set_bit(Int i) {
  bitmap_[i / 64] |= UInt(1) << (i % 64);
  if (bitmap_[i / 64] == ~UInt(0)) {
    for (Int index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
      i /= 64;
      bitmap_indexes_[index_id][i / 64] |= UInt(1) << (i % 64);
      if (bitmap_indexes_[index_id][i / 64] != ~UInt(0)) {
        break;
      }
    }
  }
}

void Table::unset_bit(Int i) {
  bool is_full = bitmap_[i / 64] == ~UInt(0);
  bitmap_[i / 64] &= ~(UInt(1) << (i % 64));
  if (is_full) {
    for (Int index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
      i /= 64;
      is_full = bitmap_indexes_[index_id][i / 64] == ~UInt(0);
      bitmap_indexes_[index_id][i / 64] &= ~(UInt(1) << (i % 64));
      if (!is_full) {
        break;
      }
    }
  }
}

Int Table::find_zero_bit() const {
  if (num_rows() == (max_row_id() - MIN_ROW_ID + 1)) {
    return max_row_id() + 1;
  }
  Int pos = 0;
  for (Int i = bitmap_indexes_.size(); i > 0; ) {
    pos = (pos * 64) + ::__builtin_ctzll(~bitmap_indexes_[--i][pos]);
  }
  return (pos * 64) + ::__builtin_ctzll(~bitmap_[pos]);
}

bool Table::reserve_bit(Error *error, Int i) {
  if (i > MAX_ROW_ID) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT,
                    "Invalid argument: i = %" PRIi64, i);
    return false;
  }
  // Resize the bitmap if required.
  Int block_id = i / 64;
  if (block_id >= bitmap_.size()) {
    if (!bitmap_.resize(error, block_id + 1, 0)) {
      return false;
    }
  }
  // Resize the existing bitmap indexes if required.
  for (Int index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
    block_id /= 64;
    if (block_id >= bitmap_indexes_[index_id].size()) {
      if (!bitmap_indexes_[index_id].resize(error, block_id + 1, 0)) {
        return false;
      }
    }
  }
  // Add bitmap indexes if requires.
  Int depth = bitmap_indexes_.size();
  while (block_id > 0) {
    block_id /= 64;
    if (!bitmap_indexes_.resize(error, depth + 1) ||
        !bitmap_indexes_[depth].resize(error, block_id + 1, 0)) {
      return false;
    }
    if (depth == 0) {
      bitmap_indexes_[depth][0] = bitmap_[0] != 0;
    } else {
      bitmap_indexes_[depth][0] = bitmap_indexes_[depth - 1][0] != 0;
    }
  }
  return true;
}

Column *Table::find_column_with_id(Error *error,
                                   const StringCRef &name,
                                   Int *column_id) const {
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

}  // namespace impl
}  // namespace grnxx
