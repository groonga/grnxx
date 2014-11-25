#include "grnxx/impl/table.hpp"

#include "grnxx/impl/cursor.hpp"
#include "grnxx/impl/db.hpp"

namespace grnxx {
namespace impl {

// -- TableRegularCursor --

class TableRegularCursor : public Cursor {
 public:
  // -- Public API --

  ~TableRegularCursor() {}

  size_t read(ArrayRef<Record> records);

  // -- Internal API --

  static std::unique_ptr<Cursor> create(const Table *table,
                                        const CursorOptions &options);

 private:
  const Table *table_;
  int64_t max_row_id_;
  bool is_full_;
  size_t offset_left_;
  size_t limit_left_;
  int64_t next_row_id_;

  TableRegularCursor(const Table *table, const CursorOptions &options);
};

size_t TableRegularCursor::read(ArrayRef<Record> records) {
  if (records.size() <= 0) {
    return 0;
  }
  size_t count = 0;
  if (is_full_) {
    // There are no false bits in the bitmap and bit checks are not required.
    size_t num_remaining_records = max_row_id_ - next_row_id_ + 1;
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
    count = records.size();
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (size_t i = 0; i < count; ++i) {
      records.set(i, Record(Int(next_row_id_), Float(0.0)));
      ++next_row_id_;
      --limit_left_;
    }
  } else {
    // There exist false bits in the bitmap and bit checks are required.
    while (next_row_id_ <= max_row_id_) {
      if (!table_->_test_row(next_row_id_)) {
        ++next_row_id_;
        continue;
      }
      if (offset_left_ > 0) {
        --offset_left_;
        ++next_row_id_;
      } else {
        records.set(count, Record(Int(next_row_id_), Float(0.0)));
        --limit_left_;
        ++count;
        ++next_row_id_;
        if ((limit_left_ <= 0) || (count >= records.size())) {
          break;
        }
      }
    }
  }
  return count;
}

std::unique_ptr<Cursor> TableRegularCursor::create(
    const Table *table,
    const CursorOptions &options) try {
  if (table->is_empty()) {
    return std::unique_ptr<Cursor>(new EmptyCursor);
  }
  return std::unique_ptr<Cursor>(new TableRegularCursor(table, options));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

TableRegularCursor::TableRegularCursor(const Table *table,
                                       const CursorOptions &options)
    : table_(table),
      max_row_id_(table->max_row_id().raw()),
      is_full_(table->is_full()),
      offset_left_(options.offset),
      limit_left_(options.limit),
      next_row_id_(0) {}

// -- TableReverseCursor --

class TableReverseCursor : public Cursor {
 public:
  // -- Public API --

  ~TableReverseCursor() {}

  size_t read(ArrayRef<Record> records);

  // -- Internal API --

  static std::unique_ptr<Cursor> create(const Table *table,
                                        const CursorOptions &options);

 private:
  const Table *table_;
  bool is_full_;
  size_t offset_left_;
  size_t limit_left_;
  int64_t next_row_id_;

  TableReverseCursor(const Table *table, const CursorOptions &options);
};

size_t TableReverseCursor::read(ArrayRef<Record> records) {
  if (records.size() <= 0) {
    return 0;
  }
  size_t count = 0;
  if (is_full_) {
    // There are no false bits in the bitmap and bit checks are not required.
    size_t num_remaining_records = next_row_id_ + 1;
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
    count = records.size();
    if (count > num_remaining_records) {
      count = num_remaining_records;
    }
    if (count > limit_left_) {
      count = limit_left_;
    }
    for (size_t i = 0; i < count; ++i) {
      records.set(i, Record(Int(next_row_id_), Float(0.0)));
      --next_row_id_;
      --limit_left_;
    }
  } else {
    while (next_row_id_ >= 0) {
      // There exist false bits in the bitmap and bit checks are required.
      if (!table_->_test_row(next_row_id_)) {
        --next_row_id_;
        continue;
      }
      if (offset_left_ > 0) {
        --offset_left_;
        --next_row_id_;
      } else {
        records.set(count, Record(Int(next_row_id_), Float(0.0)));
        --limit_left_;
        ++count;
        --next_row_id_;
        if ((limit_left_ <= 0) || (count >= records.size())) {
          break;
        }
      }
    }
  }
  return count;
}

std::unique_ptr<Cursor> TableReverseCursor::create(
    const Table *table,
    const CursorOptions &options) try {
  if (table->is_empty()) {
    return std::unique_ptr<Cursor>(new EmptyCursor);
  }
  return std::unique_ptr<Cursor>(new TableReverseCursor(table, options));
} catch (const std::bad_alloc &) {
  throw "Memorya allocation failed";  // TODO
}

TableReverseCursor::TableReverseCursor(const Table *table,
                                       const CursorOptions &options)
    : table_(table),
      is_full_(table->is_full()),
      offset_left_(options.offset),
      limit_left_(options.limit),
      next_row_id_(table->max_row_id().raw()) {}

// -- Table --

Table::Table(DB *db, const String &name)
    : TableInterface(),
      db_(db),
      name_(name),
      columns_(),
      referrer_columns_(),
      key_column_(nullptr),
      num_rows_(0),
      max_row_id_(NA()),
      bitmap_(),
      bitmap_indexes_() {}

Table::~Table() {}

DBInterface *Table::db() const {
  return db_;
}

ColumnBase *Table::create_column(const String &name,
                                 DataType data_type,
                                 const ColumnOptions &options) {
  if (find_column(name)) {
    throw "Column already exists";  // TODO
  }
  columns_.reserve(columns_.size() + 1);
  std::unique_ptr<ColumnBase> new_column =
      ColumnBase::create(this, name, data_type, options);
  if (new_column->_reference_table()) {
    new_column->_reference_table()->append_referrer_column(new_column.get());
  }
  columns_.push_back(std::move(new_column));
  return columns_.back().get();
}

void Table::remove_column(const String &name) {
  size_t column_id;
  if (!find_column_with_id(name, &column_id)) {
    throw "Column not found";  // TODO
  }
  if (!columns_[column_id]->is_removable()) {
    throw "Column not removable";  // TODO
  }
  ColumnBase *column = columns_[column_id].get();
  if (column == key_column_) {
    key_column_ = nullptr;
  }
  if (column->reference_table()) {
    column->_reference_table()->remove_referrer_column(column);
  }
  columns_.erase(column_id);
}

void Table::rename_column(const String &name, const String &new_name) {
  size_t column_id;
  if (!find_column_with_id(name, &column_id)) {
    throw "Column not found";  // TODO
  }
  if (name == new_name) {
    return;
  }
  if (find_column(new_name)) {
    throw "Column already exists";  // TODO
  }
  columns_[column_id]->rename(new_name);
}

void Table::reorder_column(const String &name, const String &prev_name) {
  size_t column_id;
  if (!find_column_with_id(name, &column_id)) {
    throw "Column not found";  // TODO
  }
  size_t new_column_id = 0;
  if (prev_name.size() != 0) {
    size_t prev_column_id;
    if (!find_column_with_id(prev_name, &prev_column_id)) {
      throw "Column not found";  // TODO
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
}

ColumnBase *Table::find_column(const String &name) const {
  for (size_t i = 0; i < num_columns(); ++i) {
    if (name == columns_[i]->name()) {
      return columns_[i].get();
    }
  }
  return nullptr;
}

void Table::set_key_column(const String &name) {
  if (key_column_) {
    throw "Key column already exists";  // TODO
  }
  ColumnBase *column = find_column(name);
  if (!column) {
    throw "Column not found";  // TODO
  }
  column->set_key_attribute();
  key_column_ = column;
}

void Table::unset_key_column() {
  if (!key_column_) {
    throw "Key column not found";  // TODO
  }
  key_column_->unset_key_attribute();
  key_column_ = nullptr;
}

Int Table::insert_row(const Datum &key) {
  if (key_column_) {
    if (!find_row(key).is_na()) {
      throw "Key already exists";  // TODO
    }
  } else if (key.type() != NA_DATA) {
    throw "Wrong key";  // TODO
  }
  Int row_id = find_next_row_id();
  reserve_row(row_id);
  if (key_column_) {
    key_column_->set_key(row_id, key);
  }
  validate_row(row_id);
  return row_id;
}

Int Table::find_or_insert_row(const Datum &key, bool *inserted) {
  if (key_column_) {
    Int row_id = find_row(key);
    if (!row_id.is_na()) {
      if (inserted) {
        *inserted = false;
      }
      return row_id;
    }
  } else if (key.type() != NA_DATA) {
    throw "Wrong key";  // TODO
  }
  Int row_id = find_next_row_id();
  reserve_row(row_id);
  if (key_column_) {
    key_column_->set_key(row_id, key);
  }
  validate_row(row_id);
  *inserted = true;
  return row_id;
}

void Table::insert_row_at(Int row_id, const Datum &key) {
  if (test_row(row_id)) {
    throw "Row ID already validated";  // TODO
  }
  if (row_id.raw() < 0) {
    throw "Negative row ID";  // TODO
  }
  if (key_column_) {
    if (!find_row(key).is_na()) {
      throw "Key already exists";  // TODO
    }
  }
  reserve_row(row_id);
  if (key_column_) {
    key_column_->set_key(row_id, key);
  }
  validate_row(row_id);
}

void Table::remove_row(Int row_id) {
  if (!test_row(row_id)) {
    throw "Invalid row ID";  // TODO
  }
  // TODO: Check removability.
  for (size_t i = 0; i < referrer_columns_.size(); ++i) {
    if (referrer_columns_[i]->is_key()) {
      throw "Reffered to from a key column";  // TODO
    }
  }
  // Unset column values.
  for (size_t i = 0; i < num_columns(); ++i) {
    columns_[i]->unset(row_id);
  }
  invalidate_row(row_id);

  // TODO: Clear referrers.
//  for (size_t i = 0; i < referrer_columns_.size(); ++i) {
//    referrer_columns_[i]->clear_references(row_id);
//  }
}

Int Table::find_row(const Datum &key) const {
  if (!key_column_) {
    throw "No key column";  // TODO
  }
  return key_column_->find_one(key);
}

std::unique_ptr<Cursor> Table::create_cursor(
    const CursorOptions &options) const {
  switch (options.order_type) {
    case CURSOR_REGULAR_ORDER: {
      return TableRegularCursor::create(this, options);
    }
    case CURSOR_REVERSE_ORDER: {
      return TableReverseCursor::create(this, options);
    }
    default: {
      throw "Invalid order type";  // TODO
    }
  }
}

std::unique_ptr<Table> Table::create(DB *db,
                                     const String &name,
                                     const TableOptions &) try {
  std::unique_ptr<Table> table(new Table(db, name));
  return table;
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void Table::rename(const String &new_name) {
  if (name_ != new_name) {
    name_.assign(new_name);
  }
}

bool Table::is_removable() const {
  // A referenced table (except self-reference) is not removable.
  for (size_t i = 0; i < referrer_columns_.size(); ++i) {
    if (referrer_columns_[i]->table() != this) {
      return false;
    }
  }
  return true;
}

void Table::append_referrer_column(ColumnBase *column) {
  if (column->reference_table() != this) {
    throw "Wrong referrer column";  // TODO
  }
  referrer_columns_.push_back(column);
}

void Table::remove_referrer_column(ColumnBase *column) {
  for (size_t i = 0; i < referrer_columns_.size(); ++i) {
    if (column == referrer_columns_[i]) {
      referrer_columns_.erase(i);
      return;
    }
  }
  throw "Referrer column not found";  // TODO
}

Int Table::find_next_row_id() const {
  if (is_empty()) {
    return Int(0);
  } else if (is_full()) {
    return Int(max_row_id_.raw() + 1);
  }
  size_t pos = 0;
  for (size_t i = bitmap_indexes_.size(); i > 0; ) {
    // TODO: ::__builtin_ctzll() is not available on VC++.
    pos = (pos * 64) + ::__builtin_ctzll(~bitmap_indexes_[--i][pos]);
  }
  // TODO: ::__builtin_ctzll() is not available on VC++.
  return Int((pos * 64) + ::__builtin_ctzll(~bitmap_[pos]));
}

void Table::reserve_row(Int row_id) {
  // TODO: Define the upper limit for row ID.
//  if (row_id.raw() > MAX_ROW_ID_VALUE) {
//    throw "Too large row ID";  // TODO
//  }
  // Resize the bitmap if required.
  size_t block_id = static_cast<size_t>(row_id.raw()) / 64;
  if (block_id >= bitmap_.size()) {
    bitmap_.resize(block_id + 1, 0);
  }
  // Resize the existing bitmap indexes if required.
  for (size_t index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
    block_id /= 64;
    if (block_id >= bitmap_indexes_[index_id].size()) {
      bitmap_indexes_[index_id].resize(block_id + 1, 0);
    }
  }
  // Add bitmap indexes if required.
  size_t depth = bitmap_indexes_.size();
  while (block_id > 0) {
    block_id /= 64;
    bitmap_indexes_.resize(depth + 1);
    bitmap_indexes_[depth].resize(block_id + 1, 0);
    if (depth == 0) {
      bitmap_indexes_[depth][0] = bitmap_[0] != 0;
    } else {
      bitmap_indexes_[depth][0] = bitmap_indexes_[depth - 1][0] != 0;
    }
  }
}

void Table::validate_row(Int row_id) {
  // Update the bitmap and its indexes.
  size_t bit_id = static_cast<size_t>(row_id.raw());
  bitmap_[bit_id / 64] |= uint64_t(1) << (bit_id % 64);
  if (bitmap_[bit_id / 64] == ~uint64_t(0)) {
    for (size_t index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
      bit_id /= 64;
      bitmap_indexes_[index_id][bit_id / 64] |= uint64_t(1) << (bit_id % 64);
      if (bitmap_indexes_[index_id][bit_id / 64] != ~uint64_t(0)) {
        break;
      }
    }
  }
  // This works well even if "max_row_id_" is N/A.
  if (row_id.raw() > max_row_id_.raw()) {
    max_row_id_ = row_id;
  }
  ++num_rows_;
}

void Table::invalidate_row(Int row_id) {
  // Update the bitmap and its indexes.
  size_t bit_id = row_id.raw();
  bool is_full = (bitmap_[bit_id / 64] == ~uint64_t(0));
  bitmap_[bit_id / 64] &= ~(uint64_t(1) << (bit_id % 64));
  if (is_full) {
    for (size_t index_id = 0; index_id < bitmap_indexes_.size(); ++index_id) {
      bit_id /= 64;
      is_full = bitmap_indexes_[index_id][bit_id / 64] == ~uint64_t(0);
      bitmap_indexes_[index_id][bit_id / 64] &=
           ~(uint64_t(1) << (bit_id % 64));
      if (!is_full) {
        break;
      }
    }
  }
  --num_rows_;
  if (is_empty()) {
    max_row_id_ = Int::na();
  } else if (row_id.match(max_row_id_)) {
    int64_t block_id = (row_id.raw() - 1) / 64;
    while (block_id >= 0) {
      if (bitmap_[block_id] != 0) {
        break;
      }
      --block_id;
    }
    // TODO: ::__builtin_clzll() is not available on VC++.
    max_row_id_ = Int((block_id * 64) + 63 -
                      ::__builtin_clzll(bitmap_[block_id]));
  }
}

ColumnBase *Table::find_column_with_id(const String &name,
                                       size_t *column_id) const {
  for (size_t i = 0; i < num_columns(); ++i) {
    if (name == columns_[i]->name()) {
      if (column_id != nullptr) {
        *column_id = i;
      }
      return columns_[i].get();
    }
  }
  return nullptr;
}

}  // namespace impl
}  // namespace grnxx
