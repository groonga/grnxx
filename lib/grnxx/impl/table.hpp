#ifndef GRNXX_IMPL_TABLE_HPP
#define GRNXX_IMPL_TABLE_HPP

#include <cstdint>
#include <memory>

#include "grnxx/db.hpp"
#include "grnxx/impl/column.hpp"
#include "grnxx/table.hpp"

namespace grnxx {
namespace impl {

using DBInterface = grnxx::DB;
using TableInterface = grnxx::Table;

class DB;

class Table : public TableInterface {
 public:
  // -- Public API (grnxx/table.hpp) --

  Table(DB *db, const String &name);
  ~Table();

  DBInterface *db() const;
  String name() const {
    return name_;
  }
  size_t num_columns() const {
    return columns_.size();
  }
  ColumnBase *key_column() const {
    return key_column_;
  }
  size_t num_rows() const {
    return num_rows_;
  }
  Int max_row_id() const {
    return max_row_id_;
  }
  bool is_empty() const {
    return num_rows_ == 0;
  }
  bool is_full() const {
    return is_empty() ||
           (num_rows_ == static_cast<size_t>(max_row_id_.value() + 1));
  }

  ColumnBase *create_column(const String &name,
                            DataType data_type,
                            const ColumnOptions &options);
  void remove_column(const String &name);
  void rename_column(const String &name, const String &new_name);
  void reorder_column(const String &name, const String &prev_name);

  ColumnBase *get_column(size_t i) const {
    return columns_[i].get();
  }
  ColumnBase *find_column(const String &name) const;

  void set_key_column(const String &name);
  void unset_key_column();

  Int insert_row(const Datum &key);
  Int find_or_insert_row(const Datum &key, bool *inserted);
  void insert_row_at(Int row_id, const Datum &key);

  void remove_row(Int row_id);

  bool test_row(Int row_id) const {
    size_t bit_id = static_cast<size_t>(row_id.value());
    size_t block_id = bit_id / 64;
    return (block_id < bitmap_.size()) &&
           ((bitmap_[block_id] & (uint64_t(1) << (bit_id % 64))) != 0);
  }
  Int find_row(const Datum &key) const;

  std::unique_ptr<Cursor> create_cursor(const CursorOptions &options) const;

  // -- Internal API --

  // Create a new table.
  //
  // On success, returns the table.
  // On failure, throws an exception.
  static std::unique_ptr<Table> create(
      DB *db,
      const String &name,
      const TableOptions &options);

  // Return the owner DB.
  DB *_db() const {
    return db_;
  }

  // Return whether a row is valid or not.
  //
  // If "row_id" is too large, the behavior is undefined.
  bool _test_row(size_t row_id) const {
    return (bitmap_[row_id / 64] & (uint64_t(1) << (row_id % 64))) != 0;
  }

  // Change the table name.
  //
  // On failure, throws an exception.
  void rename(const String &new_name);

  // Return whether the table is removable or not.
  bool is_removable() const;

  // Append a referrer column.
  //
  // On failure, throws an exception.
  void append_referrer_column(ColumnBase *column);

  // Remove a referrer column.
  //
  // On failure, throws an exception.
  void remove_referrer_column(ColumnBase *column);

 private:
  DB *db_;
  String name_;
  Array<std::unique_ptr<ColumnBase>> columns_;
  Array<ColumnBase *> referrer_columns_;
  ColumnBase *key_column_;
  size_t num_rows_;
  Int max_row_id_;
  Array<uint64_t> bitmap_;
  Array<Array<uint64_t>> bitmap_indexes_;

  // Find the next row ID candidate.
  Int find_next_row_id() const;
  // Reserve a row.
  //
  // On failure, throws an exception.
  void reserve_row(Int row_id);
  // Validate a row.
  void validate_row(Int row_id);
  // Invalidate a row.
  void invalidate_row(Int row_id);

  // Find a column with its ID.
  //
  // If found, returns the column and stores its ID to "*column_id".
  // If not found, returns nullptr.
  ColumnBase *find_column_with_id(const String &name,
                                  size_t *column_id) const;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_TABLE_HPP
