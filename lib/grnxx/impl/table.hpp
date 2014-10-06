#ifndef GRNXX_IMPL_TABLE_HPP
#define GRNXX_IMPL_TABLE_HPP

#include "grnxx/name.hpp"
#include "grnxx/db.hpp"
#include "grnxx/table.hpp"

namespace grnxx {
namespace impl {

class DB;

class Table : public grnxx::Table {
 public:
  // Public API, see grnxx/table.hpp for details.
  Table();
  ~Table();

  grnxx::DB *db() const;
  StringCRef name() const {
    return name_.ref();
  }
  Int num_columns() const {
    return columns_.size();
  }
  Column *key_column() const {
    return key_column_;
  }
  Int num_rows() const {
    return num_rows_;
  }
  Int max_row_id() const {
    return max_row_id_;
  }

  Column *create_column(Error *error,
                        const StringCRef &name,
                        DataType data_type,
                        const ColumnOptions &options = ColumnOptions());
  bool remove_column(Error *error, const StringCRef &name);
  bool rename_column(Error *error,
                     const StringCRef &name,
                     const StringCRef &new_name);
  bool reorder_column(Error *error,
                      const StringCRef &name,
                      const StringCRef &prev_name);

  Column *get_column(Int column_id) const {
    return columns_[column_id].get();
  }
  Column *find_column(Error *error, const StringCRef &name) const;

  bool set_key_column(Error *error, const StringCRef &name);
  bool unset_key_column(Error *error);

  bool insert_row(Error *error,
                  Int request_row_id,
                  const Datum &key,
                  Int *result_row_id);
  bool remove_row(Error *error, Int row_id);

  bool test_row(Error *error, Int row_id) const;
  Int find_row(Error *error, const Datum &key) const;

  unique_ptr<Cursor> create_cursor(
      Error *error,
      const CursorOptions &options = CursorOptions()) const;

  // Internal API.

  // Create a new table.
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Table> create(
      Error *error,
      DB *db,
      const StringCRef &name,
      const TableOptions &options = TableOptions());

  // Return the owner DB.
  DB *_db() const {
    return db_;
  }

  // Change the table name.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, const StringCRef &new_name);

  // Return whether the table is removable or not.
  bool is_removable();

  // Append a referrer column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append_referrer_column(Error *error, Column *column);

  // Append a referrer column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool remove_referrer_column(Error *error, Column *column);

 private:
  DB *db_;
  Name name_;
  Array<unique_ptr<Column>> columns_;
  Array<Column *> referrer_columns_;
  Column *key_column_;
  Int num_rows_;
  Int max_row_id_;
  Array<UInt> bitmap_;
  Array<Array<UInt>> bitmap_indexes_;

  // Get the "i"-th bit from the bitmap.
  bool get_bit(Int i) const {
    return (bitmap_[i / 64] & (Int(1) << (i % 64))) != 0;
  }
  // Set 1 to the "i"-th bit of the bitmap and update bitmap indexes.
  void set_bit(Int i);
  // Set 0 to the "i"-th bit of the bitmap and update bitmap indexes.
  void unset_bit(Int i);
  // Find a zero-bit and return its position.
  Int find_zero_bit() const;
  // Reserve the "i"-th bit for the next row.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reserve_bit(Error *error, Int i);

  // Find a column with its ID.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *find_column_with_id(Error *error,
                              const StringCRef &name,
                              Int *column_id) const;

  friend class TableCursor;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_TABLE_HPP
