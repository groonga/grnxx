#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace impl {

class DB;

}  // namespace impl

class Table {
 public:
   ~Table();

  // Return the owner DB.
  DB *db() const {
    return db_;
  }
  // Return the name.
  StringCRef name() const {
    return name_.ref();
  }
  // Return the number of columns.
  Int num_columns() const {
    return columns_.size();
  }
  // Return the key column, or nullptr if the table has no key column.
  Column *key_column() const {
    return key_column_;
  }
  // Return the number of rows.
  Int num_rows() const {
    return num_rows_;
  }
  // Return the maximum row ID.
  Int max_row_id() const {
    return max_row_id_;
  }

  // Create a column with "name", "data_type", and "options".
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *create_column(Error *error,
                        const StringCRef &name,
                        DataType data_type,
                        const ColumnOptions &options = ColumnOptions());

  // Remove a column named "name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed column must not be used after deletion.
  bool remove_column(Error *error, const StringCRef &name);

  // Rename a column named "name" to "new_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_column(Error *error,
                     const StringCRef &name,
                     const StringCRef &new_name);

  // Change the order of columns.
  //
  // If "prev_name" is an empty string, moves a column named "name" to the
  // head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves a column named "name" to next to a column named
  // "prev_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reorder_column(Error *error,
                      const StringCRef &name,
                      const StringCRef &prev_name);

  // Get a column identified by "column_id".
  //
  // If "column_id" is invalid, the result is undefined.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *get_column(Int column_id) const {
    return columns_[column_id].get();
  }

  // Find a column named "name".
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *find_column(Error *error, const StringCRef &name) const;

  // Set the key attribute to the column named "name".
  //
  // Fails if the table already has a key column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool set_key_column(Error *error, const StringCRef &name);

  // Unset the key attribute of the key column.
  //
  // Fails if the table does not have a key column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool unset_key_column(Error *error);

  // Insert a row.
  //
  // If "request_row_id" specifies an unused row ID, uses the row ID.
  // If the table has a key column, "key" is used as the key of the new row.
  // If "request_row_id" == NULL_ROW_ID, an unused row ID or max_row_id() + 1
  // is allocated for the new row.
  //
  // Fails if "request_row_id" specifies an existing row.
  // Fails if the table has a key column and "key" is invalid.
  //
  // On success, stores the inserted row ID into "*result_row_id".
  // On failure, if "request_row_id" or "key" matches an existing row,
  // stores the matched row ID into "*result_row_id".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool insert_row(Error *error,
                  Int request_row_id,
                  const Datum &key,
                  Int *result_row_id);

  // Remove a row identified by "row_id".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool remove_row(Error *error, Int row_id);

  // Check the validity of a row.
  //
  // Returns true if "row_id" specifies a row in use.
  // Otherwise, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool test_row(Error *error, Int row_id) const;

  // Find a row identified by "key".
  //
  // Fails if the table does not have a key column.
  //
  // On success, returns the row ID.
  // On failure, returns NULL_ROW_ID and stores error information into
  // "*error" if "error" != nullptr.
  Int find_row(Error *error, const Datum &key) const;

  // Create a cursor to get records.
  //
  // On success, returns a pointer to the cursor.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<Cursor> create_cursor(
      Error *error,
      const CursorOptions &options = CursorOptions()) const;

  // TODO: Grouping (drilldown).
  //
  // 分類器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
//  unique_ptr<Grouper> create_grouper(
//      Error *error,
//      unique_ptr<Expression> &&expression,
//      const GrouperOptions &options = GrouperOptions()) const;

  // TODO: This member function should be hidden.
  //
  // Append a referrer column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append_referrer_column(Error *error, Column *column);

  // TODO: This member function should be hidden.
  //
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

  Table();

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

  // Change the table name.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, const StringCRef &new_name);

  // Return whether the table is removable or not.
  bool is_removable();

  // Find a column with its ID.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *find_column_with_id(Error *error,
                              const StringCRef &name,
                              Int *column_id) const;

  friend class impl::DB;
  friend class TableCursor;
};

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
