#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Table {
 public:
  Table() = default;

  // Return the owner DB.
  virtual DB *db() const = 0;
  // Return the name.
  virtual StringCRef name() const = 0;
  // Return the number of columns.
  virtual Int num_columns() const = 0;
  // Return the key column, or nullptr if the table has no key column.
  virtual Column *key_column() const = 0;
  // Return the number of rows.
  virtual Int num_rows() const = 0;
  // Return the maximum row ID.
  virtual Int max_row_id() const = 0;

  // Create a column with "name", "data_type", and "options".
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Column *create_column(
      Error *error,
      const StringCRef &name,
      DataType data_type,
      const ColumnOptions &options = ColumnOptions()) = 0;

  // Remove a column named "name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed column must not be used after deletion.
  virtual bool remove_column(Error *error, const StringCRef &name) = 0;

  // Rename a column named "name" to "new_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool rename_column(Error *error,
                             const StringCRef &name,
                             const StringCRef &new_name) = 0;

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
  virtual bool reorder_column(Error *error,
                              const StringCRef &name,
                              const StringCRef &prev_name) = 0;

  // Get a column identified by "column_id".
  //
  // If "column_id" is invalid, the result is undefined.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Column *get_column(Int column_id) const = 0;

  // Find a column named "name".
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Column *find_column(Error *error, const StringCRef &name) const = 0;

  // Set the key attribute to the column named "name".
  //
  // Fails if the table already has a key column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_key_column(Error *error, const StringCRef &name) = 0;

  // Unset the key attribute of the key column.
  //
  // Fails if the table does not have a key column.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool unset_key_column(Error *error) = 0;

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
  virtual bool insert_row(Error *error,
                          Int request_row_id,
                          const Datum &key,
                          Int *result_row_id) = 0;

  // Remove a row identified by "row_id".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool remove_row(Error *error, Int row_id) = 0;

  // Check the validity of a row.
  //
  // Returns true if "row_id" specifies a row in use.
  // Otherwise, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool test_row(Error *error, Int row_id) const = 0;

  // Find a row identified by "key".
  //
  // Fails if the table does not have a key column.
  //
  // On success, returns the row ID.
  // On failure, returns NULL_ROW_ID and stores error information into
  // "*error" if "error" != nullptr.
  virtual Int find_row(Error *error, const Datum &key) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns a pointer to the cursor.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual unique_ptr<Cursor> create_cursor(
      Error *error,
      const CursorOptions &options = CursorOptions()) const = 0;

  // TODO: Grouping (drilldown).
  //
  // 分類器を作成する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - オプションが不正である．
  // - リソースが確保できない．
//  virtual unique_ptr<Grouper> create_grouper(
//      Error *error,
//      unique_ptr<Expression> &&expression,
//      const GrouperOptions &options = GrouperOptions()) const = 0;

 protected:
  virtual ~Table() = default;
};

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
