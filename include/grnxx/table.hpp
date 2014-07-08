#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include <vector>

#include "grnxx/types.hpp"

#include "grnxx/name.hpp"

namespace grnxx {

struct TableOptions {
};

class Table {
 public:
   ~Table();

  // Return the owner DB.
  DB *db() const {
    return db_;
  }
  // Return the name.
  String name() const {
    return name_.ref();
  }
  // Return the number of columns.
  size_t num_columns() const {
    return columns_.size();
  }
  // Return the key column, or nullptr if the table has no key column.
  Column *key_column() const {
    return key_column_;
  }
  // Return the maximum row ID.
  Int max_row_id() const {
    return max_row_id_;
  }

  // Create a column with "name", "data_type", and "options".
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *create_column(Error *error,
                        String name,
                        DataType data_type,
                        const ColumnOptions &options);

  // Remove a column named "name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed column must not be used after deletion.
  bool remove_column(Error *error, String name);

  // Rename a column named "name" to "new_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_column(Error *error,
                     String name,
                     String new_name);

  // Change the order of columns.
  //
  // Moves a column named "name" to the head if "prev_name" is nullptr or an
  // empty string.
  // Does nothing if "name" == "prev_name".
  // Moves a column named "name" to next to a column named "prev_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reorder_column(Error *error,
                      String name,
                      String prev_name);

  // Get a column identified by "column_id".
  //
  // If "column_id" is invalid, the result is undefined.
  //
  // Returns a pointer to the column on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *get_column(size_t column_id) const {
    return columns_[column_id].get();
  }

  // Find a column named "name".
  //
  // Returns a pointer to the column on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *find_column(Error *error, String name) const;

  // Set the key attribute to the column named "name".
  //
  // Fails if the table already has a key column.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool set_key_column(Error *error, String name);

  // Unset the key attribute of the key column.
  //
  // Fails if the table does not have a key column.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool unset_key_column(Error *error);

  // Insert a row.
  //
  // If "request_row_id" is valid, inserts a row identified by
  // "request_row_id".
  // If "key" is valid, inserts a row identified by "key".
  // If both are invalid, the next row ID is allocated.
  //
  // Fails if "request_row_id" is valid and points to an existing row.
  // Fails if the table has a key column and "key" is invalid.
  // Fails if the table does not have a key column and "key" is valid.
  //
  // On success, stores the inserted row ID into "*result_row_id".
  // On failure, if "request_row_id" or "key" matches an existing row,
  // stores the matched row ID into "*result_row_id".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool insert_row(Error *error,
                  Int request_row_id,
                  const Datum &key,
                  Int *result_row_id);

  // Remove a row identified by "row_id".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool remove_row(Error *error, Int row_id);

  // Check the validity of a row.
  //
  // Returns true if the row is valid.
  // Otherwise, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool test_row(Error *error, Int row_id) const;

  // Find a row identified by "key".
  //
  // Fails if the table does not have a key column.
  //
  // Returns the row ID on success.
  // On failure, returns NULL_ROW_ID and stores error information into
  // "*error" if "error" != nullptr.
  Int find_row(Error *error, const Datum &key) const;

  // Create a cursor to get records.
  //
  // Returns a pointer to the cursor on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<Cursor> create_cursor(
      Error *error,
      const CursorOptions &options) const;

  // Create an object to build ordering information.
  //
  // Returns a pointer to the builder on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
//  unique_ptr<OrderBuilder> create_order_builder(
//      Error *error) const;

  // Create an object to build pipelines.
  //
  // Returns a pointer to the builder on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
//  unique_ptr<PipelineBuilder> create_pipeline_builder(
//      Error *error) const;

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
//      const GrouperOptions &options) const;

 private:
  DB *db_;
  Name name_;
  std::vector<unique_ptr<Column>> columns_;
  Column *key_column_;
  Int max_row_id_;
  std::vector<uint64_t> bitmap_;

  // Create a new table.
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Table> create(Error *error,
                                  DB *db,
                                  String name,
                                  const TableOptions &options);

  Table();

  // Read a bit from the bitmap.
  //
  // Assumes that the bitmap size is greater than "row_id".
  bool get_bit(Int row_id) const {
    return (bitmap_[row_id / 64] & (Int(1) << (row_id % 64))) != 0;
  }
  // Set a bit of the bitmap.
  //
  // Assumes that the bitmap size is greater than "row_id".
  void set_bit(Int row_id) {
    bitmap_[row_id / 64] |= Int(1) << (row_id % 64);
  }
  // Unset a bit of the bitmap.
  //
  // Assumes that the bitmap size is greater than "row_id".
  void unset_bit(Int row_id) {
    bitmap_[row_id / 64] &= ~(Int(1) << (row_id % 64));
  }
  // Reverse a bit.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reserve_bit(Error *error, Int row_id);

  // Change the table name.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, String new_name);

  // Return whether the table is removable or not.
  bool is_removable();

  // Find a column with its ID.
  //
  // Returns a pointer to the column on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Column *find_column_with_id(Error *error,
                              String name,
                              size_t *column_id) const;

  friend class DB;
  friend class TableCursor;
};

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
