#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include <memory>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

class DB;

struct TableOptions {
};

class Table {
 public:
  Table() = default;

  // Return the owner DB.
  virtual DB *db() const = 0;
  // Return the name.
  virtual String name() const = 0;
  // Return the number of columns.
  virtual size_t num_columns() const = 0;
  // Return the key column, or nullptr if the table has no key column.
  virtual Column *key_column() const = 0;
  // Return the number of rows.
  virtual size_t num_rows() const = 0;
  // Return the maximum row ID.
  virtual Int max_row_id() const = 0;
  // Return whether "this" is empty or not.
  virtual bool is_empty() const = 0;
  // Return whether or not there are invalid records before the last record.
  virtual bool is_full() const = 0;

  // Create a column with "name", "data_type", and "options".
  //
  // On success, returns a pointer to the column.
  // On failure, throws an exception.
  virtual Column *create_column(
      const String &name,
      DataType data_type,
      const ColumnOptions &options = ColumnOptions()) = 0;

  // Remove a column named "name".
  //
  // On failure, throws an exception.
  virtual void remove_column(const String &name) = 0;

  // Rename a column named "name" to "new_name".
  //
  // On failure, throws an exception.
  virtual void rename_column(const String &name,
                             const String &new_name) = 0;

  // Change the order of columns.
  //
  // If "prev_name" is an empty string, moves a column named "name" to the
  // head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves a column named "name" to next to a column named
  // "prev_name".
  //
  // On failure, throws an exception.
  virtual void reorder_column(const String &name,
                              const String &prev_name) = 0;

  // Get the "i"-th column.
  //
  // If "i" >= "num_columns", the behavior is undefined.
  //
  // On success, returns a pointer to the column.
  virtual Column *get_column(size_t column_id) const = 0;

  // Find a column named "name".
  //
  // If found, returns a pointer to the column.
  // If not found, returns nullptr.
  virtual Column *find_column(const String &name) const = 0;

  // Set a key column.
  //
  // Fails if the table already has a key column.
  //
  // On failure, throws an exception.
  virtual void set_key_column(const String &name) = 0;

  // Unset a key column.
  //
  // Fails if the table does not have a key column.
  //
  // On failure, throws an exception.
  virtual void unset_key_column() = 0;

  // Insert a row.
  //
  // Fails if "key" is invalid.
  //
  // On success, returns the row ID.
  // On failure, throws an exception.
  virtual Int insert_row(const Datum &key = NA()) = 0;

  // Find or insert a row.
  //
  // If "key" does not exist, insert a row with "key".
  // If "inserted" != nullptr, stores whether inserted or not into "*inserted".
  //
  // On success, returns the row ID.
  // On failure, throws an exception.
  virtual Int find_or_insert_row(const Datum &key,
                                 bool *inserted = nullptr) = 0;

  // Insert a row.
  //
  // Fails if "row_id" specifies an existing row.
  //
  // On failure, throws an exception.
  virtual void insert_row_at(Int row_id, const Datum &key = NA()) = 0;

  // Remove a row.
  //
  // On failure, throws an exception.
  virtual void remove_row(Int row_id) = 0;

  // Return whether a row is valid or not.
  virtual bool test_row(Int row_id) const = 0;

  // Find a row with "key".
  //
  // If found, returns the row ID.
  // If not found, returns N/A.
  // On failure, throws an exception.
  virtual Int find_row(const Datum &key) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns a pointer to the cursor.
  // On failure, throws an exception.
  virtual std::unique_ptr<Cursor> create_cursor(
      const CursorOptions &options = CursorOptions()) const = 0;

 protected:
  virtual ~Table() = default;
};

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
