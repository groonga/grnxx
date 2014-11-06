#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/index.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

class Table;

struct ColumnOptions {
  // The referenced (parent) table.
  String reference_table_name;

  ColumnOptions() : reference_table_name() {}
};

class Column {
 public:
  Column() = default;

  // Return the table.
  virtual Table *table() const = 0;
  // Return the name.
  virtual String name() const = 0;
  // Return the data type.
  virtual DataType data_type() const = 0;
  // Return the reference table.
  // If "this" is not a reference column, returns nullptr.
  virtual Table *reference_table() const = 0;
  // Return whether "this" is a key column or not.
  virtual bool is_key() const = 0;
  // Return the number of indexes.
  virtual size_t num_indexes() const = 0;

  // Create an index.
  //
  // On success, returns the index.
  // On failure, throws an exception.
  virtual Index *create_index(
      const String &name,
      IndexType type,
      const IndexOptions &options = IndexOptions()) = 0;

  // Remove an index.
  //
  // On failure, throws an exception.
  virtual void remove_index(const String &name) = 0;

  // Rename an index.
  //
  // On failure, throws an exception.
  virtual void rename_index(const String &name, const String &new_name) = 0;

  // Change the order of indexes.
  //
  // If "prev_name" is an empty string, moves an index named "name" to the
  // head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves an index named "name" to next to an index named
  // "prev_name".
  //
  // On failure, throws an exception.
  virtual void reorder_index(const String &name,
                             const String &prev_name) = 0;

  // Return the "i"-th index.
  //
  // If "i" >= "num_indexes()", the behavior is undefined.
  virtual Index *get_index(size_t i) const = 0;

  // Find an index.
  //
  // If found, returns the index.
  // If not found, returns nullptr.
  virtual Index *find_index(const String &name) const = 0;

  // Get a value.
  //
  // If "row_id" is valid, stores the value into "*datum".
  // If "row_id" is invalid, stores N/A into "*datum".
  // On failure, throws an exception.
  virtual void get(Int row_id, Datum *datum) const = 0;

  // Set a value.
  //
  // On failure, throws an exception.
  virtual void set(Int row_id, const Datum &datum) = 0;

  // Return whether "this" contains "datum" or not.
  virtual bool contains(const Datum &datum) const = 0;

  // Find "datum" in the column.
  //
  // If found, returns the row ID.
  // If not found, returns N/A.
  virtual Int find_one(const Datum &datum) const = 0;

//  virtual std::unique_ptr<Cursor> create_cursor(
//      const CursorOptions &options = CursorOptions()) = 0;

 protected:
  virtual ~Column() = default;
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
