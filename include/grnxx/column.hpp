#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

#include <memory>

#include "grnxx/column.hpp"
#include "grnxx/cursor.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

class Table;

struct ColumnOptions {
  // The referenced (parent) table.
  String reference_table_name;

  ColumnOptions();
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
//  // Return whether the column has the key attribute or not.
//  virtual bool is_key() const = 0;
//  // Return the number of indexes.
//  virtual size_t num_indexes() const = 0;

//  // Create an index with "name", "index_type", and "index_options".
//  //
//  // On success, returns a pointer to the index.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual Index *create_index(
//      Error *error,
//      const String &name,
//      IndexType type,
//      const IndexOptions &options = IndexOptions()) = 0;

//  // Remove an index named "name".
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  //
//  // Note: Pointers to the removed index must not be used after deletion.
//  virtual bool remove_index(Error *error, const String &name) = 0;

//  // Rename an index named "name" to "new_name".
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual bool rename_index(Error *error,
//                            const String &name,
//                            const String &new_name) = 0;

//  // Change the order of indexes.
//  //
//  // If "prev_name" is  an empty string, moves an index named "name" to the
//  // head.
//  // If "name" == "prev_name", does nothing.
//  // Otherwise, moves an index named "name" to next to an index named
//  // "prev_name".
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual bool reorder_index(Error *error,
//                             const String &name,
//                             const String &prev_name) = 0;

//  // Get an index identified by "index_id".
//  //
//  // If "index_id" is invalid, the result is undefined.
//  //
//  // On success, returns a pointer to the index.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual Index *get_index(Int index_id) const = 0;

//  // Find an index named "name".
//  //
//  // On success, returns a pointer to the index.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual Index *find_index(Error *error, const String &name) const = 0;

  // Get a value associated with "row_id".
  //
  // If "row_id" is invalid, stores N/A into "*datum".
  //
  // On failure, throws an exception.
  virtual void get(Int row_id, Datum *datum) const = 0;

  // Set a value.
  //
  // On failure, throws an exception.
  virtual void set(Int row_id, const Datum &datum) = 0;

//  // Check if "datum" exists in the column or not.
//  //
//  // If exists, returns true.
//  // Otherwise, returns false.
//  virtual bool contains(const Datum &datum) const = 0;

//  // Find "datum" in the column.
//  //
//  // On success, returns the row ID of the matched value.
//  // On failure, returns NULL_ROW_ID.
//  virtual Int find_one(const Datum &datum) const = 0;

  std::unique_ptr<Cursor> create_cursor(
      const CursorOptions &options = CursorOptions());

 protected:
  virtual ~Column() = default;
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
