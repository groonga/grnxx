#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

#include "grnxx/array.hpp"
#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class Column {
 public:
  virtual ~Column();

  // Return the table.
  Table *table() const {
    return table_;
  }
  // Return the name.
  String name() const {
    return name_.ref();
  }
  // Return the data type.
  DataType data_type() const {
    return data_type_;
  }
  // Return the referenced (parent) table, or nullptr if the column is not a
  // reference column.
  Table *ref_table() const {
    return ref_table_;
  }
  // Return whether the column has the key attribute or not.
  bool has_key_attribute() const {
    return has_key_attribute_;
  }
  // Return the number of indexes.
  Int num_indexes() const {
    return indexes_.size();
  }

  // Create an index with "name", "index_type", and "index_options".
  //
  // On success, returns a pointer to the index.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Index *create_index(
      Error *error,
      String name,
      IndexType type,
      const IndexOptions &options = IndexOptions());

  // Remove an index named "name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed index must not be used after deletion.
  bool remove_index(Error *error, String name);

  // Rename an index named "name" to "new_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_index(Error *error,
                    String name,
                    String new_name);

  // Change the order of indexes.
  //
  // If "prev_name" is  an empty string, moves an index named "name" to the
  // head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves an index named "name" to next to an index named
  // "prev_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reorder_index(Error *error,
                     String name,
                     String prev_name);

  // Get an index identified by "index_id".
  //
  // If "index_id" is invalid, the result is undefined.
  //
  // On success, returns a pointer to the index.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *get_index(Int index_id) const {
    return indexes_[index_id].get();
  }

  // Find an index named "name".
  //
  // On success, returns a pointer to the index.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *find_index(Error *error, String name) const;

  // Set a value.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set(Error *error, Int row_id, const Datum &datum);

  // Get a value.
  //
  // Stores a value identified by "row_id" into "*datum".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool get(Error *error, Int row_id, Datum *datum) const;

  // Create a cursor to get records.
  //
  // On success, returns a pointer to the cursor.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual unique_ptr<Cursor> create_cursor(
      Error *error,
      const CursorOptions &options = CursorOptions()) const;

 protected:
  Table *table_;
  Name name_;
  DataType data_type_;
  Table *ref_table_;
  bool has_key_attribute_;
  Array<unique_ptr<Index>> indexes_;

  Column();

  // Initialize the base members.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool initialize_base(Error *error,
                       Table *table,
                       String name,
                       DataType data_type,
                       const ColumnOptions &options = ColumnOptions());

 private:
  // Create a new column.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Column> create(
      Error *error,
      Table *table,
      String name,
      DataType data_type,
      const ColumnOptions &options = ColumnOptions());

  // Change the column name.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, String new_name);

  // Return whether the column is removable or not.
  bool is_removable();

  // Set the initial key.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_initial_key(Error *error, Int row_id, const Datum &key);

  // Set the default value.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_default_value(Error *error, Int row_id) = 0;

  // Unset the value.
  virtual void unset(Int row_id) = 0;

  // Find an index with its ID.
  //
  // On success, returns a pointer to the index.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *find_index_with_id(Error *error,
                            String name,
                            Int *column_id) const;

  friend class Table;
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
