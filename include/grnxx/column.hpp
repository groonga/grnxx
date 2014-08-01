#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

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
  size_t num_indexes() const {
    // TODO: Index is not supported yet.
    return 0;
  }

  // Create an index with "name", "index_type", and "index_options".
  //
  // Returns a pointer to the index on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Index *create_index(
      Error *error,
      String name,
      IndexType index_type,
      const IndexOptions &index_options = IndexOptions());

  // Remove an index named "name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed index must not be used after deletion.
  bool remove_index(Error *error, String name);

  // Rename an index named "name" to "new_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_index(Error *error,
                    String name,
                    String new_name);

  // Change the order of indexes.
  //
  // Moves an index named "name" to the head if "prev_name" is
  // nullptr or an empty string.
  // Does nothing if "name" == "prev_name".
  // Moves an index named "name" to next to an index named
  // "prev_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reorder_index(Error *error,
                     String name,
                     String prev_name);

  // Get an index identified by "index_id".
  //
  // If "index_id" is invalid, the result is undefined.
  //
  // Returns a pointer to the index on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *get_index(size_t index_id) const {
    // TODO: Not supported yet.
    return nullptr;
  }

  // Find an index named "name".
  //
  // Returns a pointer to the index on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *find_index(Error *error, String name) const;

  // Set a value.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set(Error *error, Int row_id, const Datum &datum);

  // Get a value.
  //
  // Stores a value identified by "row_id" into "*datum".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool get(Error *error, Int row_id, Datum *datum) const;

  // Create a cursor to get records.
  //
  // Returns a pointer to the cursor on success.
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

  Column();

  // Initialize the base members.
  //
  // Returns true on success.
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
  // Returns a pointer to the column on success.
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
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, String new_name);

  // Return whether the column is removable or not.
  bool is_removable();

  // Set the initial key.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_initial_key(Error *error, Int row_id, const Datum &key);

  // Set the default value.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_default_value(Error *error, Int row_id) = 0;

  // Unset the value.
  virtual void unset(Int row_id) = 0;

  friend class Table;
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
