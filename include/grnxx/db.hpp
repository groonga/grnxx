#ifndef GRNXX_DB_HPP
#define GRNXX_DB_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class DB {
 public:
  DB() = default;
  virtual ~DB() = default;

  // Return the number of tables.
  virtual Int num_tables() const = 0;

  // Create a table with "name" and "options".
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Table *create_table(
      Error *error,
      const StringCRef &name,
      const TableOptions &options = TableOptions()) = 0;

  // Remove a table named "name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed table must not be used after deletion.
  virtual bool remove_table(Error *error, const StringCRef &name) = 0;

  // Rename a table named "name" to "new_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool rename_table(Error *error,
                            const StringCRef &name,
                            const StringCRef &new_name) = 0;

  // Change the order of tables.
  //
  // If "prev_name" is an empty string, moves a table named "name" to the head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves a table named "name" to next to a table named
  // "prev_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool reorder_table(Error *error,
                             const StringCRef &name,
                             const StringCRef &prev_name) = 0;

  // Get a table identified by "table_id".
  //
  // If "table_id" is invalid, the result is undefined.
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Table *get_table(Int table_id) const = 0;

  // Find a table named "name".
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual Table *find_table(Error *error, const StringCRef &name) const = 0;

  // TODO: Not supported yet.
  //
  // Save the database into a file.
  //
  // If "path" is nullptr or an empty string, saves the database into its
  // associated file.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool save(Error *error,
                    const StringCRef &path,
                    const DBOptions &options = DBOptions()) const = 0;
};

// Open or create a database.
//
// If "path" is nullptr or an empty string, creates a temporary database.
//
// TODO: A named database is not supoprted yet.
//
// On success, returns a pointer to the database.
// On failure, returns nullptr and stores error information into "*error" if
// "error" != nullptr.
unique_ptr<DB> open_db(Error *error,
                       const StringCRef &path,
                       const DBOptions &options = DBOptions());

// TODO: Not supported yet.
//
// Remove a database identified by "path".
//
// On success, returns true.
// On failure, returns false and stores error information into "*error" if
// "error" != nullptr.
bool remove_db(Error *error,
               const StringCRef &path,
               const DBOptions &options = DBOptions());

}  // namespace grnxx

#endif  // GRNXX_DB_HPP
