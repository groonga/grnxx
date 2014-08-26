#ifndef GRNXX_DB_HPP
#define GRNXX_DB_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

class DB {
 public:
  ~DB();

  // Return the number of tables.
  Int num_tables() const {
    return tables_.size();
  }

  // Create a table with "name" and "options".
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *create_table(Error *error,
                      String name,
                      const TableOptions &options = TableOptions());

  // Remove a table named "name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed table must not be used after deletion.
  bool remove_table(Error *error, String name);

  // Rename a table named "name" to "new_name".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_table(Error *error,
                    String name,
                    String new_name);

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
  bool reorder_table(Error *error,
                     String name,
                     String prev_name);

  // Get a table identified by "table_id".
  //
  // If "table_id" is invalid, the result is undefined.
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *get_table(Int table_id) const {
    return tables_[table_id].get();
  }

  // Find a table named "name".
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *find_table(Error *error, String name) const;

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
  bool save(Error *error,
            String path,
            const DBOptions &options = DBOptions()) const;

 private:
  Array<unique_ptr<Table>> tables_;

  DB();

  // Find a table with its ID.
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *find_table_with_id(Error *error,
                            String name,
                            Int *table_id) const;

  friend unique_ptr<DB> open_db(Error *error,
                                String path,
                                const DBOptions &options);
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
                       String path,
                       const DBOptions &options = DBOptions());

// TODO: Not supported yet.
//
// Remove a database identified by "path".
//
// On success, returns true.
// On failure, returns false and stores error information into "*error" if
// "error" != nullptr.
bool remove_db(Error *error,
               String path,
               const DBOptions &options = DBOptions());

}  // namespace grnxx

#endif  // GRNXX_DB_HPP
