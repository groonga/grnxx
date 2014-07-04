#ifndef GRNXX_DB_HPP
#define GRNXX_DB_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

struct DBOptions {
};

class DB {
 public:
  ~DB();

  // Return the number of tables.
  size_t num_tables() const {
    return tables_.size();
  }

  // Create a table with "name" and "options".
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *create_table(Error *error,
                      String name,
                      const TableOptions &options);

  // Remove a table named "name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  //
  // Note: Pointers to the removed table must not be used after deletion.
  bool remove_table(Error *error, String name);

  // Rename a table named "name" to "new_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename_table(Error *error,
                    String name,
                    String new_name);

  // Change the order of tables.
  //
  // Moves a table named "name" to the head if "prev_name" is nullptr or an
  // empty string.
  // Does nothing if "name" == "prev_name".
  // Moves a table named "name" to next to a table named "prev_name".
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool reorder_table(Error *error,
                     String name,
                     String prev_name);

  // Get a table identified by "table_id".
  //
  // If "table_id" is invalid, the result is undefined.
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *get_table(size_t table_id) const {
    return tables_[table_id].get();
  }

  // Find a table named "name".
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *find_table(Error *error, String name) const;

  // Save the database into a file.
  // If "path" is nullptr or an empty string, saves the database into its
  // associated file.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool save(Error *error,
            String path,
            const DBOptions &options) const;

 private:
  std::vector<unique_ptr<Table>> tables_;

  DB();

  // Find a table with its ID.
  //
  // Returns a pointer to the table on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *find_table_with_id(Error *error,
                            String name,
                            size_t *table_id) const;

  friend unique_ptr<DB> open_db(Error *error,
                                String path,
                                const DBOptions &options);
};

// Open or create a database.
//
// If "path" is nullptr or an empty string, creates a temporary database.
//
// Returns a pointer to the database on success.
// On failure, returns nullptr and stores error information into "*error" if
// "error" != nullptr.
unique_ptr<DB> open_db(Error *error,
                       String path,
                       const DBOptions &options);

// Remove a database identified by "path".
//
// Returns true on success.
// On failure, returns false and stores error information into "*error" if
// "error" != nullptr.
bool remove_db(Error *error, String path, const DBOptions &options);

}  // namespace grnxx

#endif  // GRNXX_DB_HPP
