#ifndef GRNXX_DB_HPP
#define GRNXX_DB_HPP

#include <memory>

#include "grnxx/string.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

struct DBOptions {
};

class DB {
 public:
  DB() = default;
  virtual ~DB() = default;

  // Return the number of tables.
  virtual size_t num_tables() const = 0;

  // Create a table.
  //
  // On success, returns a pointer to the table.
  // On failure, throws an exception.
  virtual Table *create_table(
      const String &name,
      const TableOptions &options = TableOptions()) = 0;

  // Remove a table named "name".
  //
  // On failure, throws an exception.
  virtual void remove_table(const String &name) = 0;

  // Rename a table named "name" to "new_name".
  //
  // On failure, throws an exception.
  virtual void rename_table(const String &name, const String &new_name) = 0;

  // Change the order of tables.
  //
  // If "prev_name" is an empty string, moves a table named "name" to the head.
  // If "name" == "prev_name", does nothing.
  // Otherwise, moves a table named "name" to next to a table named
  // "prev_name".
  //
  // On failure, throws an exception.
  virtual void reorder_table(const String &name, const String &prev_name) = 0;

  // Return the "i"-th table.
  //
  // If "i" >= "num_tables()", the behavior is undefined.
  virtual Table *get_table(size_t table_id) const = 0;

  // Find a table named "name".
  //
  // If found, returns a pointer to the table.
  // If not found, returns nullptr.
  virtual Table *find_table(const String &name) const = 0;

  // TODO: Not supported yet.
  //
  // Save the database into a file.
  //
  // If "path" is nullptr or an empty string, saves the database into its
  // associated file.
  //
  // On failure, throws an exception.
  virtual void save(const String &path,
                    const DBOptions &options = DBOptions()) const = 0;
};

// Open or create a database.
//
// If "path" is nullptr or an empty string, creates a temporary database.
//
// TODO: A named database is not supoprted yet.
//
// On success, returns a pointer to the database.
// On failure, throws an exception.
std::unique_ptr<DB> open_db(const String &path,
                            const DBOptions &options = DBOptions());

// TODO: Not supported yet.
//
// Remove a database.
//
// On failure, throws an exception.
void remove_db(const String &path, const DBOptions &options = DBOptions());

}  // namespace grnxx

#endif  // GRNXX_DB_HPP
