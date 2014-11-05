#ifndef GRNXX_IMPL_DB_HPP
#define GRNXX_IMPL_DB_HPP

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/string.hpp"

namespace grnxx {
namespace impl {

using DBInterface = grnxx::DB;

class DB : public DBInterface {
 public:
  // -- Public API (grnxx/db.hpp) --

  DB();
  ~DB();

  size_t num_tables() const {
    return tables_.size();
  }

  Table *create_table(const String &name, const TableOptions &options);
  void remove_table(const String &name);
  void rename_table(const String &name, const String &new_name);
  void reorder_table(const String &name, const String &prev_name);

  Table *get_table(size_t i) const {
    return tables_[i].get();
  }
  Table *find_table(const String &name) const;

  void save(const String &path, const DBOptions &options = DBOptions()) const;

 private:
  Array<std::unique_ptr<Table>> tables_;

  // Find a table with its ID.
  //
  // If found, returns the table and stores its ID into "*table_id".
  // If not found, returns nullptr.
  Table *find_table_with_id(const String &name, size_t *table_id) const;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_DB_HPP
