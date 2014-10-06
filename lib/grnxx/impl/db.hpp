#ifndef GRNXX_IMPL_DB_HPP
#define GRNXX_IMPL_DB_HPP

#include "grnxx/db.hpp"

namespace grnxx {
namespace impl {

class DB : public grnxx::DB {
 public:
  // Public API, see grnxx/db.hpp for details.
  DB();
  ~DB();

  Int num_tables() const {
    return tables_.size();
  }

  Table *create_table(Error *error,
                      const StringCRef &name,
                      const TableOptions &options);
  bool remove_table(Error *error, const StringCRef &name);
  bool rename_table(Error *error,
                    const StringCRef &name,
                    const StringCRef &new_name);
  bool reorder_table(Error *error,
                     const StringCRef &name,
                     const StringCRef &prev_name);
  Table *get_table(Int table_id) const {
    return tables_[table_id].get();
  }

  Table *find_table(Error *error, const StringCRef &name) const;

  bool save(Error *error,
            const StringCRef &path,
            const DBOptions &options = DBOptions()) const;

 private:
  Array<unique_ptr<Table>> tables_;

  // Find a table with its ID.
  //
  // On success, returns a pointer to the table.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Table *find_table_with_id(Error *error,
                            const StringCRef &name,
                            Int *table_id) const;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_DB_HPP
