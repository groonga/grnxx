#include "grnxx/impl/db.hpp"

namespace grnxx {
namespace impl {

DB::DB() : grnxx::DB(), tables_() {}

DB::~DB() {}

Int DB::num_tables() const {
  return tables_.size();
}

Table *DB::create_table(Error *error,
                        const StringCRef &name,
                        const TableOptions &options) {
  if (find_table(nullptr, name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Table already exists: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return nullptr;
  }
  if (!tables_.reserve(error, num_tables() + 1)) {
    return nullptr;
  }
  unique_ptr<Table> new_table = Table::create(error, this, name, options);
  if (!new_table) {
    return nullptr;
  }
  tables_.push_back(error, std::move(new_table));
  return tables_.back().get();
}

bool DB::remove_table(Error *error, const StringCRef &name) {
  Int table_id;
  if (!find_table_with_id(error, name, &table_id)) {
    return false;
  }
  if (!tables_[table_id]->is_removable()) {
    GRNXX_ERROR_SET(error, NOT_REMOVABLE,
                    "Table is not removable: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return false;
  }
  tables_.erase(table_id);
  return true;
}

bool DB::rename_table(Error *error,
                      const StringCRef &name,
                      const StringCRef &new_name) {
  Int table_id;
  if (!find_table_with_id(error, name, &table_id)) {
    return false;
  }
  if (name == new_name) {
    return true;
  }
  if (find_table(nullptr, new_name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Table already exists: new_name = \"%.*s\"",
                    static_cast<int>(new_name.size()), new_name.data());
    return false;
  }
  return tables_[table_id]->rename(error, new_name);
}

bool DB::reorder_table(Error *error,
                       const StringCRef &name,
                       const StringCRef &prev_name) {
  Int table_id;
  if (!find_table_with_id(error, name, &table_id)) {
    return false;
  }
  Int new_table_id = 0;
  if (prev_name.size() != 0) {
    Int prev_table_id;
    if (!find_table_with_id(error, prev_name, &prev_table_id)) {
      return false;
    }
    if (table_id <= prev_table_id) {
      new_table_id = prev_table_id;
    } else {
      new_table_id = prev_table_id + 1;
    }
  }
  for ( ; table_id < new_table_id; ++table_id) {
    std::swap(tables_[table_id], tables_[table_id + 1]);
  }
  for ( ; table_id > new_table_id; --table_id) {
    std::swap(tables_[table_id], tables_[table_id - 1]);
  }
  return true;
}

Table *DB::get_table(Int table_id) const {
  return tables_[table_id].get();
}

Table *DB::find_table(Error *error, const StringCRef &name) const {
  for (Int table_id = 0; table_id < num_tables(); ++table_id) {
    if (name == tables_[table_id]->name()) {
      return tables_[table_id].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Table not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

bool DB::save(Error *error,
              const StringCRef &,
              const DBOptions &) const {
  // TODO: Named DB is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

Table *DB::find_table_with_id(Error *error,
                              const StringCRef &name,
                              Int *table_id) const {
  for (Int i = 0; i < num_tables(); ++i) {
    if (name == tables_[i]->name()) {
      if (table_id != nullptr) {
        *table_id = i;
      }
      return tables_[i].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Table not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

}  // namespace impl
}  // namespace grnxx
