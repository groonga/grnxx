#include "grnxx/db.hpp"

#include "grnxx/error.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

DB::~DB() {}

Table *DB::create_table(Error *error,
                        String name,
                        const TableOptions &options) {
  if (find_table(nullptr, name)) {
    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
                    "Table already exists: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return nullptr;
  }
  try {
    tables_.reserve(tables_.size() + 1);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  unique_ptr<Table> new_table =
      Table::create(error, this, name, options);
  if (!new_table) {
    return nullptr;
  }
  tables_.push_back(std::move(new_table));
  return tables_.back().get();
}

bool DB::remove_table(Error *error, String name) {
  size_t table_id;
  if (!find_table_with_id(error, name, &table_id)) {
    return false;
  }
  if (!tables_[table_id]->is_removable()) {
    GRNXX_ERROR_SET(error, NOT_REMOVABLE,
                    "Table is not removable: name = \"%.*s\"",
                    static_cast<int>(name.size()), name.data());
    return false;
  }
  tables_.erase(tables_.begin() + table_id);
  return true;
}

bool DB::rename_table(Error *error,
                      String name,
                      String new_name) {
  size_t table_id;
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
                       String name,
                       String prev_name) {
  size_t table_id;
  if (!find_table_with_id(error, name, &table_id)) {
    return false;
  }
  size_t new_table_id = 0;
  if (prev_name.size() != 0) {
    size_t prev_table_id;
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

Table *DB::find_table(Error *error, String name) const {
  for (size_t table_id = 0; table_id < num_tables(); ++table_id) {
    if (name == tables_[table_id]->name()) {
      return tables_[table_id].get();
    }
  }
  GRNXX_ERROR_SET(error, NOT_FOUND, "Table not found: name = \"%.*s\"",
                  static_cast<int>(name.size()), name.data());
  return nullptr;
}

bool DB::save(Error *error,
              String path,
              const DBOptions &options) const {
  // TODO: Named DB is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

DB::DB() : tables_() {}

Table *DB::find_table_with_id(Error *error,
                              String name,
                              size_t *table_id) const {
  for (size_t i = 0; i < num_tables(); ++i) {
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

unique_ptr<DB> open_db(Error *error,
                       String path,
                       const DBOptions &options) {
  if (path.size() != 0) {
    // TODO: Named DB is not supported yet.
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
    return nullptr;
  }
  std::unique_ptr<DB> db(new (nothrow) DB);
  if (!db) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return db;
}

bool remove_db(Error *error, String path, const DBOptions &options) {
  // TODO: Named DB is not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

}  // namespace grnxx
