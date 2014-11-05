#include "grnxx/impl/db.hpp"

namespace grnxx {
namespace impl {

DB::DB() : DBInterface(), tables_() {}

DB::~DB() {}

Table *DB::create_table(const String &name, const TableOptions &options) {
  if (find_table(name)) {
    throw "Table already exists";  // TODO
  }
  tables_.reserve(num_tables() + 1);
  std::unique_ptr<Table> new_table = Table::create(this, name, options);
  tables_.push_back(std::move(new_table));
  return tables_.back().get();
}

void DB::remove_table(const String &name) {
  size_t table_id;
  if (!find_table_with_id(name, &table_id)) {
    throw "Table not found";  // TODO
  }
  if (!tables_[table_id]->is_removable()) {
    throw "Table not removable";  // TODO
  }
  tables_.erase(table_id);
}

void DB::rename_table(const String &name, const String &new_name) {
  size_t table_id;
  if (!find_table_with_id(name, &table_id)) {
    throw "Table not found";  // TODO
  }
  if (name == new_name) {
    return;
  }
  if (find_table(new_name)) {
    throw "Table already exists";  // TODO
  }
  tables_[table_id]->rename(new_name);
}

void DB::reorder_table(const String &name, const String &prev_name) {
  size_t table_id;
  if (!find_table_with_id(name, &table_id)) {
    throw "Table not found";  // TODO
  }
  size_t new_table_id = 0;
  if (prev_name.size() != 0) {
    size_t prev_table_id;
    if (!find_table_with_id(prev_name, &prev_table_id)) {
      throw "Table not found";  // TODO
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
}

Table *DB::find_table(const String &name) const {
  for (size_t i = 0; i < num_tables(); ++i) {
    if (name == tables_[i]->name()) {
      return tables_[i].get();
    }
  }
  return nullptr;
}

void DB::save(const String &, const DBOptions &) const {
  throw "Not supported yet";  // TODO
}

Table *DB::find_table_with_id(const String &name, size_t *table_id) const {
  for (size_t i = 0; i < num_tables(); ++i) {
    if (name == tables_[i]->name()) {
      if (table_id) {
        *table_id = i;
      }
      return tables_[i].get();
    }
  }
  return nullptr;
}

}  // namespace impl
}  // namespace grnxx
