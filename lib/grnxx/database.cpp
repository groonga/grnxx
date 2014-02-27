#include "grnxx/database.hpp"

#include "grnxx/table.hpp"

#include <ostream>

namespace grnxx {

// データベースを初期化する．
Database::Database()
    : tables_(MIN_TABLE_ID),
      tables_map_() {}

// データベースを破棄する．
Database::~Database() {}

// 指定された名前のテーブルを作成して返す．
// 失敗すると nullptr を返す．
Table *Database::create_table(const String &table_name) {
  auto it = tables_map_.find(table_name);
  if (it != tables_map_.end()) {
    return nullptr;
  }
  TableID table_id = min_table_id();
  for ( ; table_id <= max_table_id(); ++table_id) {
    if (!tables_[table_id]) {
      break;
    }
  }
  if (table_id > max_table_id()) {
    tables_.resize(table_id + 1);
  }
  std::unique_ptr<Table> new_table(new Table(this, table_id, table_name));
  tables_map_.insert(it, std::make_pair(new_table->name(), table_id));
  tables_[table_id] = std::move(new_table);
  return tables_[table_id].get();
}

// 指定された名前のテーブルを破棄する．
// 成功すれば true を返し，失敗すれば false を返す．
bool Database::drop_table(const String &table_name) {
  auto it = tables_map_.find(table_name);
  if (it == tables_map_.end()) {
    return false;
  }
  tables_[it->second].reset();
  tables_map_.erase(it);
  return true;
}

// 指定された名前のテーブルを返す．
// なければ nullptr を返す．
Table *Database::get_table_by_name(const String &table_name) const {
  auto it = tables_map_.find(table_name);
  if (it == tables_map_.end()) {
    return nullptr;
  }
  return tables_[it->second].get();
}

// テーブルの一覧を tables の末尾に追加し，テーブルの数を返す．
Int64 Database::get_tables(std::vector<Table *> *tables) const {
  std::size_t old_size = tables->size();
  for (auto &table : tables_) {
    if (table) {
      tables->push_back(table.get());
    }
  }
  return tables->size() - old_size;
}

// ストリームに書き出す．
std::ostream &Database::write_to(std::ostream &stream) const {
  std::vector<Table *> tables;
  if (get_tables(&tables) == 0) {
    return stream << "{}";
  }
  stream << "{ " << *tables[0];
  for (std::size_t i = 1; i < tables.size(); ++i) {
    stream << ", " << *tables[i];
  }
  stream << " }";
  return stream;
}

std::ostream &operator<<(std::ostream &stream, const Database &database) {
  return database.write_to(stream);
}

}  // namespace grnxx
