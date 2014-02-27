#ifndef GRNXX_DATABASE_HPP
#define GRNXX_DATABASE_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Database {
 public:
  // データベースを初期化する．
  Database();
  // データベースを破棄する．
  ~Database();

  // コピーと代入を禁止する．
  Database(const Database &) = delete;
  Database &operator=(const Database &) = delete;

  // 指定された名前のテーブルを作成して返す．
  // 失敗すると nullptr を返す．
  Table *create_table(const String &table_name);
  // 指定された名前のテーブルを破棄する．
  // 成功すれば true を返し，失敗すれば false を返す．
  bool drop_table(const String &table_name);

  // テーブル ID の最小値を返す．
  TableID min_table_id() const {
    return MIN_TABLE_ID;
  }
  // テーブル ID の最大値を返す．
  TableID max_table_id() const {
    return tables_.size() - 1;
  }

  // 指定された ID のテーブルを返す．
  // なければ nullptr を返す．
  Table *get_table_by_id(TableID table_id) const {
    if (table_id > max_table_id()) {
      return nullptr;
    }
    return tables_[table_id].get();
  }
  // 指定された名前のテーブルを返す．
  // なければ nullptr を返す．
  Table *get_table_by_name(const String &table_name) const;

  // テーブルの一覧を tables の末尾に追加し，テーブルの数を返す．
  Int64 get_tables(std::vector<Table *> *tables) const;

  // ストリームに書き出す．
  std::ostream &write_to(std::ostream &stream) const;

 private:
  std::vector<std::unique_ptr<Table>> tables_;
  std::map<String, TableID> tables_map_;
};

std::ostream &operator<<(std::ostream &stream, const Database &database);

}  // namespace grnxx

#endif  // GRNXX_DATABASE_HPP
