#ifndef GRNXX_TABLE_HPP
#define GRNXX_TABLE_HPP

#include "grnxx/datum.hpp"

namespace grnxx {

class Table {
 public:
  // テーブルを初期化する．
  Table(Database *database, TableID id, const String &name);
  // テーブルを破棄する．
  ~Table();

  // コピーと代入を禁止する．
  Table(const Table &) = delete;
  Table &operator=(const Table &) = delete;

  // 所属するデータベースを返す．
  Database *database() const {
    return database_;
  }
  // テーブル ID を返す．
  TableID id() const {
    return id_;
  }
  // テーブル名を返す．
  String name() const {
    return name_;
  }

  // 指定された名前とデータ型のカラムを作成して返す．
  // 失敗すると nullptr を返す．
  Column *create_column(const String &column_name, DataType data_type);
  // 参照型のカラムを作成して返す．
  // 失敗すると nullptr を返す．
  Column *create_reference_column(const String &column_name,
                                  const String &table_name);
  // 指定された名前のカラムを破棄する．
  // 成功すれば true を返し，失敗すれば false を返す．
  bool drop_column(const String &column_name);

  // カラム ID の最小値を返す．
  ColumnID min_column_id() const {
    return MIN_COLUMN_ID;
  }
  // カラム ID の最大値を返す．
  ColumnID max_column_id() const {
    return columns_.size() - 1;
  }

  // 指定された ID のカラムを返す．
  // なければ nullptr を返す．
  Column *get_column_by_id(ColumnID column_id) const {
    if (column_id > max_column_id()) {
      return nullptr;
    }
    return columns_[column_id].get();
  }
  // 指定された名前のカラムを返す．
  // なければ nullptr を返す．
  Column *get_column_by_name(const String &column_name) const;

  // カラムの一覧を columns の末尾に追加し，カラムの数を返す．
  Int64 get_columns(std::vector<Column *> *columns) const;

  // 指定された名前，対象カラム，種類の索引を作成して返す．
  // 失敗すると nullptr を返す．
  Index *create_index(const String &index_name, const String &column_name,
                      IndexType index_type);
  // 指定された名前の索引を破棄する．
  // 成功すれば true を返し，失敗すれば false を返す．
  bool drop_index(const String &index_name);

  // 索引 ID の最小値を返す．
  IndexID min_index_id() const {
    return MIN_INDEX_ID;
  }
  // 索引 ID の最大値を返す．
  IndexID max_index_id() const {
    return indexes_.size() - 1;
  }
  // 指定された ID の索引を返す．
  // なければ nullptr を返す．
  Index *get_index_by_id(IndexID index_id) const {
    if (index_id > max_index_id()) {
      return nullptr;
    }
    return indexes_[index_id].get();
  }
  // 指定された名前の索引を返す．
  // なければ nullptr を返す．
  Index *get_index_by_name(const String &index_name) const;

  // 索引の一覧を indexes の末尾に追加し，索引の数を返す．
  Int64 get_indexes(std::vector<Index *> *indexes) const;

  // 行を追加して，その行 ID を返す．
  RowID insert_row();

  // 行 ID の最小値を返す．
  RowID min_row_id() const {
    return MIN_ROW_ID;
  }
  // 行 ID の最大値を返す．
  RowID max_row_id() const {
    return max_row_id_;
  }

  // 行 ID を取得するカーソル．
  class Cursor : public RowIDCursor {
   public:
    // 指定されたテーブルの範囲内にある行 ID を取得するようにカーソルを初期化する．
    Cursor(RowID range_min, RowID range_max);

    // 行 ID を最大 size 個取得して buf に格納し，取得した行 ID の数を返す．
    // buf が nullptr のときは取得した行 ID をそのまま捨てる．
    Int64 get_next(RowID *buf, Int64 size);

   private:
    RowID row_id_;
    RowID max_row_id_;
  };

  // 指定された範囲の行 ID を取得するカーソルを作成して返す．
  // 範囲が省略された，もしくは 0 を指定されたときは，
  // 行 ID の最小値・最大値を範囲として使用する．
  Cursor *create_cursor(RowID range_min = MIN_ROW_ID,
                        RowID range_max = INT64_MAX) const;

  // 演算器を作成する．
  Calc *create_calc(const String &query) const;
  // 整列器を作成する．
  Sorter *create_sorter(const String &query) const;

  // 整列済みの行一覧を受け取り，グループの境界を求める．
  bool group_by(const RowID *row_ids, Int64 num_row_ids,
                const String &comma_separated_column_names,
                std::vector<Int64> *boundaries);

  // ストリームに書き出す．
  std::ostream &write_to(std::ostream &stream) const;

  // 行の一覧を文字列化する．
  std::ostream &write_to(std::ostream &stream,
                         const RowID *row_ids, Int64 num_row_ids,
                         const String &comma_separated_column_names);
  // 行の一覧をグループ毎に文字列化する．
  std::ostream &write_to(std::ostream &stream,
                         const RowID *row_ids, Int64 num_row_ids,
                         const std::vector<Int64> &boundaries,
                         const String &comma_separated_column_names);

 private:
  Database *database_;
  TableID id_;
  std::string name_;
  RowID max_row_id_;
  std::vector<std::unique_ptr<Column>> columns_;
  std::map<String, ColumnID> columns_map_;
  std::vector<std::unique_ptr<Index>> indexes_;
  std::map<String, IndexID> indexes_map_;
};

std::ostream &operator<<(std::ostream &stream, const Table &table);

}  // namespace grnxx

#endif  // GRNXX_TABLE_HPP
