#ifndef GRNXX_COLUMN_HPP
#define GRNXX_COLUMN_HPP

#include "grnxx/datum.hpp"

namespace grnxx {

class Column {
 public:
  // カラムを初期化する．
  Column(Table *table, ColumnID id, const String &name, DataType data_type);
  // カラムを破棄する．
  virtual ~Column();

  // 所属するテーブルを返す．
  Table *table() const {
    return table_;
  }
  // カラム ID を返す．
  ColumnID id() const {
    return id_;
  }
  // カラム名を返す．
  String name() const {
    return name_;
  }
  // データ型を返す．
  DataType data_type() const {
    return data_type_;
  }
  // UNIQUE 制約の有無を返す．
  bool is_unique() const {
    return is_unique_;
  }

  // UNIQUE 制約を設定する．
  virtual bool set_unique() = 0;
  // UNIQUE 制約を解除する．
  virtual bool unset_unique();

  // 指定された索引との関連付けをおこなう．
  virtual bool register_index(Index *index) = 0;
  // 指定された索引との関連を削除する．
  virtual bool unregister_index(Index *index) = 0;

  // 指定された行 ID が使えるようにサイズを変更する．
  virtual void resize(RowID max_row_id) = 0;

  // FIXME: 指定された値を検索する．
  virtual RowID generic_find(const Datum &datum) const;
  // FIXME: 指定された ID の値を返す．
  virtual Datum generic_get(RowID row_id) const;
  // FIXME: 指定された ID の値を設定する．
  virtual void generic_set(RowID row_id, const Datum &datum);

  // ストリームに書き出す．
  virtual std::ostream &write_to(std::ostream &stream) const;

 protected:
  Table *table_;
  ColumnID id_;
  std::string name_;
  DataType data_type_;
  bool is_unique_;
};

std::ostream &operator<<(std::ostream &stream, const Column &column);

class ColumnHelper {
 public:
  // 指定されたカラムを作成して返す．
  static Column *create_column(Table *table,
                               ColumnID column_id,
                               const String &column_name,
                               DataType data_type);
  // 指定された参照型のカラムを作成して返す．
  static Column *create_reference_column(Table *table,
                                         ColumnID column_id,
                                         const String &column_name,
                                         Table *dest_table);
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_HPP
