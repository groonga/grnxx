#ifndef GRNXX_INDEX_HPP
#define GRNXX_INDEX_HPP

#include "grnxx/datum.hpp"

namespace grnxx {

class Index {
 public:
  // 索引を初期化する．
  Index(IndexID id, const String &name, Column *column, IndexType type);
  // 索引を破棄する．
  virtual ~Index();

  // 索引の ID を返す．
  IndexID id() const {
    return id_;
  }
  // 索引の名前を返す．
  String name() const {
    return name_;
  }
  // 対象カラムを返す．
  Column *column() const {
    return column_;
  }
  // 索引の種類を返す．
  IndexType type() const {
    return type_;
  }

  // 指定されたデータを登録する．
  virtual void insert(RowID row_id) = 0;
  // 指定されたデータを削除する．
  virtual void remove(RowID row_id) = 0;

  // 行の一覧を取得できるカーソルを作成して返す．
  virtual RowIDCursor *find_all(bool reverse_order = false) = 0;
  // 指定された値が格納された行の一覧を取得できるカーソルを作成して返す．
  virtual RowIDCursor *find_equal(const Datum &datum,
                                  bool reverse_order = false) = 0;
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  virtual RowIDCursor *find_less(const Datum &datum,
                                 bool less_equal = false,
                                 bool reverse_order = false) = 0;
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  virtual RowIDCursor *find_greater(const Datum &datum,
                                    bool greater_equal = false,
                                    bool reverse_order = false) = 0;
  // 指定された範囲の値が格納された行の一覧を取得できるカーソルを作成して返す．
  virtual RowIDCursor *find_between(const Datum &begin, const Datum &end,
                                    bool greater_equal = false,
                                    bool less_equal = false,
                                    bool reverse_order = false) = 0;

 protected:
  IndexID id_;
  std::string name_;
  Column *column_;
  IndexType type_;
};

class IndexHelper {
 public:
  // 指定された ID, 名前，対処カラム，種類の索引を作成して返す．
  static Index *create(IndexID index_id,
                       const String &index_name,
                       Column *column,
                       IndexType index_type);
};

}  // namespace grnxx

#endif  // GRNXX_INDEX_HPP

