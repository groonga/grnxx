#ifndef GRNXX_COLUMN_IMPL_HPP
#define GRNXX_COLUMN_IMPL_HPP

#include "grnxx/column.hpp"

namespace grnxx {

template <typename T>
class ColumnImpl : public Column {
 public:
  // カラムを初期化する．
  ColumnImpl(Table *table, ColumnID id, const String &name);
  // カラムを破棄する．
  ~ColumnImpl();

  // コピーと代入を禁止する．
  ColumnImpl(const ColumnImpl &) = delete;
  ColumnImpl &operator=(const ColumnImpl &) = delete;

  // 指定された索引との関連付けをおこなう．
  bool register_index(Index *index);
  // 指定された索引との関連を削除する．
  bool unregister_index(Index *index);

  // 指定された行 ID が使えるようにサイズを変更する．
  void resize(RowID max_row_id);

  // 指定された ID の値を返す．
  T get(RowID row_id) const {
    return data_[row_id];
  }
  // 指定された ID の値を更新する．
  void set(RowID row_id, T value);

 private:
  std::vector<T> data_;
  std::vector<Index *> indexes_;
};

template <>
class ColumnImpl<String> : public Column {
 public:
  // カラムを初期化する．
  ColumnImpl(Table *table, ColumnID id, const String &name);
  // カラムを破棄する．
  ~ColumnImpl();

  // コピーと代入を禁止する．
  ColumnImpl(const ColumnImpl &) = delete;
  ColumnImpl &operator=(const ColumnImpl &) = delete;

  // 指定された索引との関連付けをおこなう．
  bool register_index(Index *index);
  // 指定された索引との関連を削除する．
  bool unregister_index(Index *index);

  // 指定された行 ID が使えるようにサイズを変更する．
  void resize(RowID max_row_id);

  // 指定された ID の値を返す．
  String get(RowID row_id) const {
    Int64 size = headers_[row_id] & 0xFFFF;
    if (size == 0) {
      return String("", 0);
    }
    Int64 offset = headers_[row_id] >> 16;
    if (size < 0xFFFF) {
      return String(&bodies_[offset], size);
    } else {
      // 長い文字列については offset の位置にサイズが保存してある．
      size = *reinterpret_cast<const Int64 *>(&bodies_[offset]);
      return String(&bodies_[offset + sizeof(Int64)], size);
    }
  }
  // 指定された ID の値を更新する．
  void set(RowID row_id, const String &value);

 private:
  std::vector<UInt64> headers_;
  std::vector<char> bodies_;
  std::vector<Index *> indexes_;
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_IMPL_HPP
