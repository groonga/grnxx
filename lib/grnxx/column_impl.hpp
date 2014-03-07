#ifndef GRNXX_COLUMN_IMPL_HPP
#define GRNXX_COLUMN_IMPL_HPP

#include "../config.h"
#include "grnxx/column.hpp"

namespace grnxx {

template <typename T>
class ColumnImpl : public Column {
 public:
  using Value = T;

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

#ifdef GRNXX_ENABLE_VARIABLE_INTEGER_TYPE
template <>
class ColumnImpl<Int64> : public Column {
 public:
  using Value = Int64;

  // カラムを初期化する．
  ColumnImpl(Table *table, ColumnID id, const String &name,
             Table *dest_table = nullptr);
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

  // 参照先のテーブルを返す．
  // なければ nullptr を返す．
  Table *dest_table() const {
    return dest_table_;
  }

  // 指定された ID の値を返す．
  Int64 get(RowID row_id) const {
    switch (internal_data_type_size_) {
      case 8: {
        return data_8_[row_id];
      }
      case 16: {
        return data_16_[row_id];
      }
      case 32: {
        return data_32_[row_id];
      }
      default: {
        return data_64_[row_id];
      }
    }
  }
  // 指定された ID の値を更新する．
  void set(RowID row_id, Int64 value);

 private:
  Table *dest_table_;
  std::vector<Int8> data_8_;
  std::vector<Int16> data_16_;
  std::vector<Int32> data_32_;
  std::vector<Int64> data_64_;
  Int64 internal_data_type_size_;
  std::vector<Index *> indexes_;

  // 渡された値を格納できるように拡張をおこなってから値を格納する．
  void expand_and_set(RowID row_id, Int64 value);
};
#else  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE
template <>
class ColumnImpl<Int64> : public Column {
 public:
  using Value = Int64;

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
  Int64 get(RowID row_id) const {
    return data_[row_id];
  }
  // 指定された ID の値を更新する．
  void set(RowID row_id, Int64 value);

 private:
  std::vector<Int64> data_;
  std::vector<Index *> indexes_;
};
#endif  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE

template <>
class ColumnImpl<String> : public Column {
 public:
  using Value = String;

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
