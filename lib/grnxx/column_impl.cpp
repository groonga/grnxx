#include "grnxx/column_impl.hpp"

#include "grnxx/index.hpp"

namespace grnxx {

// カラムを初期化する．
template <typename T>
ColumnImpl<T>::ColumnImpl(Table *table, ColumnID id, const String &name)
    : Column(table, id, name, TypeTraits<T>::data_type()),
      data_(MIN_ROW_ID, 0),
      indexes_() {}

// カラムを破棄する．
template <typename T>
ColumnImpl<T>::~ColumnImpl() {}

// 指定された索引との関連付けをおこなう．
template <typename T>
bool ColumnImpl<T>::register_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it != indexes_.end()) {
    return false;
  }
  indexes_.push_back(index);
  return true;
}

// 指定された索引との関連を削除する．
template <typename T>
bool ColumnImpl<T>::unregister_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it == indexes_.end()) {
    return false;
  }
  indexes_.erase(it);
  return true;
}

// 指定された行 ID が使えるようにサイズを変更する．
template <typename T>
void ColumnImpl<T>::resize(RowID max_row_id) {
  data_.resize(max_row_id + 1, 0);
}

// 指定された ID の値を更新する．
template <typename T>
void ColumnImpl<T>::set(RowID row_id, T value) {
  data_[row_id] = value;
  for (auto index : indexes_) {
    index->insert(row_id);
  }
}

template class ColumnImpl<Boolean>;
template class ColumnImpl<Int64>;
template class ColumnImpl<Float>;

// カラムを初期化する．
ColumnImpl<String>::ColumnImpl(Table *table, ColumnID id, const String &name)
    : Column(table, id, name, TypeTraits<String>::data_type()),
      headers_(MIN_ROW_ID, 0),
      bodies_(),
      indexes_() {}

// カラムを破棄する．
ColumnImpl<String>::~ColumnImpl() {}

// 指定された索引との関連付けをおこなう．
bool ColumnImpl<String>::register_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it != indexes_.end()) {
    return false;
  }
  indexes_.push_back(index);
  return true;
}

// 指定された索引との関連を削除する．
bool ColumnImpl<String>::unregister_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it == indexes_.end()) {
    return false;
  }
  indexes_.erase(it);
  return true;
}

// 指定された行 ID が使えるようにサイズを変更する．
void ColumnImpl<String>::resize(RowID max_row_id) {
  headers_.resize(max_row_id + 1, 0);
}

// 指定された ID の値を更新する．
void ColumnImpl<String>::set(RowID row_id, const String &value) {
  if (value.empty()) {
    headers_[row_id] = 0;
    return;
  }
  Int64 offset = bodies_.size();
  if (value.size() < 0xFFFF) {
    bodies_.resize(offset + value.size());
    std::memcpy(&bodies_[offset], value.data(), value.size());
    headers_[row_id] = (offset << 16) | value.size();
  } else {
    // 長い文字列については offset の位置にサイズを保存する．
    if ((offset % sizeof(Int64)) != 0) {
      offset += sizeof(Int64) - (offset % sizeof(Int64));
    }
    bodies_.resize(offset + sizeof(Int64) + value.size());
    *reinterpret_cast<Int64 *>(&bodies_[offset]) = value.size();
    std::memcpy(&bodies_[offset + sizeof(Int64)], value.data(), value.size());
    headers_[row_id] = (offset << 16) | 0xFFFF;
  }
  for (auto index : indexes_) {
    index->insert(row_id);
  }
}

}  // namespace grnxx
