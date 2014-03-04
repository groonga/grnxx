#include "grnxx/column_impl.hpp"

#include "grnxx/index.hpp"

//#include <iostream>

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
template class ColumnImpl<Float>;

#ifdef GRNXX_ENABLE_VARIABLE_INTEGER_TYPE

// カラムを初期化する．
ColumnImpl<Int64>::ColumnImpl(Table *table, ColumnID id, const String &name)
    : Column(table, id, name, INTEGER),
      data_8_(MIN_ROW_ID, 0),
      data_16_(),
      data_32_(),
      data_64_(),
      internal_data_type_size_(8),
      indexes_() {}

// カラムを破棄する．
ColumnImpl<Int64>::~ColumnImpl() {}

// 指定された索引との関連付けをおこなう．
bool ColumnImpl<Int64>::register_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it != indexes_.end()) {
    return false;
  }
  indexes_.push_back(index);
  return true;
}

// 指定された索引との関連を削除する．
bool ColumnImpl<Int64>::unregister_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it == indexes_.end()) {
    return false;
  }
  indexes_.erase(it);
  return true;
}

// 指定された行 ID が使えるようにサイズを変更する．
void ColumnImpl<Int64>::resize(RowID max_row_id) {
  switch (internal_data_type_size_) {
    case 8: {
      return data_8_.resize(max_row_id + 1, 0);
    }
    case 16: {
      return data_16_.resize(max_row_id + 1, 0);
    }
    case 32: {
      return data_32_.resize(max_row_id + 1, 0);
    }
    default: {
      return data_64_.resize(max_row_id + 1, 0);
    }
  }
}

// 指定された ID の値を更新する．
void ColumnImpl<Int64>::set(RowID row_id, Int64 value) {
  switch (internal_data_type_size_) {
    case 8: {
      if ((value < INT8_MIN) || (value > INT8_MAX)) {
        expand_and_set(row_id, value);
      } else {
        data_8_[row_id] = value;
      }
      break;
    }
    case 16: {
      if ((value < INT16_MIN) || (value > INT16_MAX)) {
        expand_and_set(row_id, value);
      } else {
        data_16_[row_id] = value;
      }
      break;
    }
    case 32: {
      if ((value < INT32_MIN) || (value > INT32_MAX)) {
        expand_and_set(row_id, value);
      } else {
        data_32_[row_id] = value;
      }
      break;
    }
    default: {
      data_64_[row_id] = value;
      break;
    }
  }
  for (auto index : indexes_) {
    index->insert(row_id);
  }
}

// 渡された値を格納できるように拡張をおこなってから値を格納する．
void ColumnImpl<Int64>::expand_and_set(RowID row_id, Int64 value) {
  switch (internal_data_type_size_) {
    case 8: {
      if ((value < INT32_MIN) || (value > INT32_MAX)) {
        // Int8 -> Int64.
//        std::cerr << "Log: " << name() << ": Int8 -> Int64" << std::endl;
        data_64_.reserve(data_8_.capacity());
        data_64_.assign(data_8_.begin(), data_8_.end());
        data_64_[row_id] = value;
        internal_data_type_size_ = 64;
      } else if ((value < INT16_MIN) || (value > INT16_MAX)) {
        // Int8 -> Int32.
//        std::cerr << "Log: " << name() << ": Int8 -> Int32" << std::endl;
        data_32_.reserve(data_8_.capacity());
        data_32_.assign(data_8_.begin(), data_8_.end());
        data_32_[row_id] = value;
        internal_data_type_size_ = 32;
      } else {
        // Int8 -> Int16.
//        std::cerr << "Log: " << name() << ": Int8 -> Int16" << std::endl;
        data_16_.reserve(data_8_.capacity());
        data_16_.assign(data_8_.begin(), data_8_.end());
        data_16_[row_id] = value;
        internal_data_type_size_ = 16;
      }
      std::vector<Int8>().swap(data_8_);
      break;
    }
    case 16: {
      if ((value < INT32_MIN) || (value > INT32_MAX)) {
        // Int16 -> Int64.
//        std::cerr << "Log: " << name() << ": Int16 -> Int64" << std::endl;
        data_64_.reserve(data_16_.capacity());
        data_64_.assign(data_16_.begin(), data_16_.end());
        data_64_[row_id] = value;
        internal_data_type_size_ = 64;
      } else if ((value < INT16_MIN) || (value > INT16_MAX)) {
        // Int16 -> Int32.
//        std::cerr << "Log: " << name() << ": Int16 -> Int32" << std::endl;
        data_32_.reserve(data_16_.capacity());
        data_32_.assign(data_16_.begin(), data_16_.end());
        data_32_[row_id] = value;
        internal_data_type_size_ = 32;
      }
      std::vector<Int16>().swap(data_16_);
      break;
    }
    case 32: {
      // Int32 -> Int64.
//      std::cerr << "Log: " << name() << ": Int32-> Int64" << std::endl;
      data_64_.reserve(data_32_.capacity());
      data_64_.assign(data_32_.begin(), data_32_.end());
      data_64_[row_id] = value;
      internal_data_type_size_ = 64;
      std::vector<Int32>().swap(data_32_);
      break;
    }
  }
}

#else  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE

// カラムを初期化する．
ColumnImpl<Int64>::ColumnImpl(Table *table, ColumnID id, const String &name)
    : Column(table, id, name, INTEGER),
      data_(MIN_ROW_ID, 0),
      indexes_() {}

// カラムを破棄する．
ColumnImpl<Int64>::~ColumnImpl() {}

// 指定された索引との関連付けをおこなう．
bool ColumnImpl<Int64>::register_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it != indexes_.end()) {
    return false;
  }
  indexes_.push_back(index);
  return true;
}

// 指定された索引との関連を削除する．
bool ColumnImpl<Int64>::unregister_index(Index *index) {
  auto it = std::find(indexes_.begin(), indexes_.end(), index);
  if (it == indexes_.end()) {
    return false;
  }
  indexes_.erase(it);
  return true;
}

// 指定された行 ID が使えるようにサイズを変更する．
void ColumnImpl<Int64>::resize(RowID max_row_id) {
  data_.resize(max_row_id + 1, 0);
}

// 指定された ID の値を更新する．
void ColumnImpl<Int64>::set(RowID row_id, Int64 value) {
  data_[row_id] = value;
  for (auto index : indexes_) {
    index->insert(row_id);
  }
}

#endif  // GRNXX_ENABLE_VARIABLE_INTEGER_TYPE

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
