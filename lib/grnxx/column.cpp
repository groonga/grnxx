#include "grnxx/column.hpp"

#include "grnxx/column_impl.hpp"

#include <ostream>

namespace grnxx {

// カラムを初期化する．
Column::Column(Table *table, ColumnID id, const String &name,
               DataType data_type)
    : table_(table),
      id_(id),
      name_(reinterpret_cast<const char *>(name.data()), name.size()),
      data_type_(data_type),
      is_unique_(false) {}

// カラムを破棄する．
Column::~Column() {}

// UNIQUE 制約を解除する．
bool Column::unset_unique() {
  if (!is_unique_) {
    return false;
  }
  is_unique_ = false;
  return true;
}

// FIXME: 指定された値を検索する．
RowID Column::generic_find(const Datum &datum) const {
  switch (data_type()) {
    case BOOLEAN: {
      auto impl = static_cast<const ColumnImpl<Boolean> *>(this);
      return impl->find(static_cast<Boolean>(datum));
    }
    case INTEGER: {
      auto impl = static_cast<const ColumnImpl<Int64> *>(this);
      return impl->find(static_cast<Int64>(datum));
    }
    case FLOAT: {
      auto impl = static_cast<const ColumnImpl<Float> *>(this);
      return impl->find(static_cast<Float>(datum));
    }
    case STRING: {
      auto impl = static_cast<const ColumnImpl<String> *>(this);
      return impl->find(static_cast<std::string>(datum));
    }
  }
  return false;
}

// FIXME: 指定された ID の値を返す．
Datum Column::generic_get(RowID row_id) const {
  switch (data_type()) {
    case BOOLEAN: {
      return static_cast<const ColumnImpl<Boolean> *>(this)->get(row_id);
    }
    case INTEGER: {
      return static_cast<const ColumnImpl<Int64> *>(this)->get(row_id);
    }
    case FLOAT: {
      return static_cast<const ColumnImpl<Float> *>(this)->get(row_id);
    }
    case STRING: {
      return static_cast<const ColumnImpl<String> *>(this)->get(row_id);
    }
  }
  return false;
}

// FIXME: 指定された ID の値を設定する．
void Column::generic_set(RowID row_id, const Datum &datum) {
  switch (data_type()) {
    case BOOLEAN: {
      static_cast<ColumnImpl<Boolean> *>(this)->set(
          row_id, static_cast<Boolean>(datum));
      break;
    }
    case INTEGER: {
      static_cast<ColumnImpl<Int64> *>(this)->set(
          row_id, static_cast<Int64>(datum));
      break;
    }
    case FLOAT: {
      static_cast<ColumnImpl<Float> *>(this)->set(
          row_id, static_cast<Float>(datum));
      break;
    }
    case STRING: {
      static_cast<ColumnImpl<String> *>(this)->set(
          row_id, static_cast<std::string>(datum));
      break;
    }
  }
}

// ストリームに書き出す．
std::ostream &Column::write_to(std::ostream &stream) const {
  stream << "{ id = " << id_
         << ", name = \"" << name_ << '"'
         << ", data_type = " << data_type_
         << " }";
  return stream;
}

std::ostream &operator<<(std::ostream &stream, const Column &column) {
  return column.write_to(stream);
}

// 指定されたカラムを作成して返す．
Column *ColumnHelper::create_column(Table *table,
                             ColumnID column_id,
                             const String &column_name,
                             DataType data_type) {
  switch (data_type) {
    case BOOLEAN: {
      return new ColumnImpl<Boolean>(table, column_id, column_name);
    }
    case INTEGER: {
      return new ColumnImpl<Int64>(table, column_id, column_name);
    }
    case FLOAT: {
      return new ColumnImpl<Float>(table, column_id, column_name);
    }
    case STRING: {
      return new ColumnImpl<String>(table, column_id, column_name);
    }
  }
  return nullptr;
}

// 指定された参照型のカラムを作成して返す．
Column *ColumnHelper::create_reference_column(Table *table,
                                              ColumnID column_id,
                                              const String &column_name,
                                              Table *dest_table) {
  return new ColumnImpl<Int64>(table, column_id, column_name, dest_table);
}

}  // namespace grnxx
