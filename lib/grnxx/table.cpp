#include "grnxx/table.hpp"

#include "grnxx/calc.hpp"
#include "grnxx/column_impl.hpp"
#include "grnxx/index.hpp"
#include "grnxx/sorter.hpp"

#include <ostream>

namespace grnxx {
namespace {

// カンマ区切りの文字列をばらす．
std::vector<String> split_column_names(
    const String &comma_separated_column_names) {
  std::vector<String> column_names;
  if (comma_separated_column_names.empty()) {
    return column_names;
  }
  Int64 begin = 0;
  while (begin < comma_separated_column_names.size()) {
    auto end = comma_separated_column_names.find_first_of(',', begin);
    if (end == comma_separated_column_names.npos) {
      end = comma_separated_column_names.size();
    }
    column_names.push_back(
        comma_separated_column_names.extract(begin, end - begin));
    begin = end + 1;
  }
  return column_names;
}

// FIXME: これのためだけに column_impl.hpp を #include している状態なので，
//        ColumnImpl に持たせるべきなのかもしれない．
// 基準となるカラムの値が変化する位置を求める．
template <typename T>
void find_boundaries(const RowID *row_ids, Int64 num_row_ids,
                     const Column *column,
                     const std::vector<Int64> &boundaries,
                     std::vector<Int64> *new_boundaries) {
  // あらかじめ十分なサイズの仮想領域を確保することで，拡張にかかるコストをなくす．
  new_boundaries->reserve(num_row_ids);
  auto data = static_cast<const ColumnImpl<T> *>(column);
  Int64 begin = 0;
  for (auto end : boundaries) {
    for (auto i = begin + 1; i < end; ++i) {
      if (data->get(row_ids[i - 1]) != data->get(row_ids[i])) {
        new_boundaries->push_back(i);
      }
    }
    new_boundaries->push_back(end);
    begin = end;
  }
}

}  // namespace

// テーブルを初期化する．
Table::Table(Database *database, TableID id, const String &name)
    : database_(database),
      id_(id),
      name_(reinterpret_cast<const char *>(name.data()), name.size()),
      max_row_id_(MIN_ROW_ID - 1),
      columns_(MIN_COLUMN_ID),
      columns_map_(),
      indexes_(MIN_INDEX_ID),
      indexes_map_() {}

// テーブルを破棄する．
Table::~Table() {}

// 指定された名前とデータ型のカラムを作成して返す．
// 失敗すると nullptr を返す．
Column *Table::create_column(const String &column_name, DataType data_type) {
  auto it = columns_map_.find(column_name);
  if (it != columns_map_.end()) {
    return nullptr;
  }
  ColumnID column_id = min_column_id();
  for ( ; column_id <= max_column_id(); ++column_id) {
    if (!columns_[column_id]) {
      break;
    }
  }
  if (column_id > max_column_id()) {
    columns_.resize(column_id + 1);
  }
  std::unique_ptr<Column> new_column(
      ColumnHelper::create(this, column_id, column_name, data_type));
  new_column->resize(max_row_id());
  columns_map_.insert(it, std::make_pair(new_column->name(), column_id));
  columns_[column_id] = std::move(new_column);
  return columns_[column_id].get();
}

// 指定された名前のカラムを破棄する．
// 成功すれば true を返し，失敗すれば false を返す．
bool Table::drop_column(const String &column_name) {
  auto it = columns_map_.find(column_name);
  if (it == columns_map_.end()) {
    return false;
  }
  auto &column = columns_[it->second];
  for (auto &index : indexes_) {
    if (index) {
      if (index->column() == column.get()) {
        drop_index(index->name());
      }
    }
  }
  column.reset();
  columns_map_.erase(it);
  return true;
}

// 指定された 名前 のカラムを返す．
// なければ nullptr を返す．
Column *Table::get_column_by_name(const String &column_name) const {
  auto it = columns_map_.find(column_name);
  if (it == columns_map_.end()) {
    return nullptr;
  }
  return columns_[it->second].get();
}

// カラムの一覧を columns の末尾に追加し，カラムの数を返す．
Int64 Table::get_columns(std::vector<Column *> *columns) const {
  std::size_t old_size = columns->size();
  for (auto &column : columns_) {
    if (column) {
      columns->push_back(column.get());
    }
  }
  return columns->size() - old_size;
}

// 指定された名前，対象カラム，種類の索引を作成して返す．
// 失敗すると nullptr を返す．
Index *Table::create_index(const String &index_name, const String &column_name,
                           IndexType index_type) {
  Column *column = get_column_by_name(column_name);
  if (!column) {
    return nullptr;
  }
  auto it = indexes_map_.find(index_name);
  if (it != indexes_map_.end()) {
    return nullptr;
  }
  IndexID index_id = min_index_id();
  for ( ; index_id <= max_index_id(); ++index_id) {
    if (!indexes_[index_id]) {
      break;
    }
  }
  if (index_id > max_index_id()) {
    indexes_.resize(index_id + 1);
  }
  std::unique_ptr<Index> new_index(
      IndexHelper::create(index_id, index_name, column, index_type));
  if (!column->register_index(new_index.get())) {
    return nullptr;
  }
  for (RowID row_id = min_row_id(); row_id <= max_row_id(); ++row_id) {
    new_index->insert(row_id);
  }
  indexes_map_.insert(it, std::make_pair(new_index->name(), index_id));
  indexes_[index_id] = std::move(new_index);
  return indexes_[index_id].get();
}

// 指定された名前の索引を破棄する．
// 成功すれば true を返し，失敗すれば false を返す．
bool Table::drop_index(const String &index_name) {
  auto it = indexes_map_.find(index_name);
  if (it == indexes_map_.end()) {
    return false;
  }
  auto &index = indexes_[it->second];
  if (!index->column()->unregister_index(index.get())) {
    return false;
  }
  index.reset();
  indexes_map_.erase(it);
  return true;
}

// 指定された名前の索引を返す．
// なければ nullptr を返す．
Index *Table::get_index_by_name(const String &index_name) const {
  auto it = indexes_map_.find(index_name);
  if (it == indexes_map_.end()) {
    return nullptr;
  }
  return indexes_[it->second].get();
}

// 索引の一覧を indexes の末尾に追加し，索引の数を返す．
Int64 Table::get_indexes(std::vector<Index *> *indexes) const {
  std::size_t old_size = indexes->size();
  for (auto &index : indexes_) {
    if (index) {
      indexes->push_back(index.get());
    }
  }
  return indexes->size() - old_size;
}

// 行を追加して，その行 ID を返す．
RowID Table::insert_row() {
  RowID row_id = max_row_id() + 1;
  for (auto &column : columns_) {
    if (column) {
      column->resize(row_id);
    }
  }
  max_row_id_ = row_id;
  return row_id;
}

// 指定されたテーブルの範囲内にある行 ID を取得するようにカーソルを初期化する．
Table::Cursor::Cursor(RowID range_min, RowID range_max)
    : RowIDCursor(),
      row_id_(range_min),
      max_row_id_(range_max) {}

// 行 ID を最大 limit 個取得して row_ids の末尾に追加し，取得した行 ID の数を返す．
// 取得できる行 ID が尽きたときは limit より小さい値を返す．
Int64 Table::Cursor::get_next(Int64 limit, std::vector<RowID> *row_ids) {
  if (row_ids) {
    Int64 count = 0;
    while ((count < limit) && (row_id_ <= max_row_id_)) {
      row_ids->push_back(row_id_);
      ++row_id_;
      ++count;
    }
    return count;
  } else {
    Int64 count;
    if (limit > (max_row_id_ - row_id_ + 1)) {
      count = max_row_id_ - row_id_ + 1;
    } else {
      count = limit;
    }
    row_id_ += count;
    return count;
  }
}

// 指定された範囲の行 ID を取得するカーソルを作成して返す．
// 範囲が省略された，もしくは 0 を指定されたときは，
// 行 ID の最小値・最大値を範囲として使用する．
Table::Cursor *Table::create_cursor(RowID range_min, RowID range_max) const {
  if (range_min < min_row_id()) {
    range_min = min_row_id();
  }
  if (range_max > max_row_id()) {
    range_max = max_row_id();
  }
  return new Cursor(range_min, range_max);
}

// 演算器を作成する．
Calc *Table::create_calc(const String &query) const {
  return CalcHelper::create(this, query);
}

// 整列器を作成する．
Sorter *Table::create_sorter(const String &query) const {
  return SorterHelper::create(this, query);
}

// 整列済みの行一覧を受け取り，グループの境界を求める．
bool Table::group_by(const RowID *row_ids, Int64 num_row_ids,
                     const String &comma_separated_column_names,
                     std::vector<Int64> *boundaries) {
  boundaries->clear();
  if (num_row_ids == 0) {
    return true;
  }
  boundaries->push_back(num_row_ids);
  auto column_names = split_column_names(comma_separated_column_names);
  for (const String &column_name : column_names) {
    // 一カラムずつ走査する．
    auto column = get_column_by_name(column_name);
    if (!column) {
      return false;
    }
    std::vector<Int64> new_boundaries;
    switch (column->data_type()) {
      case BOOLEAN: {
        find_boundaries<Boolean>(row_ids, num_row_ids, column,
                                 *boundaries, &new_boundaries);
        break;
      }
      case INTEGER: {
        find_boundaries<Int64>(row_ids, num_row_ids, column,
                               *boundaries, &new_boundaries);
        break;
      }
      case FLOAT: {
        find_boundaries<Float>(row_ids, num_row_ids, column,
                               *boundaries, &new_boundaries);
        break;
      }
      case STRING: {
        find_boundaries<String>(row_ids, num_row_ids, column,
                                *boundaries, &new_boundaries);
        break;
      }
      default: {
        return false;
      }
    }
    boundaries->swap(new_boundaries);
  }
  return true;
}

// ストリームに書き出す．
std::ostream &Table::write_to(std::ostream &stream) const {
  stream << "{ id = " << id_
         << ", name = \"" << name_ << '"'
         << ", columns = ";
  std::vector<Column *> columns;
  if (get_columns(&columns) == 0) {
    stream << "{}";
  } else {
    stream << "{ " << *columns[0];
    for (std::size_t i = 1; i < columns.size(); ++i) {
      stream << ", " << *columns[i];
    }
    stream << " }";
  }
  stream << " }";
  return stream;
}

// 行の一覧を文字列化する．
std::ostream &Table::write_to(std::ostream &stream,
                              const RowID *row_ids, Int64 num_row_ids,
                              const String &comma_separated_column_names) {
  std::vector<String> column_names;
  std::vector<Column *> columns;
  if (comma_separated_column_names.empty() ||
      (comma_separated_column_names == "*")) {
    // 指定なし，およびに "*" のときはすべてのカラムに展開する．
    get_columns(&columns);
    column_names.push_back("_id");
    for (auto column : columns) {
      column_names.push_back(column->name());
    }
    columns.insert(columns.begin(), nullptr);
  } else {
    column_names = split_column_names(comma_separated_column_names);
    for (const String &column_name : column_names) {
      auto column = get_column_by_name(column_name);
      if (!column) {
        // "_id"という名前が指定されて，しかも該当するカラムが存在しないときは行IDを使う．
        if (column_name != "_id") {
//          std::cerr << "unknown column: \"" << column_name << "\"";
          return stream;
        }
      }
      columns.push_back(column);
    }
  }
  stream << "{ column_names = {";
  for (std::size_t i = 0; i < column_names.size(); ++i) {
    if (i == 0) {
      stream << ' ';
    }
    stream << '"' << column_names[i] << '"';
    if (i != (column_names.size() - 1)) {
      stream << ',';
    }
    stream << ' ';
  }
  stream << "}, rows = {";

  for (Int64 i = 0; i < num_row_ids; ++i) {
    if (i == 0) {
      stream << ' ';
    }
    stream << '{';
    for (std::size_t j = 0; j < columns.size(); ++j) {
      if (j == 0) {
        stream << ' ';
      }
      if (columns[j]) {
        stream << columns[j]->generic_get(row_ids[i]);
      } else {
        stream << row_ids[i];
      }
      if (j != (columns.size() - 1)) {
        stream << ',';
      }
      stream << ' ';
    }
    stream << '}';
    if (i != (num_row_ids - 1)) {
      stream << ',';
    }
    stream << ' ';
  }
  stream << "} }";
  return stream;
}

// 行の一覧をグループ毎に文字列化する．
std::ostream &Table::write_to(std::ostream &stream,
                              const RowID *row_ids, Int64 num_row_ids,
                              const std::vector<Int64> &boundaries,
                              const String &comma_separated_column_names) {
  std::vector<String> column_names;
  std::vector<Column *> columns;
  if (comma_separated_column_names.empty() ||
      (comma_separated_column_names == "*")) {
    // 指定なし，およびに "*" のときはすべてのカラムに展開する．
    get_columns(&columns);
    column_names.push_back("_id");
    for (auto column : columns) {
      column_names.push_back(column->name());
    }
    columns.insert(columns.begin(), nullptr);
  } else {
    column_names = split_column_names(comma_separated_column_names);
    for (const String &column_name : column_names) {
      auto column = get_column_by_name(column_name);
      if (!column) {
        // "_id"という名前が指定されて，しかも該当するカラムが存在しないときは行IDを使う．
        if (column_name != "_id") {
//          std::cerr << "unknown column: \"" << column_name << "\"";
          return stream;
        }
      }
      columns.push_back(column);
    }
  }
  stream << "{ column_names = {";
  for (std::size_t i = 0; i < column_names.size(); ++i) {
    if (i == 0) {
      stream << ' ';
    }
    stream << '"' << column_names[i] << '"';
    if (i != (column_names.size() - 1)) {
      stream << ',';
    }
    stream << ' ';
  }
  stream << "}, groups = {";

  Int64 begin = 0;
  for (auto end : boundaries) {
    if (end == boundaries.front()) {
      stream << ' ';
    }
    stream << '{';
    for (Int64 i = begin; i < end; ++i) {
      if (i == begin) {
        stream << ' ';
      }
      stream << '{';
      for (std::size_t j = 0; j < columns.size(); ++j) {
        if (j == 0) {
          stream << ' ';
        }
        if (columns[j]) {
          stream << columns[j]->generic_get(row_ids[i]);
        } else {
          stream << row_ids[i];
        }
        if (j != (columns.size() - 1)) {
          stream << ',';
        }
        stream << ' ';
      }
      stream << '}';
      if (i != (end - 1)) {
        stream << ',';
      }
      stream << ' ';
    }
    stream << '}';
    if (end != boundaries.back()) {
      stream << ',';
    }
    stream << ' ';
    begin = end;
  }
  stream << "} }";
  return stream;
}

std::ostream &operator<<(std::ostream &stream, const Table &table) {
  return table.write_to(stream);
}

}  // namespace grnxx
