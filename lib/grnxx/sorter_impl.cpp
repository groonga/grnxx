#include "grnxx/sorter_impl.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/table.hpp"

#include <ostream>

namespace grnxx {
namespace {

// 整列順序．
enum SortOrder {
  ASCENDING,
  DESCENDING
};

template <typename T>
class GenericSorterNode : public SorterNode {
 public:
  // ノードを初期化する．
  GenericSorterNode(Column *column, SortOrder sort_order)
      : column_(static_cast<ColumnImpl<T> *>(column)),
        sort_order_(sort_order),
        data_() {}

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [begin, end) の範囲に限定される．
  void sort(RowID *row_ids, Int64 num_row_ids,
            Int64 begin, Int64 end);

 private:
  ColumnImpl<T> *column_;
  SortOrder sort_order_;
  std::vector<T> data_;

  // 第一引数の優先度が第二引数より高ければ true を返す．
  struct AscendingPriorTo {
    bool operator()(const T &lhs, const T &rhs) const {
      return lhs < rhs;
    }
  };
  struct DescendingPriorTo {
    bool operator()(const T &lhs, const T &rhs) const {
      return rhs < lhs;
    }
  };

  // 三分岐のクイックソートをおこなう．
  template <typename U>
  void quick_sort(RowID *row_ids, T *values, Int64 size,
                  Int64 begin, Int64 end, U prior_to);
  // 挿入ソートをおこなう．
  template <typename U>
  void insertion_sort(RowID *row_ids, T *values, Int64 size, U prior_to);

  // 枢軸となる要素を見つけ，その要素を先頭に移動する．
  template <typename U>
  void move_pivot_first(RowID *row_ids, T *values, Int64 size, U prior_to);
};

// 与えられた行の一覧を整列する．
// 整列の結果が保証されるのは [begin, end) の範囲に限定される．
template <typename T>
void GenericSorterNode<T>::sort(RowID *row_ids, Int64 num_row_ids,
                                Int64 begin, Int64 end) {
  // 整列に使うデータを取ってくる．
  data_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    data_[i] = column_->get(row_ids[i]);
  }
  if (sort_order_ == ASCENDING) {
    quick_sort(row_ids, &*data_.begin(), num_row_ids,
               begin, end, AscendingPriorTo());
  } else {
    quick_sort(row_ids, &*data_.begin(), num_row_ids,
               begin, end, DescendingPriorTo());
  }
}

// 三分岐のクイックソートをおこなう．
template <typename T>
template <typename U>
void GenericSorterNode<T>::quick_sort(RowID *row_ids, T *values, Int64 size,
                                      Int64 begin, Int64 end, U prior_to) {
  while (size >= 16) {
    move_pivot_first(row_ids, values, size, prior_to);
    const auto &pivot = values[0];
    Int64 left = 1;
    Int64 right = size;
    Int64 pivot_left = 1;
    Int64 pivot_right = size;
    for ( ; ; ) {
      // 枢軸を基準として左右へと分ける．
      // 枢軸と等しい要素は左右端へと移動する．
      while (left < right) {
        if (prior_to(pivot, values[left])) {
          break;
        } else if (pivot == values[left]) {
          std::swap(values[left], values[pivot_left]);
          std::swap(row_ids[left], row_ids[pivot_left]);
          ++pivot_left;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (prior_to(values[right], pivot)) {
          break;
        } else if (values[right] == pivot) {
          --pivot_right;
          std::swap(values[right], values[pivot_right]);
          std::swap(row_ids[right], row_ids[pivot_right]);
        }
      }
      if (left >= right) {
        break;
      }
      std::swap(values[left], values[right]);
      std::swap(row_ids[left], row_ids[right]);
      ++left;
    }

    // 左端の値を境界の左に移動する．
    while (pivot_left > 0) {
      --pivot_left;
      --left;
      std::swap(values[pivot_left], values[left]);
      std::swap(row_ids[pivot_left], row_ids[left]);
    }
    // 右端の値を境界の右に移動する．
    while (pivot_right < size) {
      std::swap(values[pivot_right], values[right]);
      std::swap(row_ids[pivot_right], row_ids[right]);
      ++pivot_right;
      ++right;
    }

    // 枢軸と同値の範囲 [left, right) には次の検索条件を適用する．
    if (next()) {
      if (((right - left) >= 2) && (begin < right) && (end > left)) {
        Int64 next_begin = (begin < left) ? 0 : (begin - left);
        Int64 next_end = ((end > right) ? right : end) - left;
        next()->sort(row_ids + left, right - left, next_begin, next_end);
      }
    }

    // 再帰呼び出しの深さを抑えるため，左側 [0, left) と右側 [right, size) の
    // 小さい方を再帰呼び出しで処理し，大きい方を次のループで処理する．
    if (left < (size - right)) {
      if ((begin < left) && (left >= 2)) {
        Int64 next_end = (end < left) ? end : left;
        quick_sort(row_ids, values, left, begin, next_end, prior_to);
      }
      if (end <= right) {
        return;
      }
      row_ids += right;
      values += right;
      size -= right;
      begin -= right;
      if (begin < 0) {
        begin = 0;
      }
      end -= right;
    } else {
      if ((end > right) && ((size - right) >= 2)) {
        Int64 next_begin = (begin < right) ? 0 : (begin - right);
        Int64 next_end = end - right;
        quick_sort(row_ids + right, values + right, size - right,
                   next_begin, next_end, prior_to);
      }
      if (begin >= left) {
        return;
      }
      size = left;
      if (end > left) {
        end = left;
      }
    }
  }

  if (size >= 2) {
    insertion_sort(row_ids, values, size, prior_to);
  }
}

// 挿入ソートをおこなう．
template <typename T>
template <typename U>
void GenericSorterNode<T>::insertion_sort(RowID *row_ids, T *values,
                                          Int64 size, U prior_to) {
  // まずは挿入ソートをおこなう．
  for (Int64 i = 1; i < size; ++i) {
    for (Int64 j = i; j > 0; --j) {
      if (prior_to(values[j], values[j - 1])) {
        std::swap(row_ids[j], row_ids[j - 1]);
        std::swap(values[j], values[j - 1]);
      } else {
        break;
      }
    }
  }

  // 同値の範囲には次の検索条件を適用する．
  if (next()) {
    Int64 begin = 0;
    for (Int64 i = 1; i < size; ++i) {
      if (values[i] != values[begin]) {
        if ((i - begin) >= 2) {
          next()->sort(row_ids + begin, i - begin, 0, i - begin);
        }
        begin = i;
      }
    }
    if ((size - begin) >= 2) {
      next()->sort(row_ids + begin, size - begin, 0, size - begin);
    }
  }
}

// 枢軸となる要素を見つけ，その要素を先頭に移動する．
template <typename T>
template <typename U>
void GenericSorterNode<T>::move_pivot_first(RowID *row_ids, T *values,
                                            Int64 size, U prior_to) {
  RowID first = 1;
  RowID middle = size / 2;
  RowID last = size - 2;
  if (prior_to(values[first], values[middle])) {
    // first < middle.
    if (prior_to(values[middle], values[last])) {
      // first < middle < last.
      std::swap(values[0], values[middle]);
      std::swap(row_ids[0], row_ids[middle]);
    } else if (prior_to(values[first], values[last])) {
      // first < last < middle.
      std::swap(values[0], values[last]);
      std::swap(row_ids[0], row_ids[last]);
    } else {
      // last < first < middle.
      std::swap(values[0], values[first]);
      std::swap(row_ids[0], row_ids[first]);
    }
  } else if (prior_to(values[last], values[middle])) {
    // last < middle < first.
    std::swap(values[0], values[middle]);
    std::swap(row_ids[0], row_ids[middle]);
  } else if (prior_to(values[last], values[first])) {
    // middle < last < first.
    std::swap(values[0], values[last]);
    std::swap(row_ids[0], row_ids[last]);
  } else {
    // middle < first < last.
    std::swap(values[0], values[first]);
    std::swap(row_ids[0], row_ids[first]);
  }
}

class BooleanSorterNode : public SorterNode {
 public:
  // ノードを初期化する．
  BooleanSorterNode(Column *column, SortOrder sort_order)
      : SorterNode(),
        column_(static_cast<ColumnImpl<Boolean> *>(column)),
        sort_order_(sort_order) {}

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [begin, end) の範囲に限定される．
  void sort(RowID *row_ids, Int64 num_row_ids,
            Int64 begin, Int64 end);

 private:
  ColumnImpl<Boolean> *column_;
  SortOrder sort_order_;

  // 昇順のときは TRUE を後ろに移動する．
  struct AscendingIsPrior {
    bool operator()(Boolean value) const {
      return !value;
    }
  };
  // 降順のときは FALSE を後ろに移動する．
  struct DescendingIsPrior {
    bool operator()(Boolean value) const {
      return value;
    }
  };

  // 行 ID の一覧を全域ソートする．
  template <typename T>
  void entire_sort(RowID *row_ids, Int64 num_row_ids,
                   Int64 begin, Int64 end, T is_prior);
};

// 与えられた行の一覧を整列する．
// 整列の結果が保証されるのは [begin, end) の範囲に限定される．
void BooleanSorterNode::sort(RowID *row_ids, Int64 num_row_ids,
                             Int64 begin, Int64 end) {
  if (sort_order_ == ASCENDING) {
    entire_sort(row_ids, num_row_ids, begin, end, AscendingIsPrior());
  } else {
    entire_sort(row_ids, num_row_ids, begin, end, DescendingIsPrior());
  }
}

// 行 ID の一覧を全域ソートする．
template <typename T>
void BooleanSorterNode::entire_sort(RowID *row_ids, Int64 num_row_ids,
                                    Int64 begin, Int64 end, T is_prior) {
  // 優先する値を左に，そうでない値を右に移動する．
  Int64 left = 0;
  Int64 right = num_row_ids;
  while (left < right) {
    auto value = column_->get(row_ids[left]);
    if (is_prior(value)) {
      ++left;
    } else {
      --right;
      std::swap(row_ids[left], row_ids[right]);
    }
  }
  // この時点で left, right は等しく，左右の境界になっている．
  if (next()) {
    // 左右に 2 つ以上の行があり，整列範囲と重なっていれば，次の整列条件を適用する．
    if ((left >= 2) && (begin < left)) {
      next()->sort(row_ids, left, begin, (end < left) ? end : left);
    }
    if (((num_row_ids - left) >= 2) && (end > left)) {
      if (begin < left) {
        begin = 0;
      } else {
        begin -= left;
      }
      end -= left;
      next()->sort(row_ids + left, num_row_ids - left, begin, end);
    }
  }
}

// 行 ID の大小関係にしたがって整列する．
class RowIDSorterNode : public SorterNode {
 public:
  // ノードを初期化する．
  explicit RowIDSorterNode(SortOrder sort_order) : sort_order_(sort_order) {}

  // 与えられた行の一覧を整列する．
  // 整列の結果が保証されるのは [begin, end) の範囲に限定される．
  void sort(RowID *row_ids, Int64 num_row_ids,
            Int64 begin, Int64 end);

 private:
  SortOrder sort_order_;
};

// 与えられた行の一覧を整列する．
// 整列の結果が保証されるのは [begin, end) の範囲に限定される．
void RowIDSorterNode::sort(RowID *row_ids, Int64 num_row_ids,
                           Int64 begin, Int64 end) {
  // 整列対象が多くて整列範囲が狭いときは部分ソートを使い，それ以外のときは全域を整列する．
  // FIXME: 適当に設定したので，将来的には最適な設定を求めるべきである．
  // FIXME: 行 ID に重複がないという前提になっているため，次の整列条件があっても関係ない．
  if ((num_row_ids >= 1000) && (end < 100)) {
    if (sort_order_ == ASCENDING) {
      std::partial_sort(row_ids, row_ids + end,
                        row_ids + num_row_ids, std::less<RowID>());
    } else {
      std::partial_sort(row_ids, row_ids + end,
                        row_ids + num_row_ids, std::greater<RowID>());
    }
  } else {
    if (sort_order_ == ASCENDING) {
      std::sort(row_ids, row_ids + num_row_ids, std::less<RowID>());
    } else {
      std::sort(row_ids, row_ids + num_row_ids, std::greater<RowID>());
    }
  }
}

}  // namespace

// 整列器を初期化する．
SorterImpl::SorterImpl()
    : Sorter(),
      table_(nullptr),
      nodes_() {}

// 整列器を破棄する．
SorterImpl::~SorterImpl() {}

// 指定された文字列に対応する整列器を作成する．
// 文字列には，コンマ区切りでカラムを指定することができる．
// カラム名の先頭に '-' を付けると降順になる．
bool SorterImpl::parse(const Table *table, String query) {
  table_ = table;
  nodes_.clear();
  while (!query.empty()) {
    Int64 delim_pos = query.find_first_of(',');
    String column_name;
    if (delim_pos == query.npos) {
      column_name = query;
      query = "";
    } else {
      column_name = query.prefix(delim_pos);
      query = query.except_prefix(delim_pos + 1);
    }
    if (!append_column(column_name)) {
      return false;
    }
  }
  return true;
}

// 与えられた行の一覧を整列する．
// 整列の結果が保証されるのは [offset, offset + limit) の範囲に限定される．
void SorterImpl::sort(RowID *row_ids, Int64 num_row_ids,
                      Int64 offset, Int64 limit) {
  // 整列条件が存在しなければ何もしない．
  if (nodes_.empty()) {
    return;
  }
  // 整列対象が存在しなければ何もしない．
  if ((num_row_ids <= 1) || (offset >= num_row_ids) || (limit <= 0)) {
    return;
  }
  // 整列範囲を補正する．
  if (offset < 0) {
    offset = 0;
  }
  if (limit > (num_row_ids - offset)) {
    limit = num_row_ids - offset;
  }
  nodes_.front()->sort(row_ids, num_row_ids, offset, offset + limit);
}

// 指定された名前のカラムを整列条件に加える．
// 先頭に '-' を付けると降順になる．
// 成功すれば true を返し，失敗すれば false を返す．
bool SorterImpl::append_column(String column_name) {
  Column *column = nullptr;
  SortOrder sort_order = ASCENDING;
  if (column_name.starts_with("-")) {
    column_name = column_name.except_prefix(1);
    sort_order = DESCENDING;
  }
  if (column_name != "_id") {
    column = table_->get_column_by_name(column_name);
    if (!column) {
      return false;
    }
  }
  std::unique_ptr<SorterNode> node;
  if (column) {
    switch (column->data_type()) {
      case BOOLEAN: {
        node.reset(new BooleanSorterNode(column, sort_order));
        break;
      }
      case INTEGER: {
        node.reset(new GenericSorterNode<Int64>(column, sort_order));
        break;
      }
      case FLOAT: {
        node.reset(new GenericSorterNode<Float>(column, sort_order));
        break;
      }
      case STRING: {
        node.reset(new GenericSorterNode<String>(column, sort_order));
        break;
      }
    }
  } else {
    node.reset(new RowIDSorterNode(sort_order));
  }
  if (!nodes_.empty()) {
    nodes_.back()->set_next(node.get());
  }
  nodes_.push_back(std::move(node));
  return true;
}

}  // namespace grnxx
