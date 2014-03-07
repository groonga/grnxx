#include "grnxx/calc_impl.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/table.hpp"

namespace grnxx {
namespace {

// 定数と対応するノード．
template <typename T>
class ConstantNode : public CalcNode {
 public:
  // 指定された定数と対応するノードとして初期化する．
  explicit ConstantNode(T value)
      : CalcNode(CONSTANT_NODE, TypeTraits<T>::data_type()),
        value_(value) {}
  // ノードを破棄する．
  ~ConstantNode() {}

  // 指定された値を返す．
  T get(Int64 i, RowID row_id) const {
    return value_;
  }

 private:
  T value_;
};

// 真偽値と対応するノード．
template <>
class ConstantNode<Boolean> : public CalcNode {
 public:
  // 指定された定数と対応するノードとして初期化する．
  explicit ConstantNode(Boolean value)
      : CalcNode(CONSTANT_NODE, TypeTraits<Boolean>::data_type()),
        value_(value) {}
  // ノードを破棄する．
  ~ConstantNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids) {
    // 真のときは何もせず，偽のときはクリアする．
    return value_ ? num_row_ids : 0;
  }

  // 指定された値を返す．
  Boolean get(Int64 i, RowID row_id) const {
    return value_;
  }

 private:
  Boolean value_;
};

// 文字列の定数と対応するノード．
template <>
class ConstantNode<String> : public CalcNode {
 public:
  // 指定された定数と対応するノードとして初期化する．
  explicit ConstantNode(const String &value)
      : CalcNode(CONSTANT_NODE, TypeTraits<String>::data_type()),
        value_(),
        buf_() {
    buf_.assign(reinterpret_cast<const char *>(value.data()), value.size());
    value_ = buf_;
  }
  // ノードを破棄する．
  ~ConstantNode() {}

  // 指定された値を返す．
  String get(Int64 i, RowID row_id) const {
    return value_;
  }

 private:
  String value_;
  std::string buf_;
};

// カラムと対応するノード．
template <typename T>
class ColumnNode : public CalcNode {
 public:
  // 指定されたカラムに対応するノードとして初期化する．
  explicit ColumnNode(Column *column)
      : CalcNode(COLUMN_NODE, TypeTraits<T>::data_type()),
        column_(static_cast<ColumnImpl<T> *>(column)) {}
  // ノードを破棄する．
  ~ColumnNode() {}

  ColumnImpl<T> *column() const {
    return column_;
  }

  // 指定された値を返す．
  T get(Int64 i, RowID row_id) const {
    return column_->get(row_id);
  }

 private:
  ColumnImpl<T> *column_;
};

// 真偽値のカラムと対応するノード．
template <>
class ColumnNode<Boolean> : public CalcNode {
 public:
  // 指定されたカラムに対応するノードとして初期化する．
  explicit ColumnNode(Column *column)
      : CalcNode(COLUMN_NODE, TypeTraits<Boolean>::data_type()),
        column_(static_cast<ColumnImpl<Boolean> *>(column)) {}
  // ノードを破棄する．
  ~ColumnNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 指定された値を返す．
  Boolean get(Int64 i, RowID row_id) const {
    return column_->get(row_id);
  }

 private:
  ColumnImpl<Boolean> *column_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
Int64 ColumnNode<Boolean>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 真になる行だけを残す．
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (column_->get(row_id)) {
      row_ids[count++] = row_id;
    }
  }
  return count;
}

// 演算子と対応するノード．
template <typename T>
class OperatorNode : public CalcNode {
 public:
  // 指定された定数と対応するノードとして初期化する．
  OperatorNode()
      : CalcNode(OPERATOR_NODE, TypeTraits<T>::data_type()),
        data_() {}
  // ノードを破棄する．
  virtual ~OperatorNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  virtual Int64 filter(RowID *row_ids, Int64 num_row_ids) = 0;

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  virtual void fill(const RowID *row_ids, Int64 num_row_ids) = 0;

  // 指定された値を返す．
  T get(Int64 i, RowID row_id) const {
    return data_[i];
  }

 protected:
  std::vector<T> data_;
};

// LOGICAL_NOT と対応するノード．
template <typename T>
class LogicalNotNode : public OperatorNode<Boolean> {
 public:
  // 指定されたノードに対する LOGICAL_NOT と対応するノードとして初期化する．
  explicit LogicalNotNode(CalcNode *operand)
      : OperatorNode<Boolean>(),
        operand_(static_cast<T *>(operand)) {}
  // ノードを破棄する．
  ~LogicalNotNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  T *operand_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T>
Int64 LogicalNotNode<T>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 真になる行だけを残す．
  operand_->fill(row_ids, num_row_ids);
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (!operand_->get(i, row_id)) {
      row_ids[count++] = row_id;
    }
  }
  return count;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T>
void LogicalNotNode<T>::fill(const RowID *row_ids, Int64 num_row_ids) {
  operand_->fill(row_ids, num_row_ids);
  data_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    data_[i] = !operand_->get(i, row_ids[i]);
  }
}

// 比較演算子の実装．
template <typename T> using EqualOperator = std::equal_to<T>;
template <typename T> using NotEqualOperator = std::not_equal_to<T>;
template <typename T> using LessOperator = std::less<T>;
template <typename T> using LessEqualOperator = std::less_equal<T>;
template <typename T> using GreaterOperator = std::greater<T>;
template <typename T> using GreaterEqualOperator = std::greater_equal<T>;

// 比較演算子と対応するノード．
template <typename T, typename U, typename V>
class ComparerNode : public OperatorNode<Boolean> {
 public:
  // 指定されたノードに対する比較演算子と対応するノードとして初期化する．
  explicit ComparerNode(CalcNode *lhs, CalcNode *rhs)
      : OperatorNode<Boolean>(),
        comparer_(),
        lhs_(static_cast<U *>(lhs)),
        rhs_(static_cast<V *>(rhs)) {}
  // ノードを破棄する．
  ~ComparerNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  T comparer_;
  U *lhs_;
  V *rhs_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T, typename U, typename V>
Int64 ComparerNode<T, U, V>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 真になる行だけを残す．
  lhs_->fill(row_ids, num_row_ids);
  rhs_->fill(row_ids, num_row_ids);
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (comparer_(lhs_->get(i, row_id), rhs_->get(i, row_id))) {
      row_ids[count++] = row_id;
    }
  }
  return count;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T, typename U, typename V>
void ComparerNode<T, U, V>::fill(const RowID *row_ids, Int64 num_row_ids) {
  lhs_->fill(row_ids, num_row_ids);
  rhs_->fill(row_ids, num_row_ids);
  data_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    data_[i] = comparer_(lhs_->get(i, row_id), rhs_->get(i, row_id));
  }
}

// LOGICAL_AND と対応するノード．
template <typename T, typename U>
class LogicalAndNode : public OperatorNode<Boolean> {
 public:
  // 指定されたノードに対する LOGICAL_AND と対応するノードとして初期化する．
  explicit LogicalAndNode(CalcNode *lhs, CalcNode *rhs)
      : OperatorNode<Boolean>(),
        lhs_(static_cast<T *>(lhs)),
        rhs_(static_cast<U *>(rhs)),
        local_row_ids_() {}
  // ノードを破棄する．
  ~LogicalAndNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  T *lhs_;
  U *rhs_;
  std::vector<RowID> local_row_ids_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T, typename U>
Int64 LogicalAndNode<T, U>::filter(RowID *row_ids, Int64 num_row_ids) {
  num_row_ids = lhs_->filter(row_ids, num_row_ids);
  num_row_ids = rhs_->filter(row_ids, num_row_ids);
  return num_row_ids;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T, typename U>
void LogicalAndNode<T, U>::fill(const RowID *row_ids, Int64 num_row_ids) {
  // 必要な領域を確保する．
  data_.resize(num_row_ids);
  local_row_ids_.resize(num_row_ids);

  lhs_->fill(row_ids, num_row_ids);

  // 左側の評価が真になる行のみを抜き出す．
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (lhs_->get(i, row_id)) {
      local_row_ids_[count] = row_id;
      ++count;
    }
  }

  rhs_->fill(&*local_row_ids_.begin(), count);

  // 左右ともに真になる行のみ真にする．
  Int64 j = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    auto lhs_value = lhs_->get(i, row_ids[i]);
    if (lhs_value) {
      data_[i] = rhs_->get(j, row_ids[j]);
      ++j;
    } else {
      data_[i] = lhs_value;
    }
  }
}

// LOGICAL_OR と対応するノード．
template <typename T, typename U>
class LogicalOrNode : public OperatorNode<Boolean> {
 public:
  // 指定されたノードに対する LOGICAL_OR と対応するノードとして初期化する．
  explicit LogicalOrNode(CalcNode *lhs, CalcNode *rhs)
      : OperatorNode<Boolean>(),
        lhs_(static_cast<T *>(lhs)),
        rhs_(static_cast<U *>(rhs)),
        local_row_ids_() {}
  // ノードを破棄する．
  ~LogicalOrNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  T *lhs_;
  U *rhs_;
  std::vector<RowID> local_row_ids_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T, typename U>
Int64 LogicalOrNode<T, U>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 入力のコピーを作成する．
  // 番兵を追加しても大丈夫なように，サイズは num_row_ids + 2 としている．
  local_row_ids_.resize(num_row_ids + 2);
  RowID *left_row_ids = &*local_row_ids_.begin();
  for (Int64 i = 0; i < num_row_ids; ++i) {
    local_row_ids_[i] = row_ids[i];
  }

  // 入力のコピーを左のフィルタにかける．
  Int64 num_left_row_ids = lhs_->filter(left_row_ids, num_row_ids);
  if (num_left_row_ids == 0) {
    // すべて偽なので，入力を丸ごと右のフィルタにかける．
    return rhs_->filter(row_ids, num_row_ids);
  } else if (num_left_row_ids == num_row_ids) {
    // すべて真なので，何もしなくてよい．
    return num_row_ids;
  }

  // 番兵を配置する．
  left_row_ids[num_left_row_ids] = 0;

  // 左のフィルタにかけて残らなかった行 ID を列挙する．
  RowID *right_row_ids = left_row_ids + num_left_row_ids + 1;
  Int64 left_count = 0;
  Int64 right_count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    if (row_ids[i] == left_row_ids[left_count]) {
      ++left_count;
    } else {
      right_row_ids[right_count] = row_ids[i];
      ++right_count;
    }
  }

  // 右のフィルタにかける．
  Int64 num_right_row_ids = rhs_->filter(right_row_ids, right_count);
  if (num_right_row_ids == 0) {
    // すべて偽なので，左のをそのまま返せばよい．
    for (Int64 i = 0; i < num_left_row_ids; ++i) {
      row_ids[i] = left_row_ids[i];
    }
    return num_left_row_ids;
  } else if (num_right_row_ids == right_count) {
    // すべて真なので，何もしなくてよい．
    return num_row_ids;
  }

  // 番兵を配置する．
  right_row_ids[num_right_row_ids] = 0;

  // 左右の結果をマージする（どちらかに含まれている行 ID を取り出す）．
  left_count = 0;
  right_count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    if (row_ids[i] == left_row_ids[left_count]) {
      row_ids[left_count + right_count] = row_ids[i];
      ++left_count;
    } else if (row_ids[i] == right_row_ids[right_count]) {
      row_ids[left_count + right_count] = row_ids[i];
      ++right_count;
    }
  }
  return left_count + right_count;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T, typename U>
void LogicalOrNode<T, U>::fill(const RowID *row_ids, Int64 num_row_ids) {
  // 必要な領域を確保する．
  data_.resize(num_row_ids);
  local_row_ids_.resize(num_row_ids);

  lhs_->fill(row_ids, num_row_ids);

  // 左側の評価が偽になる行のみを抜き出す．
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (!lhs_->get(i, row_id)) {
      local_row_ids_[count] = row_id;
      ++count;
    }
  }

  rhs_->fill(&*local_row_ids_.begin(), count);

  // 左右ともに真になる行のみ真にする．
  Int64 j = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    auto lhs_value = lhs_->get(i, row_ids[i]);
    if (lhs_value) {
      data_[i] = lhs_value;
    } else {
      data_[i] = rhs_->get(j, row_ids[j]);
      ++j;
    }
  }
}

// FIXME: 以下，同じような演算子を定義するのはマクロで簡略化できるし，
//        そうした方が誤りを減らせそうである．

// FIXME: オーバーフロー時の例外には専用の型を用意すべきである．

// 無効な型．
struct InvalidResult {};

// 算術加算．
//Int64 plus_with_overflow_check(Int64 lhs, Int64 rhs) {
//  Int64 result = lhs + rhs;
//  lhs ^= result;
//  rhs ^= result;
//  if ((lhs & rhs) >> 63) {
//    throw "overflow or underflow!";
//  }
//  return result;
//}
#ifdef __clang__
Int64 plus_with_overflow_check(Int64 lhs, Int64 rhs) {
  UInt8 overflow_flag = 0;
  __asm__ ("add %2, %0; seto %1;"
           : "+r" (lhs), "=r" (overflow_flag) : "r" (rhs));
  if (overflow_flag) {
    throw "overflow or underflow!";
  }
  return lhs;
}
#else  // __clang__
Int64 plus_with_overflow_check(Int64 lhs, Int64 rhs) {
  __asm__ volatile ("add %1, %0;"
                    : "+r" (lhs) : "r" (rhs));
  __asm__ goto ("jo %l0;"
                : : : : PLUS_WITH_OVERFLOW_CHECK_LABEL);
  return lhs;

 PLUS_WITH_OVERFLOW_CHECK_LABEL:
  throw "overflow or underflow!";
}
#endif  // __clang__
template <typename T, typename U> struct PlusOperator {
  using Result = InvalidResult;
  using Lhs = T;
  using Rhs = U;
};
template <> struct PlusOperator<Int64, Int64> {
  using Result = Int64;
  using Lhs = Int64;
  using Rhs = Int64;
  Result operator()(Int64 lhs, Int64 rhs) const {
    return plus_with_overflow_check(lhs, rhs);
  }
};
template <> struct PlusOperator<Float, Float> {
  using Result = Float;
  using Lhs = Float;
  using Rhs = Float;
  Result operator()(Float lhs, Float rhs) const { return lhs + rhs; }
};

// 算術減算．
//Int64 minus_with_overflow_check(Int64 lhs, Int64 rhs) {
//  Int64 result = lhs - rhs;
//  rhs ^= lhs;
//  lhs ^= result;
//  if ((lhs & rhs) >> 63) {
//    throw "overflow or underflow!";
//  }
//  return result;
//}
#ifdef __clang__
Int64 minus_with_overflow_check(Int64 lhs, Int64 rhs) {
  UInt8 overflow_flag = 0;
  __asm__ ("sub %2, %0; seto %1;"
           : "+r" (lhs), "=r" (overflow_flag) : "r" (rhs));
  if (overflow_flag) {
    throw "overflow or underflow!";
  }
  return lhs;
}
#else  // __clang__
Int64 minus_with_overflow_check(Int64 lhs, Int64 rhs) {
  __asm__ volatile ("sub %1, %0;"
                    : "+r" (lhs) : "r" (rhs));
  __asm__ goto ("jo %l0;"
                : : : : MINUS_WITH_OVERFLOW_CHECK_LABEL);
  return lhs;

 MINUS_WITH_OVERFLOW_CHECK_LABEL:
  throw "overflow or underflow!";
}
#endif  // __clang__
template <typename T, typename U> struct MinusOperator {
  using Result = InvalidResult;
  using Lhs = T;
  using Rhs = U;
};
template <> struct MinusOperator<Int64, Int64> {
  using Result = Int64;
  using Lhs = Int64;
  using Rhs = Int64;
  Result operator()(Int64 lhs, Int64 rhs) const {
    return minus_with_overflow_check(lhs, rhs);
  }
};
template <> struct MinusOperator<Float, Float> {
  using Result = Float;
  using Lhs = Float;
  using Rhs = Float;
  Result operator()(Float lhs, Float rhs) const { return lhs - rhs; }
};

// 算術乗算．
//Int64 multiplies_with_overflow_check(Int64 lhs, Int64 rhs) {
//  if (lhs == 0) {
//    return 0;
//  }
//  Int64 result = lhs * rhs;
//  if ((result / rhs) != lhs) {
//    throw "overflow or underflow";
//  }
//  return result;
//}
#ifdef __clang__
Int64 multiplies_with_overflow_check(Int64 lhs, Int64 rhs) {
  UInt8 overflow_flag = 0;
  __asm__ ("imul %2, %0; seto %1;"
           : "+r" (lhs), "=r" (overflow_flag) : "r" (rhs));
  if (overflow_flag) {
    throw "overflow or underflow!";
  }
  return lhs;
}
#else  // __clang__
Int64 multiplies_with_overflow_check(Int64 lhs, Int64 rhs) {
  __asm__ volatile ("imul %1, %0;"
                    : "+r" (lhs) : "r" (rhs));
  __asm__ goto ("jo %l0;"
                : : : : MULTIPLIES_WITH_OVERFLOW_CHECK_LABEL);
  return lhs;

 MULTIPLIES_WITH_OVERFLOW_CHECK_LABEL:
  throw "overflow or underflow!";
}
#endif  // __clang__
template <typename T, typename U> struct MultipliesOperator {
  using Result = InvalidResult;
  using Lhs = T;
  using Rhs = U;
};
template <> struct MultipliesOperator<Int64, Int64> {
  using Result = Int64;
  using Lhs = Int64;
  using Rhs = Int64;
  Result operator()(Int64 lhs, Int64 rhs) const {
    return multiplies_with_overflow_check(lhs, rhs);
  }
};
template <> struct MultipliesOperator<Float, Float> {
  using Result = Float;
  using Lhs = Float;
  using Rhs = Float;
  Result operator()(Float lhs, Float rhs) const { return lhs * rhs; }
};

// 算術除算．
Int64 divides_with_overflow_check(Int64 lhs, Int64 rhs) {
  if (rhs == 0) {
    throw "division by zero";
  }
  if ((lhs == INT64_MIN) && (rhs == -1)) {
    throw "overflow or underflow";
  }
  return lhs / rhs;
}
template <typename T, typename U> struct DividesOperator {
  using Result = InvalidResult;
  using Lhs = T;
  using Rhs = U;
};
template <> struct DividesOperator<Int64, Int64> {
  using Result = Int64;
  using Lhs = Int64;
  using Rhs = Int64;
  Result operator()(Int64 lhs, Int64 rhs) const {
    return divides_with_overflow_check(lhs, rhs);
  }
};
template <> struct DividesOperator<Float, Float> {
  using Result = Float;
  using Lhs = Float;
  using Rhs = Float;
  Result operator()(Float lhs, Float rhs) const { return lhs / rhs; }
};

// 算術剰余算．
Int64 modulus_with_overflow_check(Int64 lhs, Int64 rhs) {
  if (rhs == 0) {
    throw "division by zero";
  }
  if ((lhs == INT64_MIN) && (rhs == -1)) {
    throw "overflow or underflow";
  }
  return lhs % rhs;
}
template <typename T, typename U> struct ModulusOperator {
  using Result = InvalidResult;
  using Lhs = T;
  using Rhs = U;
};
template <> struct ModulusOperator<Int64, Int64> {
  using Result = Int64;
  using Lhs = Int64;
  using Rhs = Int64;
  Result operator()(Int64 lhs, Int64 rhs) const {
    return modulus_with_overflow_check(lhs, rhs);
  }
};

// 四則演算と対応するノード．
template <typename T, typename U, typename V>
class ArithmeticNode : public OperatorNode<typename T::Result> {
 public:
  using Result = typename T::Result;

  // 指定されたノードに対する四則演算と対応するノードとして初期化する．
  explicit ArithmeticNode(CalcNode *lhs, CalcNode *rhs)
      : OperatorNode<Result>(),
        operator_(),
        lhs_(static_cast<U *>(lhs)),
        rhs_(static_cast<V *>(rhs)) {}
  // ノードを破棄する．
  ~ArithmeticNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  T operator_;
  U *lhs_;
  V *rhs_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T, typename U, typename V>
Int64 ArithmeticNode<T, U, V>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 真になる行だけを残す．
  lhs_->fill(row_ids, num_row_ids);
  rhs_->fill(row_ids, num_row_ids);
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    RowID row_id = row_ids[i];
    if (operator_(lhs_->get(i, row_ids[i]), rhs_->get(i, row_ids[i]))) {
      row_ids[count++] = row_id;
    }
  }
  return count;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T, typename U, typename V>
void ArithmeticNode<T, U, V>::fill(const RowID *row_ids, Int64 num_row_ids) {
  lhs_->fill(row_ids, num_row_ids);
  rhs_->fill(row_ids, num_row_ids);
  this->data_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    this->data_[i] = operator_(lhs_->get(i, row_ids[i]),
                               rhs_->get(i, row_ids[i]));
  }
}

// 四則演算と対応するノードの作成を補助する．
template <typename T, typename U = typename T::Result>
class ArithmeticNodeHelper {
 public:
  template <typename V, typename W>
  static CalcNode *create(CalcNode *lhs, CalcNode *rhs) {
    return new ArithmeticNode<T, V, W>(lhs, rhs);
  }

  static CalcNode *create(typename T::Lhs lhs, typename T::Rhs rhs) {
    return new ConstantNode<typename T::Result>(T()(lhs, rhs));
  }
};
template <typename T>
class ArithmeticNodeHelper<T, InvalidResult> {
 public:
  template <typename V, typename W>
  static CalcNode *create(CalcNode *lhs, CalcNode *rhs) {
    return nullptr;
  }

  static CalcNode *create(typename T::Lhs lhs, typename T::Rhs rhs) {
    return nullptr;
  }
};

// 参照と対応するノード．
template <typename T>
class ReferenceNode : public OperatorNode<T> {
 public:
  using Result = T;

  // 指定されたノードに対する四則演算と対応するノードとして初期化する．
  explicit ReferenceNode(CalcNode *lhs, CalcNode *rhs)
      : OperatorNode<Result>(),
        lhs_(static_cast<ColumnNode<Int64> *>(lhs)),
        rhs_(static_cast<ColumnNode<T> *>(rhs)) {}
  // ノードを破棄する．
  ~ReferenceNode() {}

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  void fill(const RowID *row_ids, Int64 num_row_ids);

 private:
  ColumnNode<Int64> *lhs_;
  ColumnNode<T> *rhs_;
  std::vector<RowID> local_row_ids_;
};

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <typename T>
Int64 ReferenceNode<T>::filter(RowID *row_ids, Int64 num_row_ids) {
  // 参照先が真になる行だけを残す．
  lhs_->fill(row_ids, num_row_ids);
  // FIXME: lhs が行 ID の一覧を持っているのにコピーしなければならない．
  local_row_ids_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    local_row_ids_[i] = lhs_->get(i, row_ids[i]);
  }
  rhs_->fill(&*local_row_ids_.begin(), num_row_ids);
  Int64 count = 0;
  for (Int64 i = 0; i < num_row_ids; ++i) {
    if (rhs_->get(i, local_row_ids_[i])) {
      row_ids[count++] = row_ids[i];
    }
  }
  return count;
}

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
template <>
Int64 ReferenceNode<String>::filter(RowID *row_ids, Int64 num_row_ids) {
  // FIXME: String から Boolean の変換は未定義．
  return 0;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
template <typename T>
void ReferenceNode<T>::fill(const RowID *row_ids, Int64 num_row_ids) {
  lhs_->fill(row_ids, num_row_ids);
  // FIXME: lhs が行 ID の一覧を持っているのにコピーしなければならない．
  local_row_ids_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    local_row_ids_[i] = lhs_->get(i, row_ids[i]);
  }
  // FIXME: わざわざコピーしなければならない．
  rhs_->fill(&*local_row_ids_.begin(), num_row_ids);
  this->data_.resize(num_row_ids);
  for (Int64 i = 0; i < num_row_ids; ++i) {
    this->data_[i] = rhs_->get(i, local_row_ids_[i]);
  }
}

}  // namespace

// 指定された種類のノードとして初期化する．
CalcNode::CalcNode(CalcNodeType type, DataType data_type)
    : type_(type),
      data_type_(data_type) {}

// ノードを破棄する．
CalcNode::~CalcNode() {}

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
Int64 CalcNode::filter(RowID *row_ids, Int64 num_row_ids) {
  // すべて破棄する．
  return 0;
}

// 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
void CalcNode::fill(const RowID *row_ids, Int64 num_row_ids) {
  // 何もしない
}

// 二項演算子の優先度を返す．
int CalcToken::get_binary_operator_priority(BinaryOperatorType operator_type) {
  switch (operator_type) {
    case EQUAL_OPERATOR: {
      return 6;
    }
    case NOT_EQUAL_OPERATOR: {
      return 6;
    }
    case LESS_OPERATOR: {
      return 7;
    }
    case LESS_EQUAL_OPERATOR: {
      return 7;
    }
    case GREATER_OPERATOR: {
      return 7;
    }
    case GREATER_EQUAL_OPERATOR: {
      return 7;
    }
    case LOGICAL_AND_OPERATOR: {
      return 2;
    }
    case LOGICAL_OR_OPERATOR: {
      return 1;
    }
    case PLUS_OPERATOR: {
      return 8;
    }
    case MINUS_OPERATOR: {
      return 8;
    }
    case MULTIPLIES_OPERATOR: {
      return 9;
    }
    case DIVIDES_OPERATOR: {
      return 9;
    }
    case MODULUS_OPERATOR: {
      return 9;
    }
    case REFERENCE_OPERATOR: {
      return 10;
    }
  }
  return 0;
}

CalcImpl::CalcImpl()
    : Calc(),
      table_(nullptr),
      nodes_() {}

CalcImpl::~CalcImpl() {}

// 指定された文字列に対応する演算器を作成する．
bool CalcImpl::parse(const Table *table, const String &query) try {
  if (!table) {
    return false;
  }
  table_ = table;
  nodes_.clear();

  // 指定された文字列をトークンに分割する．
  // 先頭と末尾に括弧を配置することで例外をなくす．
  std::vector<CalcToken> tokens;
  tokens.push_back(CalcToken(LEFT_BRACKET));
  if (!tokenize_query(query, &tokens)) {
    return false;
  }
  tokens.push_back(CalcToken(RIGHT_BRACKET));

  // トークンがひとつもないときは何もしない．
  if (tokens.size() == 2) {
    return true;
  }

  // スタックを使って木を構築する．
  std::vector<CalcToken> stack;
  for (const auto &token : tokens) {
    if (!push_token(token, &stack)) {
      return false;
    }
  }

  // 最終的にはトークンがひとつだけ残らなければならない．
  if (stack.size() != 1) {
    return false;
  }
  return true;
} catch (const char *) {
  // ゼロによる除算もしくはオーバーフローによる例外．
  return false;
}

// 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
Int64 CalcImpl::filter(RowID *row_ids, Int64 num_row_ids) {
  if (nodes_.empty()) {
    return num_row_ids;
  }
  try {
    return nodes_.back()->filter(row_ids, num_row_ids);
  } catch (const char *) {
    // ゼロによる除算もしくはオーバーフローによる例外．
    return 0;
  }
}

// 演算が何も指定されていなければ true を返し，そうでなければ false を返す．
bool CalcImpl::empty() const {
  return nodes_.empty();
}

// クエリをトークンに分割する．
bool CalcImpl::tokenize_query(const String &query,
                              std::vector<CalcToken> *tokens) {
  String left = query;
  while (!left.empty()) {
    // 先頭の空白は無視する．
    auto delim_pos = left.find_first_not_of(" \t\r\n");
    if (delim_pos == left.npos) {
      break;
    }
    left = left.except_prefix(delim_pos);
    switch (left[0]) {
      case '!': {
        if (left[1] == '=') {
          tokens->push_back(CalcToken(NOT_EQUAL_OPERATOR));
          left = left.except_prefix(2);
        } else {
          tokens->push_back(CalcToken(LOGICAL_NOT_OPERATOR));
          left = left.except_prefix(1);
        }
        break;
      }
      case '=': {
        if (left[1] == '=') {
          tokens->push_back(CalcToken(EQUAL_OPERATOR));
          left = left.except_prefix(2);
        } else {
          return false;
        }
        break;
      }
      case '<': {
        if (left[1] == '=') {
          tokens->push_back(CalcToken(LESS_EQUAL_OPERATOR));
          left = left.except_prefix(2);
        } else {
          tokens->push_back(CalcToken(LESS_OPERATOR));
          left = left.except_prefix(1);
        }
        break;
      }
      case '>': {
        if (left[1] == '=') {
          tokens->push_back(CalcToken(GREATER_EQUAL_OPERATOR));
          left = left.except_prefix(2);
        } else {
          tokens->push_back(CalcToken(GREATER_OPERATOR));
          left = left.except_prefix(1);
        }
        break;
      }
      case '&': {
        if (left[1] == '&') {
          tokens->push_back(CalcToken(LOGICAL_AND_OPERATOR));
          left = left.except_prefix(2);
        } else {
          return false;
        }
        break;
      }
      case '|': {
        if (left[1] == '|') {
          tokens->push_back(CalcToken(LOGICAL_OR_OPERATOR));
          left = left.except_prefix(2);
        } else {
          return false;
        }
        break;
      }
      case '+': {
        tokens->push_back(CalcToken(PLUS_OPERATOR));
        left = left.except_prefix(1);
        break;
      }
      case '-': {
        tokens->push_back(CalcToken(MINUS_OPERATOR));
        left = left.except_prefix(1);
        break;
      }
      case '*': {
        tokens->push_back(CalcToken(MULTIPLIES_OPERATOR));
        left = left.except_prefix(1);
        break;
      }
      case '/': {
        tokens->push_back(CalcToken(DIVIDES_OPERATOR));
        left = left.except_prefix(1);
        break;
      }
      case '%': {
        tokens->push_back(CalcToken(MODULUS_OPERATOR));
        left = left.except_prefix(1);
        break;
      }
      case '(': {
        tokens->push_back(CalcToken(LEFT_BRACKET));
        left = left.except_prefix(1);
        break;
      }
      case ')': {
        tokens->push_back(CalcToken(RIGHT_BRACKET));
        left = left.except_prefix(1);
        break;
      }
      case '"': {
        auto end = left.find_first_of('"', 1);
        if (end == left.npos) {
          // 文字列の終端がないときは失敗する．
          return false;
        }
        // 文字列の定数に対応するノードを作成する．
        String value = left.extract(1, end - 1);
        auto node = create_string_node(value);
        if (!node) {
          return false;
        }
        tokens->push_back(CalcToken(node));
        left = left.except_prefix(end + 1);
        break;
      }
      default: {
        auto end = left.find_first_of(" \t\r\n!=<>&|+-*/%()");
        if (end == left.npos) {
          end = left.size();
        }
        // FIXME: アドホックに書き足したのでひどいことになっている．
        // 最初に Boolean, Int64, Float の可能性を調べる．
        String token = left.prefix(end);
        CalcNode *node = nullptr;
        if (token == "TRUE") {
          node = create_boolean_node(true);
        } else if (token == "FALSE") {
          node = create_boolean_node(false);
        } else if (std::isdigit(static_cast<UInt8>(token[0]))) {
          if (token.find_first_of('.') != token.npos) {
            node = create_float_node(static_cast<Float>(std::stod(
                std::string(reinterpret_cast<const char *>(token.data()),
                            token.size()))));
          } else {
            node = create_int64_node(static_cast<Int64>(std::stol(
                std::string(reinterpret_cast<const char *>(token.data()),
                            token.size()))));
          }
        } else {
          // カラムと参照演算子に対応するノードを作成する．
          // 参照演算子は単項演算子より優先順位が高いため，以降の処理における面倒を
          // なくすべく，最後に作成したノードのみをトークン化する．
          const Table *current_table = table_;
          ColumnNode<Int64> *src_node = nullptr;
          for ( ; ; ) {
            auto delim_pos = token.find_first_of('.');
            auto column = current_table->get_column_by_name(
                (delim_pos != token.npos) ? token.prefix(delim_pos) : token);
            if (!column) {
              return false;
            }
            node = create_column_node(column);
            if (!node) {
              return false;
            }
            if (src_node) {
              node = create_binary_operator_node(REFERENCE_OPERATOR,
                                                 src_node, node);
              if (!node) {
                return false;
              }
            }
            if (delim_pos == token.npos) {
              // 参照演算子がなければここで終わる．
              break;
            }

            token = token.except_prefix(delim_pos + 1);
            if (column->data_type() != INTEGER) {
              return false;
            }
            src_node = static_cast<ColumnNode<Int64> *>(node);
            current_table = src_node->column()->dest_table();
          }
        }
        if (!node) {
          return false;
        }
        tokens->push_back(CalcToken(node));
        left = left.except_prefix(end);
        break;
      }
    }
  }
  return true;
}

// トークンをひとつずつ解釈する．
bool CalcImpl::push_token(const CalcToken &token,
                          std::vector<CalcToken> *stack) {
  switch (token.type()) {
    case BRACKET_TOKEN: {
      if (token.bracket_type() == LEFT_BRACKET) {
        // 開き括弧の直前にノードがあるのはおかしい．
        if (!stack->empty() && (stack->back().type() == NODE_TOKEN)) {
          return false;
        }
        stack->push_back(token);
      } else {
        // 閉じ括弧の直前にはノードが必要であり，それより前に開き括弧が必要である．
        if ((stack->size() < 2) || (stack->back().type() != NODE_TOKEN)) {
          return false;
        }
        // 括弧内にある演算子をすべて解決する．
        // 単項演算子は既に片付いているはずなので，二項演算子のみに注意すればよい．
        while (stack->size() >= 3) {
          CalcToken operator_token = (*stack)[stack->size() - 2];
          if (operator_token.type() != BINARY_OPERATOR_TOKEN) {
            break;
          }
          CalcToken lhs_token = (*stack)[stack->size() - 3];
          CalcToken rhs_token = (*stack)[stack->size() - 1];
          stack->resize(stack->size() - 3);

          // 演算に対応するノードに置き換える．
          auto node = create_binary_operator_node(
              operator_token.binary_operator_type(),
              lhs_token.node(), rhs_token.node());
          if (!node) {
            return false;
          }
          if (!push_token(CalcToken(node), stack)) {
            return false;
          }
        }
        // 括弧を取り除き，中身だけを残す．
        if ((stack->size() < 2) ||
            ((*stack)[stack->size() - 2].type() != BRACKET_TOKEN) ||
            ((*stack)[stack->size() - 2].bracket_type() != LEFT_BRACKET)) {
          return false;
        }
        CalcToken content_token = stack->back();
        stack->resize(stack->size() - 2);
        push_token(content_token, stack);
        break;
      }
      break;
    }
    case NODE_TOKEN: {
      if (stack->empty()) {
        // 最初のトークンであれば追加する．
        stack->push_back(token);
        break;
      }
      // ノードが連続するのはおかしい．
      if (stack->back().type() == NODE_TOKEN) {
        return false;
      }
      // 直前が単項演算子でなければ末尾に追加する．
      if (stack->back().type() != UNARY_OPERATOR_TOKEN) {
        stack->push_back(token);
        break;
      }
      // 直前の単項演算子を適用した結果に置き換える．
      auto node = create_unary_operator_node(
          LOGICAL_NOT_OPERATOR, token.node());
      if (!node) {
        return false;
      }
      stack->pop_back();
      // FIXME: 単項演算子が大量に連続していると，再帰呼び出しが深くなりすぎて
      //        スタックオーバーフローになる．
      if (!push_token(CalcToken(node), stack)) {
        return false;
      }
      break;
    }
    case UNARY_OPERATOR_TOKEN: {
      // 単項演算子の直前にオペランドがあるのはおかしい．
      if (!stack->empty() && (stack->back().type() == NODE_TOKEN)) {
        return false;
      }
      stack->push_back(token);
      break;
    }
    case BINARY_OPERATOR_TOKEN: {
      // 二項演算子の直前にはノードがなければならない．
      if (stack->empty() || (stack->back().type() != NODE_TOKEN)) {
        return false;
      }
      // 前方に優先度の高い二項演算子があるときは，それらを先に適用する．
      while (stack->size() >= 3) {
        CalcToken operator_token = (*stack)[stack->size() - 2];
        if ((operator_token.type() != BINARY_OPERATOR_TOKEN) ||
            (operator_token.priority() < token.priority())) {
          break;
        }
        CalcToken lhs_token = (*stack)[stack->size() - 3];
        CalcToken rhs_token = (*stack)[stack->size() - 1];
        stack->resize(stack->size() - 3);
        auto node = create_binary_operator_node(
            operator_token.binary_operator_type(),
            lhs_token.node(), rhs_token.node());
        if (!node) {
          return false;
        }
        if (!push_token(CalcToken(node), stack)) {
          return false;
        }
      }
      stack->push_back(token);
      break;
    }
  }
  return true;
}

// 指定された名前のカラムと対応するノードを作成する．
CalcNode *CalcImpl::create_column_node(Column *column) {
  std::unique_ptr<CalcNode> node;
  switch (column->data_type()) {
    case BOOLEAN: {
      node.reset(new ColumnNode<Boolean>(column));
      break;
    }
    case INTEGER: {
      node.reset(new ColumnNode<Int64>(column));
      break;
    }
    case FLOAT: {
      node.reset(new ColumnNode<Float>(column));
      break;
    }
    case STRING: {
      node.reset(new ColumnNode<String>(column));
      break;
    }
  }
  if (!node) {
    return nullptr;
  }
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された Boolean の定数と対応するノードを作成する．
CalcNode *CalcImpl::create_boolean_node(Boolean value) {
  std::unique_ptr<CalcNode> node(new ConstantNode<Boolean>(value));
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された Int64 の定数と対応するノードを作成する．
CalcNode *CalcImpl::create_int64_node(Int64 value) {
  std::unique_ptr<CalcNode> node(new ConstantNode<Int64>(value));
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された Float の定数と対応するノードを作成する．
CalcNode *CalcImpl::create_float_node(Float value) {
  std::unique_ptr<CalcNode> node(new ConstantNode<Float>(value));
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された String の定数と対応するノードを作成する．
CalcNode *CalcImpl::create_string_node(const String &value) {
  std::unique_ptr<CalcNode> node(new ConstantNode<String>(value));
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された単項演算子と対応するノードを作成する．
CalcNode *CalcImpl::create_unary_operator_node(
    UnaryOperatorType unary_operator_type, CalcNode *operand) {
  std::unique_ptr<CalcNode> node;
  switch (unary_operator_type) {
    case LOGICAL_NOT_OPERATOR: {
      node.reset(create_logical_not_operator_node(operand));
      break;
    }
  }
  if (!node) {
    return nullptr;
  }
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// LOGICAL_NOT と対応するノードを作成する．
CalcNode *CalcImpl::create_logical_not_operator_node(CalcNode *operand) {
  if (operand->data_type() != BOOLEAN) {
    return nullptr;
  }
  switch (operand->type()) {
    case CONSTANT_NODE: {
      auto value = !static_cast<ConstantNode<Boolean> *>(operand)->get(0, 0);
      return new ConstantNode<Boolean>(value);
    }
    case COLUMN_NODE: {
      return new LogicalNotNode<ColumnNode<Boolean>>(operand);
    }
    case OPERATOR_NODE: {
      return new LogicalNotNode<OperatorNode<Boolean>>(operand);
    }
  }
  return nullptr;
}

// 指定された二項演算子と対応するノードを作成する．
CalcNode *CalcImpl::create_binary_operator_node(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  std::unique_ptr<CalcNode> node;
  switch (binary_operator_type) {
    case EQUAL_OPERATOR:
    case NOT_EQUAL_OPERATOR:
    case LESS_OPERATOR:
    case LESS_EQUAL_OPERATOR:
    case GREATER_OPERATOR:
    case GREATER_EQUAL_OPERATOR: {
      node.reset(create_comparer_node(binary_operator_type, lhs, rhs));
      break;
    }
    case LOGICAL_AND_OPERATOR: {
      node.reset(create_logical_and_node(lhs, rhs));
      break;
    }
    case LOGICAL_OR_OPERATOR: {
      node.reset(create_logical_or_node(lhs, rhs));
      break;
    }
    case PLUS_OPERATOR:
    case MINUS_OPERATOR:
    case MULTIPLIES_OPERATOR:
    case DIVIDES_OPERATOR:
    case MODULUS_OPERATOR: {
      node.reset(create_arithmetic_node(binary_operator_type, lhs, rhs));
      break;
    }
    case REFERENCE_OPERATOR: {
      node.reset(create_reference_node(lhs, rhs));
      break;
    }
  }
  if (!node) {
    return nullptr;
  }
  nodes_.push_back(std::move(node));
  return nodes_.back().get();
}

// 指定された比較演算子と対応するノードを作成する．
CalcNode *CalcImpl::create_comparer_node(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  // 同じ型同士と整数同士のみを許す．
  switch (lhs->data_type()) {
    case BOOLEAN: {
      if (rhs->data_type() != BOOLEAN) {
        return nullptr;
      }
      return create_comparer_node_2<Boolean, Boolean, Boolean>(
          binary_operator_type, lhs, rhs);
    }
    case INTEGER: {
      if (rhs->data_type() != INTEGER) {
        return nullptr;
      }
      return create_comparer_node_2<Int64, Int64, Int64>(
          binary_operator_type, lhs, rhs);
    }
    case FLOAT: {
      if (rhs->data_type() != FLOAT) {
        return nullptr;
      }
      return create_comparer_node_2<Float, Float, Float>(
          binary_operator_type, lhs, rhs);
    }
    case STRING: {
      if (rhs->data_type() != STRING) {
        return nullptr;
      }
      return create_comparer_node_2<String, String, String>(
          binary_operator_type, lhs, rhs);
    }
  }
  return nullptr;
}

// 指定された比較演算子と対応するノードを作成する．
// T = 比較に用いる型， U = lhs のデータ型， V = rhs のデータ型．
template <typename T, typename U, typename V>
CalcNode *CalcImpl::create_comparer_node_2(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  switch (binary_operator_type) {
    case EQUAL_OPERATOR: {
      return create_comparer_node_3<EqualOperator<T>, U, V>(lhs, rhs);
    }
    case NOT_EQUAL_OPERATOR: {
      return create_comparer_node_3<NotEqualOperator<T>, U, V>(lhs, rhs);
    }
    case LESS_OPERATOR: {
      return create_comparer_node_3<LessOperator<T>, U, V>(lhs, rhs);
    }
    case LESS_EQUAL_OPERATOR: {
      return create_comparer_node_3<LessEqualOperator<T>, U, V>(lhs, rhs);
    }
    case GREATER_OPERATOR: {
      return create_comparer_node_3<GreaterOperator<T>, U, V>(lhs, rhs);
    }
    case GREATER_EQUAL_OPERATOR: {
      return create_comparer_node_3<GreaterEqualOperator<T>, U, V>(lhs, rhs);
    }
    default: {
      return nullptr;
    }
  }
}

// 指定された比較演算子と対応するノードを作成する．
// T = 比較演算子， U = lhs のデータ型， V = rhs のデータ型．
template <typename T, typename U, typename V>
CalcNode *CalcImpl::create_comparer_node_3(
    CalcNode *lhs, CalcNode *rhs) {
  switch (lhs->type()) {
    case CONSTANT_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          auto lhs_value = static_cast<ConstantNode<U> *>(lhs)->get(0, 0);
          auto rhs_value = static_cast<ConstantNode<V> *>(rhs)->get(0, 0);
          return new ConstantNode<Boolean>(T()(lhs_value, rhs_value));
        }
        case COLUMN_NODE: {
          return new ComparerNode<T, ConstantNode<U>, ColumnNode<V>>(
              lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new ComparerNode<T, ConstantNode<U>, OperatorNode<V>>(
              lhs, rhs);
        }
      }
      break;
    }
    case COLUMN_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new ComparerNode<T, ColumnNode<U>, ConstantNode<V>>(
              lhs, rhs);
        }
        case COLUMN_NODE: {
          return new ComparerNode<T, ColumnNode<U>, ColumnNode<V>>(
              lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new ComparerNode<T, ColumnNode<U>, OperatorNode<V>>(
              lhs, rhs);
        }
      }
      break;
    }
    case OPERATOR_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new ComparerNode<T, OperatorNode<U>, ConstantNode<V>>(
              lhs, rhs);
        }
        case COLUMN_NODE: {
          return new ComparerNode<T, OperatorNode<U>, ColumnNode<V>>(
              lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new ComparerNode<T, OperatorNode<U>, OperatorNode<V>>(
              lhs, rhs);
        }
      }
    }
  }
  return nullptr;
}

// LOGICAL_AND と対応するノードを作成する．
CalcNode *CalcImpl::create_logical_and_node(CalcNode *lhs, CalcNode *rhs) {
  if ((lhs->data_type() != BOOLEAN) || (rhs->data_type() != BOOLEAN)) {
    return nullptr;
  }
  using ConstantNodeB = ConstantNode<Boolean>;
  using ColumnNodeB = ColumnNode<Boolean>;
  using OperatorNodeB = OperatorNode<Boolean>;
  switch (lhs->type()) {
    case CONSTANT_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          auto lhs_value = static_cast<ConstantNodeB *>(lhs)->get(0, 0);
          auto rhs_value = static_cast<ConstantNodeB *>(rhs)->get(0, 0);
          return new ConstantNode<Boolean>(lhs_value && rhs_value);
        }
        case COLUMN_NODE: {
          return new LogicalAndNode<ConstantNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalAndNode<ConstantNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
      break;
    }
    case COLUMN_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new LogicalAndNode<ColumnNodeB, ConstantNodeB>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return new LogicalAndNode<ColumnNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalAndNode<ColumnNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
      break;
    }
    case OPERATOR_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new LogicalAndNode<OperatorNodeB, ConstantNodeB>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return new LogicalAndNode<OperatorNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalAndNode<OperatorNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
    }
  }
  return nullptr;
}

// LOGICAL_OR と対応するノードを作成する．
CalcNode *CalcImpl::create_logical_or_node(CalcNode *lhs, CalcNode *rhs) {
  if ((lhs->data_type() != BOOLEAN) || (rhs->data_type() != BOOLEAN)) {
    return nullptr;
  }
  using ConstantNodeB = ConstantNode<Boolean>;
  using ColumnNodeB = ColumnNode<Boolean>;
  using OperatorNodeB = OperatorNode<Boolean>;
  switch (lhs->type()) {
    case CONSTANT_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          auto lhs_value = static_cast<ConstantNodeB *>(lhs)->get(0, 0);
          auto rhs_value = static_cast<ConstantNodeB *>(rhs)->get(0, 0);
          return new ConstantNode<Boolean>(lhs_value || rhs_value);
        }
        case COLUMN_NODE: {
          return new LogicalOrNode<ConstantNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalOrNode<ConstantNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
      break;
    }
    case COLUMN_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new LogicalOrNode<ColumnNodeB, ConstantNodeB>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return new LogicalOrNode<ColumnNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalOrNode<ColumnNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
      break;
    }
    case OPERATOR_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return new LogicalOrNode<OperatorNodeB, ConstantNodeB>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return new LogicalOrNode<OperatorNodeB, ColumnNodeB>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return new LogicalOrNode<OperatorNodeB, OperatorNodeB>(lhs, rhs);
        }
      }
    }
  }
  return nullptr;
}

// 指定された算術演算子と対応するノードを作成する．
CalcNode *CalcImpl::create_arithmetic_node(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  // 整数同士およびに浮動小数点数同士のみを許す．
  switch (lhs->data_type()) {
    case BOOLEAN: {
      return create_arithmetic_node_2<Boolean>(binary_operator_type, lhs, rhs);
    }
    case INTEGER: {
      return create_arithmetic_node_2<Int64>(binary_operator_type, lhs, rhs);
    }
    case FLOAT: {
      return create_arithmetic_node_2<Float>(binary_operator_type, lhs, rhs);
    }
    case STRING: {
      return create_arithmetic_node_2<String>(binary_operator_type, lhs, rhs);
    }
  }
  return nullptr;
}

// 指定された算術演算子と対応するノードを作成する．
// T: lhs のデータ型．
template <typename T>
CalcNode *CalcImpl::create_arithmetic_node_2(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  // 整数同士およびに浮動小数点数同士のみを許す．
  switch (rhs->data_type()) {
    case BOOLEAN: {
      return create_arithmetic_node_3<T, Boolean>(
          binary_operator_type, lhs, rhs);
    }
    case INTEGER: {
      return create_arithmetic_node_3<T, Int64>(
          binary_operator_type, lhs, rhs);
    }
    case FLOAT: {
      return create_arithmetic_node_3<T, Float>(
          binary_operator_type, lhs, rhs);
    }
    case STRING: {
      return create_arithmetic_node_3<T, String>(
          binary_operator_type, lhs, rhs);
    }
  }
  return nullptr;
}

// 指定された算術演算子と対応するノードを作成する．
// T: lhs のデータ型， U: rhs のデータ型．
template <typename T, typename U>
CalcNode *CalcImpl::create_arithmetic_node_3(
    BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs) {
  switch (binary_operator_type) {
    case PLUS_OPERATOR: {
      return create_arithmetic_node_4<PlusOperator<T, U>>(lhs, rhs);
    }
    case MINUS_OPERATOR: {
      return create_arithmetic_node_4<MinusOperator<T, U>>(lhs, rhs);
    }
    case MULTIPLIES_OPERATOR: {
      return create_arithmetic_node_4<MultipliesOperator<T, U>>(lhs, rhs);
    }
    case DIVIDES_OPERATOR: {
      return create_arithmetic_node_4<DividesOperator<T, U>>(lhs, rhs);
    }
    case MODULUS_OPERATOR: {
      return create_arithmetic_node_4<ModulusOperator<T, U>>(lhs, rhs);
    }
    default: {
      return nullptr;
    }
  }
}

// 指定された算術演算子と対応するノードを作成する．
// T: 算術演算子．
template <typename T>
CalcNode *CalcImpl::create_arithmetic_node_4(CalcNode *lhs, CalcNode *rhs) {
//  using Result = typename T::Result;
  using Lhs = typename T::Lhs;
  using Rhs = typename T::Rhs;
  switch (lhs->type()) {
    case CONSTANT_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          auto lhs_value = static_cast<ConstantNode<Lhs> *>(lhs)->get(0, 0);
          auto rhs_value = static_cast<ConstantNode<Rhs> *>(rhs)->get(0, 0);
          return ArithmeticNodeHelper<T>::template create(
              lhs_value, rhs_value);
        }
        case COLUMN_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              ConstantNode<Lhs>, ColumnNode<Rhs>>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              ConstantNode<Lhs>, OperatorNode<Rhs>>(lhs, rhs);
        }
      }
      break;
    }
    case COLUMN_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              ColumnNode<Lhs>, ConstantNode<Rhs>>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              ColumnNode<Lhs>, ColumnNode<Rhs>>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              ColumnNode<Lhs>, OperatorNode<Rhs>>(lhs, rhs);
        }
      }
      break;
    }
    case OPERATOR_NODE: {
      switch (rhs->type()) {
        case CONSTANT_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              OperatorNode<Lhs>, ConstantNode<Rhs>>(lhs, rhs);
        }
        case COLUMN_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              OperatorNode<Lhs>, ColumnNode<Rhs>>(lhs, rhs);
        }
        case OPERATOR_NODE: {
          return ArithmeticNodeHelper<T>::template create<
              OperatorNode<Lhs>, OperatorNode<Rhs>>(lhs, rhs);
        }
      }
    }
  }
  return nullptr;
}

// 参照演算子と対応するノードを作成する．
CalcNode *CalcImpl::create_reference_node(CalcNode *lhs, CalcNode *rhs) {
  // 左の被演算子は参照型のカラムでなければならない．
  if ((lhs->data_type() != INTEGER) ||
      (lhs->type() != COLUMN_NODE) ||
      !static_cast<ColumnNode<Int64> *>(lhs)->column()->dest_table()) {
    return nullptr;
  }
  // 右の被演算子もカラムでなければならない．
  if (rhs->type() != COLUMN_NODE) {
    return nullptr;
  }
  // 右の被演算子の型に応じたノードを作成する．
  switch (rhs->data_type()) {
    case BOOLEAN: {
      return new ReferenceNode<Boolean>(lhs, rhs);
    }
    case INTEGER: {
      return new ReferenceNode<Int64>(lhs, rhs);
    }
    case FLOAT: {
      return new ReferenceNode<Float>(lhs, rhs);
    }
    case STRING: {
      return new ReferenceNode<String>(lhs, rhs);
    }
  }
  return nullptr;
}

}  // namespace grnxx
