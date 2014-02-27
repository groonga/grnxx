#ifndef GRNXX_CALC_IMPL_HPP
#define GRNXX_CALC_IMPL_HPP

#include "grnxx/calc.hpp"

namespace grnxx {

// 単項演算子の種類．
enum UnaryOperatorType {
  LOGICAL_NOT_OPERATOR
};

// 二項演算子の種類．
enum BinaryOperatorType {
  EQUAL_OPERATOR,
  NOT_EQUAL_OPERATOR,
  LESS_OPERATOR,
  LESS_EQUAL_OPERATOR,
  GREATER_OPERATOR,
  GREATER_EQUAL_OPERATOR,
  LOGICAL_AND_OPERATOR,
  LOGICAL_OR_OPERATOR,
  PLUS_OPERATOR,
  MINUS_OPERATOR,
  MULTIPLIES_OPERATOR,
  DIVIDES_OPERATOR,
  MODULUS_OPERATOR
};

// 演算器を構成するノードの種類．
enum CalcNodeType {
  CONSTANT_NODE,
  COLUMN_NODE,
  OPERATOR_NODE
};

// 演算器を構成するノード．
class CalcNode {
 public:
  // 指定された種類のノードとして初期化する．
  CalcNode(CalcNodeType type, DataType data_type);
  // ノードを破棄する．
  virtual ~CalcNode();

  // ノードの種類を返す．
  CalcNodeType type() const {
    return type_;
  }
  // データ型を返す．
  DataType data_type() const {
    return data_type_;
  }

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  virtual Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 与えられた行の一覧について演算をおこない，その結果を取得できる状態にする．
  virtual void fill(const RowID *row_ids, Int64 num_row_ids);

 protected:
  CalcNodeType type_;
  DataType data_type_;
};

// 括弧の種類．
enum CalcBracketType {
  LEFT_BRACKET,
  RIGHT_BRACKET
};

// クエリを構成するトークンの種類．
enum CalcTokenType {
  BRACKET_TOKEN,
  NODE_TOKEN,
  UNARY_OPERATOR_TOKEN,
  BINARY_OPERATOR_TOKEN
};

// クエリを構成するトークン．
class CalcToken {
 public:
  // std::vector<CalcToken>::resize() を使えるようにする．
  CalcToken()
      : type_(NODE_TOKEN),
        node_(nullptr),
        priority_(0) {}
  // 括弧に対応するトークンを作成する．
  explicit CalcToken(CalcBracketType bracket_type)
      : type_(BRACKET_TOKEN),
        bracket_type_(bracket_type),
        priority_(0) {}
  // ノードに対応するトークンを作成する．
  explicit CalcToken(CalcNode *node)
      : type_(NODE_TOKEN),
        node_(node),
        priority_(0) {}
  // 単項演算子に対応するトークンを作成する．
  explicit CalcToken(UnaryOperatorType unary_operator_type)
      : type_(UNARY_OPERATOR_TOKEN),
        unary_operator_type_(unary_operator_type),
        priority_(0) {}
  // 二項演算子に対応するトークンを作成する．
  explicit CalcToken(BinaryOperatorType binary_operator_type)
      : type_(BINARY_OPERATOR_TOKEN),
        binary_operator_type_(binary_operator_type),
        priority_(get_binary_operator_priority(binary_operator_type)) {}

  // トークンの種類を返す．
  CalcTokenType type() const {
    return type_;
  }
  // 対応する括弧の種類を返す．
  CalcBracketType bracket_type() const {
    return bracket_type_;
  }
  // 対応するノードを返す．
  CalcNode *node() const {
    return node_;
  }
  // 対応する単項演算子の種類を返す．
  UnaryOperatorType unary_operator_type() const {
    return unary_operator_type_;
  }
  // 対応する二項演算子の種類を返す．
  BinaryOperatorType binary_operator_type() const {
    return binary_operator_type_;
  }
  // 対応する二項演算子の優先度を返す．
  int priority() const {
    return priority_;
  }

 private:
  CalcTokenType type_;
  union {
    CalcBracketType bracket_type_;
    CalcNode *node_;
    UnaryOperatorType unary_operator_type_;
    BinaryOperatorType binary_operator_type_;
  };
  int priority_;

  // 二項演算子の優先度を返す．
  static int get_binary_operator_priority(BinaryOperatorType operator_type);
};

class CalcImpl : public Calc {
 public:
  CalcImpl();
  ~CalcImpl();

  // 指定された文字列に対応する演算器を作成する．
  bool parse(const Table *table, const String &query);

  // 行の一覧を受け取り，演算結果が真になる行のみを残して，残った行の数を返す．
  Int64 filter(RowID *row_ids, Int64 num_row_ids);

  // 演算が何も指定されていなければ true を返し，そうでなければ false を返す．
  bool empty() const;

 private:
  const Table *table_;
  std::vector<std::unique_ptr<CalcNode>> nodes_;

  // クエリをトークンに分割する．
  bool tokenize_query(const String &query, std::vector<CalcToken> *tokens);
  // トークンをひとつずつ解釈する．
  bool push_token(const CalcToken &token, std::vector<CalcToken> *stack);

  // 指定された名前のカラムと対応するノードを作成する．
  CalcNode *create_column_node(const String &column_name);

  // 指定された Boolean の定数と対応するノードを作成する．
  CalcNode *create_boolean_node(Boolean value);
  // 指定された Int64 の定数と対応するノードを作成する．
  CalcNode *create_int64_node(Int64 value);
  // 指定された Float の定数と対応するノードを作成する．
  CalcNode *create_float_node(Float value);
  // 指定された String の定数と対応するノードを作成する．
  CalcNode *create_string_node(const String &value);

  // 指定された単項演算子と対応するノードを作成する．
  CalcNode *create_unary_operator_node(UnaryOperatorType unary_operator_type,
                                       CalcNode *operand);

  // LOGICAL_NOT と対応するノードを作成する．
  CalcNode *create_logical_not_operator_node(CalcNode *operand);

  // 指定された二項演算子と対応するノードを作成する．
  CalcNode *create_binary_operator_node(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);

  // 指定された比較演算子と対応するノードを作成する．
  CalcNode *create_comparer_node(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);
  // 指定された比較演算子と対応するノードを作成する．
  // T = 比較に用いる型， U = lhs のデータ型， V = rhs のデータ型．
  template <typename T, typename U, typename V>
  CalcNode *create_comparer_node_2(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);
  // 指定された比較演算子と対応するノードを作成する．
  // T = 比較演算子， U = lhs のデータ型， V = rhs のデータ型．
  template <typename T, typename U, typename V>
  CalcNode *create_comparer_node_3(CalcNode *lhs, CalcNode *rhs);

  // LOGICAL_AND と対応するノードを作成する．
  CalcNode *create_logical_and_node(CalcNode *lhs, CalcNode *rhs);

  // LOGICAL_OR と対応するノードを作成する．
  CalcNode *create_logical_or_node(CalcNode *lhs, CalcNode *rhs);

  // 指定された算術演算子と対応するノードを作成する．
  CalcNode *create_arithmetic_node(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);
  // 指定された算術演算子と対応するノードを作成する．
  // T: lhs のデータ型．
  template <typename T>
  CalcNode *create_arithmetic_node_2(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);
  // 指定された算術演算子と対応するノードを作成する．
  // T: lhs のデータ型， U: rhs のデータ型．
  template <typename T, typename U>
  CalcNode *create_arithmetic_node_3(
      BinaryOperatorType binary_operator_type, CalcNode *lhs, CalcNode *rhs);
  // 指定された算術演算子と対応するノードを作成する．
  // T: 算術演算子．
  template <typename T>
  CalcNode *create_arithmetic_node_4(CalcNode *lhs, CalcNode *rhs);
};

}  // namespace grnxx

#endif  // GRNXX_CALC_IMPL_HPP
