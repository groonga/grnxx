#ifndef GRNXX_IMPL_EXPRESSION_HPP
#define GRNXX_IMPL_EXPRESSION_HPP

#include "grnxx/expression.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/impl/column.hpp"

namespace grnxx {
namespace impl {
namespace expression {

class Node;
class Builder;

}  // namespace expression

using ExpressionInterface = grnxx::Expression;
using ExpressionBuilderInterface = grnxx::ExpressionBuilder;

class Expression : public ExpressionInterface {
 public:
  using Node = expression::Node;

  // -- Public API (grnxx/expression.hpp) --

  Expression(const Table *table,
             std::unique_ptr<Node> &&root,
             const ExpressionOptions &options);
  ~Expression();

  const Table *table() const {
    return table_;
  }
  DataType data_type() const;
  bool is_row_id() const;
  bool is_score() const;
  size_t block_size() const {
    return block_size_;
  }

  void filter(Array<Record> *records,
              size_t input_offset,
              size_t output_offset,
              size_t output_limit);
  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records,
              size_t offset,
              size_t limit);

  void adjust(Array<Record> *records, size_t offset);
  void adjust(ArrayRef<Record> records);

  void evaluate(ArrayCRef<Record> records, Array<Bool> *results);
  void evaluate(ArrayCRef<Record> records, Array<Int> *results);
  void evaluate(ArrayCRef<Record> records, Array<Float> *results);
  void evaluate(ArrayCRef<Record> records, Array<GeoPoint> *results);
  void evaluate(ArrayCRef<Record> records, Array<Text> *results);
  void evaluate(ArrayCRef<Record> records, Array<Vector<Bool>> *results);
  void evaluate(ArrayCRef<Record> records, Array<Vector<Int>> *results);
  void evaluate(ArrayCRef<Record> records, Array<Vector<Float>> *results);
  void evaluate(ArrayCRef<Record> records, Array<Vector<GeoPoint>> *results);
  void evaluate(ArrayCRef<Record> records, Array<Vector<Text>> *results);

  void evaluate(ArrayCRef<Record> records, ArrayRef<Bool> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Int> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Float> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<GeoPoint> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Text> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Vector<Bool>> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Vector<Int>> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Vector<Float>> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Vector<GeoPoint>> results);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Vector<Text>> results);

 private:
  const Table *table_;
  std::unique_ptr<Node> root_;
  size_t block_size_;

  template <typename T>
  void _evaluate(ArrayCRef<Record> records, Array<T> *results);
  template <typename T>
  void _evaluate(ArrayCRef<Record> records, ArrayRef<T> results);
};

class ExpressionBuilder : public ExpressionBuilderInterface {
 public:
  using Node = expression::Node;

  // -- Public API (grnxx/expression.hpp) --

  explicit ExpressionBuilder(const Table *table);
  ~ExpressionBuilder();

  const Table *table() const {
    return table_;
  }

  void push_constant(const Datum &datum);
  void push_row_id();
  void push_score();
  void push_column(const String &name);
  void push_operator(OperatorType operator_type);

  void begin_subexpression();
  void end_subexpression(const ExpressionOptions &options);

  void clear();

  std::unique_ptr<ExpressionInterface> release(
      const ExpressionOptions &options = ExpressionOptions());

 private:
  const Table *table_;
  Array<std::unique_ptr<Node>> node_stack_;
  std::unique_ptr<ExpressionBuilder> subexpression_builder_;

  // Push a node associated with a unary operator.
  //
  // On failure, throws an exception.
  void push_unary_operator(OperatorType operator_type);

  // Push a node associated with a binary operator.
  //
  // On failure, throws an exception.
  void push_binary_operator(OperatorType operator_type);

  // Push a node associated with the dereference operator.
  //
  // On failure, throws an exception.
  void push_dereference(const ExpressionOptions &options);

  // Create a node associated with a constant.
  //
  // On failure, throws an exception.
  static Node *create_constant_node(const Datum &datum);

  // Create a node associated with a column.
  //
  // On failure, throws an exception.
  static Node *create_column_node(ColumnBase *column);

  // Create a node associated with a unary operator.
  //
  // On failure, throws an exception.
  static Node *create_unary_node(OperatorType operator_type,
                                 std::unique_ptr<Node> &&arg);

  // Create a node associated with a binary operator.
  //
  // On failure, throws an exception.
  static Node *create_binary_node(OperatorType operator_type,
                                  std::unique_ptr<Node> &&arg1,
                                  std::unique_ptr<Node> &&arg2);

  // Create a node associated with an equality test operator.
  //
  // On failure, throws an exception.
  template <typename T>
  static Node *create_equality_test_node(OperatorType operator_type,
                                         std::unique_ptr<Node> &&arg1,
                                         std::unique_ptr<Node> &&arg2);

  // Create a node associated with a comparison operator.
  //
  // On failure, throws an exception.
  template <typename T>
  static Node *create_comparison_node(OperatorType operator_type,
                                      std::unique_ptr<Node> &&arg1,
                                      std::unique_ptr<Node> &&arg2);

  // Create a node associated with a bitwise binary operator.
  //
  // On failure, throws an exception.
  template <typename T>
  static Node *create_bitwise_binary_node(OperatorType operator_type,
                                          std::unique_ptr<Node> &&arg1,
                                          std::unique_ptr<Node> &&arg2);

  // Create a node associated with an arithmetic operator.
  //
  // On failure, throws an exception.
  template <typename T>
  static Node *create_arithmetic_node(OperatorType operator_type,
                                      std::unique_ptr<Node> &&arg1,
                                      std::unique_ptr<Node> &&arg2);

  // Create a node associated with a search operator.
  //
  // On failure, throws an exception.
  template <typename T>
  static Node *create_search_node(OperatorType operator_type,
                                  std::unique_ptr<Node> &&arg1,
                                  std::unique_ptr<Node> &&arg2);

  // Create a node associated with a subscript operator.
  //
  // On failure, throws an exception.
  static Node *create_subscript_node(std::unique_ptr<Node> &&arg1,
                                     std::unique_ptr<Node> &&arg2);

  // Create a node associated with a dereference operator.
  //
  // On failure, throws an exception.
  static Node *create_dereference_node(std::unique_ptr<Node> &&arg1,
                                       std::unique_ptr<Node> &&arg2,
                                       const ExpressionOptions &options);
};

enum ExpressionTokenType {
  DUMMY_TOKEN,
  CONSTANT_TOKEN,
  NAME_TOKEN,
  UNARY_OPERATOR_TOKEN,
  BINARY_OPERATOR_TOKEN,
  DEREFERENCE_TOKEN,
  BRACKET_TOKEN
};

enum ExpressionBracketType {
  LEFT_ROUND_BRACKET,
  RIGHT_ROUND_BRACKET,
  LEFT_SQUARE_BRACKET,
  RIGHT_SQUARE_BRACKET
};

class ExpressionToken {
 public:
  ExpressionToken() : string_(), type_(DUMMY_TOKEN), dummy_(0), priority_(0) {}
  explicit ExpressionToken(const String &string,
                           ExpressionTokenType token_type)
      : string_(string),
        type_(token_type),
        dummy_(0),
        priority_(0) {}
  explicit ExpressionToken(const String &string,
                           ExpressionBracketType bracket_type)
      : string_(string),
        type_(BRACKET_TOKEN),
        bracket_type_(bracket_type),
        priority_(0) {}
  explicit ExpressionToken(const String &string,
                           OperatorType operator_type)
      : string_(string),
        type_(get_operator_token_type(operator_type)),
        operator_type_(operator_type),
        priority_(get_operator_priority(operator_type)) {}

  const String &string() const {
    return string_;
  }
  ExpressionTokenType type() const {
    return type_;
  }
  ExpressionBracketType bracket_type() const {
    return bracket_type_;
  }
  OperatorType operator_type() const {
    return operator_type_;
  }
  int priority() const {
    return priority_;
  }

 private:
  String string_;
  ExpressionTokenType type_;
  union {
    int dummy_;
    ExpressionBracketType bracket_type_;
    OperatorType operator_type_;
  };
  int priority_;

  static ExpressionTokenType get_operator_token_type(
      OperatorType operator_type);
  static int get_operator_priority(OperatorType operator_type);
};

class ExpressionParser {
 public:
  static std::unique_ptr<ExpressionInterface> parse(
      const TableInterface *table,
      const String &query);

  ~ExpressionParser() = default;

 private:
  const TableInterface *table_;
  Array<ExpressionToken> tokens_;
  Array<ExpressionToken> stack_;
  std::unique_ptr<ExpressionBuilderInterface> builder_;

  explicit ExpressionParser(const TableInterface *table);

  void tokenize(const String &query);
  void analyze();
  void push_token(const ExpressionToken &token);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_EXPRESSION_HPP
