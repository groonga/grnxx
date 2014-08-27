#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace expression {

class Node;

}  // namespace expression

enum OperatorType {
  // -- Unary operators --

  LOGICAL_NOT_OPERATOR,  // For Bool.

  BITWISE_NOT_OPERATOR,  // For Bool, Int.

  POSITIVE_OPERATOR,  // For Int, Float.
  NEGATIVE_OPERATOR,  // For Int, Float.

  // Typecast operators.
//  TO_BOOL_OPERATOR,
  TO_INT_OPERATOR,
  TO_FLOAT_OPERATOR,
//  TO_GEO_POINT_OPERATOR,
//  TO_TEXT_OPERATOR,

  // -- Binary operators --

  // Logical operators.
  LOGICAL_AND_OPERATOR,  // For Bool.
  LOGICAL_OR_OPERATOR,   // For Bool.

  // Equality operators.
  EQUAL_OPERATOR,      // For any types.
  NOT_EQUAL_OPERATOR,  // For any types.

  // Comparison operators.
  LESS_OPERATOR,           // Int, Float, Text.
  LESS_EQUAL_OPERATOR,     // Int, Float, Text.
  GREATER_OPERATOR,        // Int, Float, Text.
  GREATER_EQUAL_OPERATOR,  // Int, Float, Text.

  // Bitwise operators.
  BITWISE_AND_OPERATOR,  // For Bool, Int.
  BITWISE_OR_OPERATOR,   // For Bool, Int.
  BITWISE_XOR_OPERATOR,  // For Bool, Int.

  // Arithmetic operators.
  PLUS_OPERATOR,            // For Int, Float.
  MINUS_OPERATOR,           // For Int, Float.
  MULTIPLICATION_OPERATOR,  // For Int, Float.
  DIVISION_OPERATOR,        // For Int, Float.
  MODULUS_OPERATOR,         // For Int.

  // Array operators.
//  SUBSCRIPT_OPERATOR,

  // -- Ternary operators --
};

class Expression {
 public:
  using Node = expression::Node;

  ~Expression();

  // Return the associated table.
  const Table *table() const {
    return table_;
  }
  // Return the result data type.
  DataType data_type() const;
  // Return the evaluation block size.
  Int block_size() const {
    return block_size_;
  }

  // TODO: The following interface is not yet fixed.

//  // Filter out false records.
//  //
//  // Evaluates the expression for "*records" and removes records whose
//  // evaluation results are false.
//  // Note that the first "offset" records are left without evaluation.
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  bool filter(Error *error, Array<Record> *records, Int offset = 0);

  // Filter out false records.
  //
  // Evaluates the expression for "*records" and removes records whose
  // evaluation results are false.
  //
  // Note that the first "input_offset" records in "*records" are left as is
  // without evaluation.
  // Also note that the first "output_offset" true records are removed and
  // the number of output records is at most "output_limit".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool filter(Error *error, Array<Record> *records,
              Int input_offset = 0,
              Int output_offset = 0,
              Int output_limit = numeric_limits<Int>::max());

  // Extract true records.
  //
  // Evaluates the expression for "input_records" and copies records whose
  // evaluation results are true into "*output_records".
  // "*output_records" is truncated to fit the number of extracted records.
  //
  // Fails if "output_records->size()" is less than "input_records.size()".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);

  // Adjust scores of records.
  //
  // Evaluates the expression for "*records" and replaces the scores with
  // the evaluation results.
  // Note that the first "offset" records are not modified.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool adjust(Error *error, Array<Record> *records, Int offset = 0);

  // Adjust scores of records.
  //
  // Evaluates the expression for "records" and replaces the scores with
  // the evaluation results.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool adjust(Error *error, ArrayRef<Record> records);

  // Evaluate the expression.
  //
  // Evaluates the expression for "records" and stores the results into
  // "*results".
  //
  // Fails if "T" is different from the result data type.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  template <typename T>
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                Array<T> *results);

  // Evaluate the expression.
  //
  // Evaluates the expression for "records" and stores the results into
  // "*results".
  //
  // Fails if "T" is different from the result data type.
  // Fails if "results.size()" != "records.size()".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  template <typename T>
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<T> results);

 private:
  const Table *table_;
  unique_ptr<Node> root_;
  Int block_size_;

  // Create an expression.
  //
  // On success, returns a poitner to the expression.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Expression> create(Error *error,
                                       const Table *table,
                                       unique_ptr<Node> &&root,
                                       const ExpressionOptions &options);

  Expression(const Table *table,
             unique_ptr<Node> &&root,
             Int block_size);

  friend ExpressionBuilder;
};

class ExpressionBuilder {
 public:
  using Node = expression::Node;

  // Create an object for building expressons.
  //
  // On success, returns a poitner to the builder.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<ExpressionBuilder> create(Error *error,
                                              const Table *table);

  ~ExpressionBuilder();

  // Return the target table.
  const Table *table() const {
    return table_;
  }

  // Push a datum.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_datum(Error *error, const Datum &datum);

  // Push a column.
  //
  // If "name" == "_id", pushes a pseudo column associated with row IDs.
  // If "name" == "_score", pushes a pseudo column associated with scores.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_column(Error *error, String name);

  // Push an operator.
  //
  // Pops operands and pushes an operator.
  // Fails if there are not enough operands.
  // Fails if the combination of operands is invalid.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_operator(Error *error, OperatorType operator_type);

  // Clear the internal stack.
  void clear();

  // Complete building an expression and clear the internal stack.
  //
  // Fails if the stack is empty or contains more than one nodes.
  //
  // On success, returns a pointer to the expression.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<Expression> release(
      Error *error,
      const ExpressionOptions &options = ExpressionOptions());

 private:
  const Table *table_;
  Array<unique_ptr<Node>> stack_;

  explicit ExpressionBuilder(const Table *table);

  // Create a node associated with a constant.
  unique_ptr<Node> create_datum_node(Error *error, const Datum &datum);
  // Create a node associated with a column.
  unique_ptr<Node> create_column_node(Error *error, String name);

  // Push a unary operator.
  bool push_unary_operator(Error *error, OperatorType operator_type);
  // Push a binary operator.
  bool push_binary_operator(Error *error, OperatorType operator_type);

  // Create a node associated with a unary operator.
  unique_ptr<Node> create_unary_node(
      Error *error,
      OperatorType operator_type,
      unique_ptr<Node> &&arg);
  // Create a node associated with a binary operator.
  unique_ptr<Node> create_binary_node(
      Error *error,
      OperatorType operator_type,
      unique_ptr<Node> &&arg1,
      unique_ptr<Node> &&arg2);

  // Create a equality test node.
  template <typename T>
  unique_ptr<Node> create_equality_test_node(
      Error *error,
      unique_ptr<Node> &&arg1,
      unique_ptr<Node> &&arg2);
  // Create a comparison node.
  template <typename T>
  unique_ptr<Node> create_comparison_node(
      Error *error,
      unique_ptr<Node> &&arg1,
      unique_ptr<Node> &&arg2);
  // Create a bitwise node.
  template <typename T>
  unique_ptr<Node> create_bitwise_node(
      Error *error,
      OperatorType operator_type,
      unique_ptr<Node> &&arg1,
      unique_ptr<Node> &&arg2);
  // Create an arithmetic node.
  template <typename T>
  unique_ptr<Node> create_arithmetic_node(
      Error *error,
      OperatorType operator_type,
      unique_ptr<Node> &&arg1,
      unique_ptr<Node> &&arg2);
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
