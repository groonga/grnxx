#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

enum OperatorType {
  // -- Unary operators --

//  LOGICAL_NOT_OPERATOR,  // For Bool.

//  BITWISE_NOT_OPERATOR,  // For Int.

//  POSITIVE_OPERATOR,  // For Int, Float.
//  NEGATIVE_OPERATOR,  // For Int, Float.

  // Typecast operators.
//  BOOL_OPERATOR,
//  INT_OPERATOR,
//  FLOAT_OPERATOR,
//  TIME_OPERATOR,
//  GEO_POINT_OPERATOR,
//  TEXT_OPERATOR,

  // -- Binary operators --

  // Logical operators.
  LOGICAL_AND_OPERATOR,  // For Bool.
  LOGICAL_OR_OPERATOR,   // For Bool.

  // Equality operators.
  EQUAL_OPERATOR,      // For any types.
  NOT_EQUAL_OPERATOR,  // For any types.

  // Comparison operators.
  LESS_OPERATOR,           // Int, Float, Time.
  LESS_EQUAL_OPERATOR,     // Int, Float, Time.
  GREATER_OPERATOR,        // Int, Float, Time.
  GREATER_EQUAL_OPERATOR,  // Int, Float, Time.

  // Bitwise operators.
  BITWISE_AND_OPERATOR,  // For Bool, Int.
  BITWISE_OR_OPERATOR,   // For Bool, Int.
  BITWISE_XOR_OPERATOR,  // For Bool, Int.

  // Arithmetic operators.
  PLUS_OPERATOR,            // For Int, Float.
  MINUS_OPERATOR,           // For Int, Float.
  MULTIPLICATION_OPERATOR,  // For Int, Float.
//  DIVISION_OPERATOR,        // For Int, Float.
//  MODULUS_OPERATOR,         // For Int.

  // Array operators.
//  SUBSCRIPT_OPERATOR,
};

class Expression {
 public:
  // Create an expression.
  //
  // Returns a poitner to the builder on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Expression> create(Error *error,
                                       const Table *table,
                                       unique_ptr<ExpressionNode> &&root);

  ~Expression();

  // Return the associated table.
  const Table *table() const {
    return table_;
  }
  // Return the result data type.
  DataType data_type() const;

  // Filter out false records.
  //
  // Evaluates the expression for the given record set and removes records
  // whose evaluation results are false.
  //
  // TODO: If there are many records, the filter should be applied per block.
  //       The best block size is not clear.
  //
  // TODO: An interface to apply a filter to a part of "*record_set" is needed.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool filter(Error *error, RecordSet *record_set);

 private:
  const Table *table_;
  unique_ptr<ExpressionNode> root_;

  Expression(const Table *table, unique_ptr<ExpressionNode> &&root);
};

class ExpressionBuilder {
 public:
  // Create an object for building expressons.
  //
  // Returns a poitner to the builder on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<ExpressionBuilder> create(Error *error,
                                              const Table *table);

  ~ExpressionBuilder();

  const Table *table() const {
    return table_;
  }

  // Push a datum.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_datum(Error *error, const Datum &datum);

  // Push a column.
  //
  // If "name" == "_id", pushes a pseudo column associated with row IDs.
  // If "name" == "_score", pushes a pseudo column associated with scores.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_column(Error *error, String name);

  // Push an operator.
  //
  // Pops operands and pushes an operator.
  // Fails if there are not enough operands.
  // Fails if the combination of operands are invalid.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_operator(Error *error, OperatorType operator_type);

  // Clear the stack.
  void clear();

  // Complete building an expression and clear the builder.
  //
  // Fails if the stack is empty or contains more than one nodes.
  //
  // Returns a poitner to the expression on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<Expression> release(Error *error);

 private:
  const Table *table_;
  std::vector<unique_ptr<ExpressionNode>> stack_;

  explicit ExpressionBuilder(const Table *table);

  // Push an operator &&.
  bool push_logical_and_operator(Error *error);
  // Push an operator ||.
  bool push_logical_or_operator(Error *error);
  // Push an operator == or !=.
  template <typename T>
  bool push_equality_operator(Error *error);
  // Push an operator <, <=, >, or >=.
  template <typename T>
  bool push_comparison_operator(Error *error);
  // Push an operator &, |, or ^.
  template <typename T>
  bool push_bitwise_operator(Error *error);
  // Push an operator +, -, or *.
  template <typename T>
  bool push_arithmetic_operator(Error *error);
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
