#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include "grnxx/array.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

enum OperatorType {
  // -- Unary operators --

//  LOGICAL_NOT_OPERATOR,  // For Bool.

//  BITWISE_NOT_OPERATOR,  // For Int.

  POSITIVE_OPERATOR,  // For Int, Float.
  NEGATIVE_OPERATOR,  // For Int, Float.

  // Typecast operators.
//  TO_BOOL_OPERATOR,
  TO_INT_OPERATOR,
  TO_FLOAT_OPERATOR,
//  TO_TIME_OPERATOR,
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
  // Return the evaluation block size.
  Int block_size() const {
    // TODO: This value should be optimized.
    return 1024;
  }

  // TODO: If the given record set contains many records (e.g. 1,048,576), the
  //       expression should be evaluated per block (e.g. 1,024).
  //       The best block size is not clear.

  // Filter out false records.
  //
  // Evaluates the expression for the given record set and removes records
  // whose evaluation results are false.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool filter(Error *error, RecordSet *record_set, Int offset = 0);

  // Adjust scores of records.
  //
  // Evaluates the expression for the given record set and replaces their
  // scores with the evaluation results.
  // The first "offset" records are not modified.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool adjust(Error *error, RecordSet *record_set, Int offset = 0);

  // Evaluate the expression.
  //
  // The result is stored into "*results".
  //
  // Fails if "T" is different from the result data type.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  template <typename T>
  bool evaluate(Error *error,
                const RecordSubset &record_set,
                Array<T> *results);

  // Evaluate the expression.
  //
  // The result is stored into "*results".
  //
  // Fails if the number of records is different from the size of "*results".
  // Fails if "T" is different from the result data type.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  template <typename T>
  bool evaluate(Error *error,
                const RecordSubset &record_set,
                Subarray<T> *results);

 private:
  const Table *table_;
  unique_ptr<ExpressionNode> root_;

  Expression(const Table *table, unique_ptr<ExpressionNode> &&root);

  template <typename T>
  bool evaluate_block(Error *error,
                      const RecordSubset &record_set,
                      Subarray<T> *results);
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
  Array<unique_ptr<ExpressionNode>> stack_;

  explicit ExpressionBuilder(const Table *table);

  // Push a unary operator.
  bool push_unary_operator(Error *error, OperatorType operator_type);
  // Push a positive operator.
  bool push_positive_operator(Error *error);
  // Push a negative operator.
  bool push_negative_operator(Error *error);
  // Push a typecast operator to Int.
  bool push_to_int_operator(Error *error);
  // Push a typecast operator to Float.
  bool push_to_float_operator(Error *error);

  // Push a binary operator.
  bool push_binary_operator(Error *error, OperatorType operator_type);
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
