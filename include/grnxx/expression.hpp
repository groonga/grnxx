#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

enum OperatorType {
  // -- Unary operators --

  // -- Binary operators --

  EQUAL_OPERATOR,
  NOT_EQUAL_OPERATOR
};

class Expression {
 public:
  // Create an expression.
  //
  // Returns a poitner to the builder on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Expression> create(Error *error,
                                       unique_ptr<ExpressionNode> &&root);

  ~Expression();

  // Return the associated table.
  Table *table() const {
    return table_;
  }
  // Return the result data type.
  DataType data_type() const;

  // Filter out false records.
  //
  // Evaluates the expression for the given record set and removes records
  // whose evaluation results are false.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool filter(Error *error, RecordSet *record_set);

 private:
  Table *table_;
  unique_ptr<ExpressionNode> root_;

  Expression(Table *table, unique_ptr<ExpressionNode> &&root);
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
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
