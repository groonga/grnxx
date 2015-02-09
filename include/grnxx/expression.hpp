#ifndef GRNXX_EXPRESSION_HPP
#define GRNXX_EXPRESSION_HPP

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/column.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

using OperatorType = grnxx_operator_type;

struct ExpressionOptions {
  // Records are evaluated per block.
  size_t block_size;

  ExpressionOptions() : block_size(1024) {}
};

class Expression {
 public:
  Expression() = default;
  virtual ~Expression() = default;

  // Return the associated table.
  virtual const Table *table() const = 0;
  // Return the result data type.
  virtual DataType data_type() const = 0;
  // Return whether "*this" is equal to RowID or not.
  virtual bool is_row_id() const = 0;
  // Return whether "*this" is equal to Score or not.
  virtual bool is_score() const = 0;
  // Return the evaluation block size.
  virtual size_t block_size() const = 0;

  // Extract true records.
  //
  // Evaluates the expression for "*records" and removes records whose
  // evaluation results are not true.
  //
  // The first "input_offset" records in "*records" are left as is without
  // evaluation.
  // The first "output_offset" true records are removed.
  // The number of output records is at most "output_limit".
  //
  // On failure, throws an exception.
  virtual void filter(
      Array<Record> *records,
      size_t input_offset = 0,
      size_t output_offset = 0,
      size_t output_limit = std::numeric_limits<size_t>::max()) = 0;

  // Extract true records.
  //
  // Evaluates the expression for "input_records" and stores true records
  // into "*output_records".
  // "*output_records" is truncated to fit the number of extracted records.
  //
  // Fails if "output_records->size()" is less than "input_records.size()".
  //
  // On failure, throws an exception.
  virtual void filter(ArrayCRef<Record> input_records,
                      ArrayRef<Record> *output_records) = 0;

  // Adjust scores of records.
  //
  // Evaluates the expression for "*records" and replaces the scores with
  // the evaluation results.
  //
  // The first "offset" records are not modified.
  //
  // On failure, throws an exception.
  virtual void adjust(Array<Record> *records, size_t offset = 0) = 0;

  // Adjust scores of records.
  //
  // Evaluates the expression for "records" and replaces the scores with
  // the evaluation results.
  //
  // On failure, throws an exception.
  virtual void adjust(ArrayRef<Record> records) = 0;

  // Evaluate the expression.
  //
  // Evaluates the expression for "records" and stores the results into
  // "*results".
  //
  // Fails if "T" differs from the result data type.
  //
  // On failure, throws an exception.
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Bool> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Int> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Float> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<GeoPoint> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Text> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Vector<Bool>> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Vector<Int>> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Vector<Float>> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Vector<GeoPoint>> *results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        Array<Vector<Text>> *results) = 0;

  // Evaluate the expression.
  //
  // Evaluates the expression for "records" and stores the results into
  // "results".
  //
  // Fails if the data type of "records" differs from the result data type.
  // Fails if "records.size()" != "results.size()".
  //
  // On failure, throws an exception.
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Bool> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Int> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Float> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<GeoPoint> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Text> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Vector<Bool>> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Vector<Int>> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Vector<Float>> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Vector<GeoPoint>> results) = 0;
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Vector<Text>> results) = 0;
};

class ExpressionBuilder {
 public:
  // Create an object for building expressions.
  //
  // On success, returns the builder.
  // On failure, throws an exception.
  static std::unique_ptr<ExpressionBuilder> create(const Table *table);

  ExpressionBuilder() = default;
  virtual ~ExpressionBuilder() = default;

  // Return the target table.
  virtual const Table *table() const = 0;

  // Push a node associated with a constant.
  //
  // On failure, throws an exception.
  virtual void push_constant(const Datum &datum) = 0;

  // Push a node associated with row IDs of Records.
  //
  // On failure, throws an exception.
  virtual void push_row_id() = 0;

  // Push a node associated with scores of Records.
  //
  // On failure, throws an exception.
  virtual void push_score() = 0;

  // Push a node associated with a column.
  //
  // On failure, throws an exception.
  virtual void push_column(const String &name) = 0;

  // Push a node associated with an operator.
  //
  // Pops operands and pushes an operator.
  // Fails if there are not enough operands.
  // Fails if the combination of the operator and its operands is invalid.
  //
  // On failure, throws an exception.
  virtual void push_operator(OperatorType operator_type) = 0;

  // Begin a subexpression.
  //
  // On failure, throws an exception.
  virtual void begin_subexpression() = 0;

  // End a subexpression.
  //
  // Fails if the stack for the subexpression is empty or contains more than
  // one nodes.
  //
  // On failure, throws an exception.
  virtual void end_subexpression(
      const ExpressionOptions &options = ExpressionOptions()) = 0;

  // Clear the internal stack.
  virtual void clear() = 0;

  // Complete building an expression and clear the internal stack.
  //
  // Fails if the node stack is empty or contains more than one nodes.
  // Fails if there is an incomplete subexpression.
  //
  // On success, returns the expression.
  // On failure, throws an exception.
  virtual std::unique_ptr<Expression> release(
      const ExpressionOptions &options = ExpressionOptions()) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_EXPRESSION_HPP
