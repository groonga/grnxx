#include "grnxx/expression.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
#include "grnxx/record.hpp"
#include "grnxx/table.hpp"

#include <iostream>  // For debugging.

namespace grnxx {
namespace expression {

// TODO: Only CONSTANT_NODE and VARIABLE_NODE are required?
enum NodeType {
  DATUM_NODE,
  ROW_ID_NODE,
  SCORE_NODE,
  COLUMN_NODE,
  OPERATOR_NODE
};

// -- Node --

class Node {
 public:
  Node() {}
  virtual ~Node() {}

  // Return the node type.
  virtual NodeType node_type() const = 0;
  // Return the result data type.
  virtual DataType data_type() const = 0;

  // Extract true records.
  //
  // Evaluates the expression for "input_records" and appends records
  // whose evaluation results are true into "*output_records".
  // "*output_records" is truncated to the number of true records.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool filter(Error *error,
                      const RecordSubset &input_records,
                      RecordSubset *output_records) = 0;
  // TODO
//  virtual bool filter(Error *error,
//                      const RecordSubset &input_records,
//                      RecordSet *output_records) = 0;

  // Adjust scores of records.
  //
  // Evaluates the expression for the given record set and replaces their
  // scores with the evaluation results.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool adjust(Error *error, RecordSubset *records) = 0;
};

// -- TypedNode --

template <typename T>
class TypedNode : public Node {
 public:
  using Value = T;

  TypedNode() : Node() {}
  virtual ~TypedNode() {}

  DataType data_type() const {
    return TypeTraits<Value>::data_type();
  }

  virtual bool filter(Error *error,
                      const RecordSubset &input_records,
                      RecordSubset *output_records);
  virtual bool adjust(Error *error, RecordSubset *records);

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into "*results".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool evaluate(Error *error,
                        const RecordSubset &records,
                        Subarray<Value> *results) = 0;
};

template <typename T>
bool TypedNode<T>::filter(Error *error,
                          const RecordSubset &input_records,
                          RecordSubset *output_records) {
  // Only TypedNode<Bool> supports filter().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool TypedNode<Bool>::filter(Error *error,
                             const RecordSubset &input_records,
                             RecordSubset *output_records) {
  // TODO: This implementation should be overridden by derived classes.
  Array<Bool> results;
  if (!results.resize(error, input_records.size())) {
    return false;
  }
  Subarray<Bool> results_ref = results.subarray();
  if (!evaluate(error, input_records, &results_ref)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (results[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->subset(0, count);
  return true;
}

template <typename T>
bool TypedNode<T>::adjust(Error *error,
                          RecordSubset *records) {
  // Only TypedNode<Float> supports adjust().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool TypedNode<Float>::adjust(Error *error, RecordSubset *records) {
  // TODO: This implementation should be overridden by derived classes.
  Array<Float> scores;
  if (!scores.resize(error, records->size())) {
    return false;
  }
  Subarray<Float> scores_ref = scores.subarray();
  if (!evaluate(error, *records, &scores_ref)) {
    return false;
  }
  for (Int i = 0; i < records->size(); ++i) {
    records->set_score(i, scores[i]);
  }
  return true;
}

// -- DatumNode --

template <typename T>
class DatumNode : public TypedNode<T> {
 public:
  using Value = T;

  explicit DatumNode(Value datum)
      : TypedNode<Value>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Value> *results);

 private:
  T datum_;
};

template <typename T>
bool DatumNode<T>::evaluate(Error *error,
                            const RecordSubset &records,
                            Subarray<Value> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = datum_;
  }
  return true;
}

template <>
class DatumNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit DatumNode(Bool datum)
      : TypedNode<Bool>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool filter(Error *error,
              const RecordSubset &input_records,
              RecordSubset *output_records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Bool> *results);

 private:
  Bool datum_;
};

bool DatumNode<Bool>::filter(Error *error,
                             const RecordSubset &input_records,
                             RecordSubset *output_records) {
  if (datum_) {
    if (&input_records != output_records) {
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
    }
  } else {
    *output_records = output_records->subset(0, 0);
  }
  return true;
}

bool DatumNode<Bool>::evaluate(Error *error,
                               const RecordSubset &records,
                               Subarray<Bool> *results) {
  // TODO: Fill results per 64 bits.
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = datum_;
  }
  return true;
}

template <>
class DatumNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  explicit DatumNode(Float datum)
      : TypedNode<Float>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool adjust(Error *error, RecordSubset *records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Float> *results);

 private:
  Float datum_;
};

bool DatumNode<Float>::adjust(Error *error, RecordSubset *records) {
  for (Int i = 0; i < records->size(); ++i) {
    records->set_score(i, datum_);
  }
  return true;
}

bool DatumNode<Float>::evaluate(Error *error,
                                const RecordSubset &records,
                                Subarray<Float> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = datum_;
  }
  return true;
}

template <>
class DatumNode<Text> : public TypedNode<Text> {
 public:
  using Value = Text;

  // TODO: This may throw an exception.
  explicit DatumNode(Text datum)
      : TypedNode<Text>(),
        datum_(datum.data(), datum.size()) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Text> *results);

 private:
  std::string datum_;
};

bool DatumNode<Text>::evaluate(Error *error,
                               const RecordSubset &records,
                               Subarray<Text> *results) {
  Text datum = Text(datum_.data(), datum_.size());
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = datum;
  }
  return true;
}

// -- RowIDNode --


class RowIDNode : public TypedNode<Int> {
 public:
  using Value = Int;

  RowIDNode() : TypedNode<Int>() {}

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Int> *results);
};

bool RowIDNode::evaluate(Error *error,
                         const RecordSubset &records,
                         Subarray<Int> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = records.get_row_id(i);
  }
  return true;
}

// -- ScoreNode --

class ScoreNode : public TypedNode<Float> {
 public:
  using Value = Float;

  ScoreNode() : TypedNode<Float>() {}

  NodeType node_type() const {
    return SCORE_NODE;
  }

  bool adjust(Error *error, RecordSubset *records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Float> *results);
};

bool ScoreNode::adjust(Error *error, RecordSubset *records) {
  // Nothing to do.
  return true;
}

bool ScoreNode::evaluate(Error *error,
                         const RecordSubset &records,
                         Subarray<Float> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = records.get_score(i);
  }
  return true;
}

// -- ColumnNode --

template <typename T>
class ColumnNode : public TypedNode<T> {
 public:
  using Value = T;

  explicit ColumnNode(const Column *column)
      : TypedNode<Value>(),
        column_(static_cast<const ColumnImpl<Value> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Value> *results);

 private:
  const ColumnImpl<T> *column_;
};

template <typename T>
bool ColumnNode<T>::evaluate(Error *error,
                             const RecordSubset &records,
                             Subarray<Value> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = column_->get(records.get_row_id(i));
  }
  return true;
}

template <>
class ColumnNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit ColumnNode(const Column *column)
      : TypedNode<Bool>(),
        column_(static_cast<const ColumnImpl<Bool> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool filter(Error *error,
              const RecordSubset &input_records,
              RecordSubset *output_records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Bool> *results);

 private:
  const ColumnImpl<Bool> *column_;
};

bool ColumnNode<Bool>::filter(Error *error,
                              const RecordSubset &input_records,
                              RecordSubset *output_records) {
  Int dest = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (column_->get(input_records.get_row_id(i))) {
      output_records->set(dest, input_records.get(i));
      ++dest;
    }
  }
  *output_records = output_records->subset(0, dest);
  return true;
}

bool ColumnNode<Bool>::evaluate(Error *error,
                                const RecordSubset &records,
                                Subarray<Bool> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = column_->get(records.get_row_id(i));
  }
  return true;
}

template <>
class ColumnNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  explicit ColumnNode(const Column *column)
      : TypedNode<Float>(),
        column_(static_cast<const ColumnImpl<Float> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool adjust(Error *error, RecordSubset *records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Float> *results);

 private:
  const ColumnImpl<Float> *column_;
};

bool ColumnNode<Float>::adjust(Error *error, RecordSubset *records) {
  for (Int i = 0; i < records->size(); ++i) {
    records->set_score(i, column_->get(records->get_row_id(i)));
  }
  return true;
}

bool ColumnNode<Float>::evaluate(Error *error,
                                 const RecordSubset &records,
                                 Subarray<Float> *results) {
  for (Int i = 0; i < records.size(); ++i) {
    (*results)[i] = column_->get(records.get_row_id(i));
  }
  return true;
}

// -- OperatorNode --

template <typename T>
class OperatorNode : public TypedNode<T> {
 public:
  using Value = T;

  OperatorNode() : TypedNode<T>() {}
  virtual ~OperatorNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }
};

// Evaluate "*arg" for "records".
//
// The evaluation results are stored into "*arg_values".
//
// On success, returns true.
// On failure, returns false and stores error information into "*error" if
// "error" != nullptr.
template <typename T>
bool fill_node_arg_values(Error *error, const RecordSubset &records,
                          TypedNode<T> *arg, Array<T> *arg_values) {
  Int old_size = arg_values->size();
  if (old_size < records.size()) {
    if (!arg_values->resize(error, records.size())) {
      return false;
    }
  }
  switch (arg->node_type()) {
    case DATUM_NODE: {
      if (old_size < records.size()) {
        Subarray<T> subarray = arg_values->subarray(old_size);
        if (!arg->evaluate(error, records.subset(old_size), &subarray)) {
          return false;
        }
      }
      break;
    }
    default: {
      Subarray<T> subarray = arg_values->subarray(0, records.size());
      if (!arg->evaluate(error, records, &subarray)) {
        return false;
      }
    }
  }
  return true;
}

// --- UnaryNode ---

template <typename T, typename U>
class UnaryNode : public OperatorNode<T> {
 public:
  using Value = T;
  using Arg = U;

  explicit UnaryNode(unique_ptr<Node> &&arg)
      : OperatorNode<Value>(),
        arg_(static_cast<TypedNode<Bool> *>(arg.release())),
        arg_values_() {}
  virtual ~UnaryNode() {}

 protected:
  unique_ptr<TypedNode<Arg>> arg_;
  Array<Arg> arg_values_;

  bool fill_arg_values(Error *error, const RecordSubset &records) {
    return fill_node_arg_values(error, records, arg_.get(), &arg_values_);
  }
};

// ---- LogicalNotNode ----

class LogicalNotNode : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;

  explicit LogicalNotNode(unique_ptr<Node> &&arg)
      : UnaryNode<Bool, Bool>(std::move(arg)),
        temp_records_() {}

  bool filter(Error *error,
              const RecordSubset &input_records,
              RecordSubset *output_records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Bool> *results);

 private:
  RecordSet temp_records_;
};

bool LogicalNotNode::filter(Error *error,
                            const RecordSubset &input_records,
                            RecordSubset *output_records) {
  // Create a copy of "input_records" and then apply a filter to it.
  // Then, appends a sentinel to the end of the result.
  if (!temp_records_.resize(error, input_records.size() + 1)) {
    return false;
  }
  for (Int i = 0; i < input_records.size(); ++i) {
    temp_records_.set(i, input_records.get(i));
  }
  RecordSubset subset = temp_records_.subset();
  if (!arg_->filter(error, subset, &subset)) {
    return false;
  }
  temp_records_.set_row_id(subset.size(), NULL_ROW_ID);

  // Extract records which appear in "input_records" and don't appear in
  // "subset".
  Int count = 0;
  for (Int i = 0, j = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) == subset.get_row_id(j)) {
      continue;
    }
    output_records->set(count, input_records.get(i));
    ++count;
    ++j;
  }
  *output_records = output_records->subset(0, count);
  return true;
}

bool LogicalNotNode::evaluate(Error *error,
                              const RecordSubset &records,
                              Subarray<Bool> *results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < results->size(); ++i) {
    (*results)[i] = !(*results)[i];
  }
  return true;
}

// ---- BitwiseNotNode ----

class BitwiseNotNode : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;

  explicit BitwiseNotNode(unique_ptr<Node> &&arg)
      : UnaryNode<Bool, Bool>(std::move(arg)) {}

  bool filter(Error *error,
              const RecordSubset &input_records,
              RecordSubset *output_records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Bool> *results);
};

bool BitwiseNotNode::filter(Error *error,
                            const RecordSubset &input_records,
                            RecordSubset *output_records) {
  if (!fill_arg_values(error, input_records)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (arg_values_[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->subset(0, count);
  return true;
}

bool BitwiseNotNode::evaluate(Error *error,
                              const RecordSubset &records,
                              Subarray<Bool> *results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < results->size(); ++i) {
    (*results)[i] = !(*results)[i];
  }
  return true;
}

// TODO: Other unary operators!
//
//POSITIVE_OPERATOR  // For Int, Float.
//NEGATIVE_OPERATOR  // For Int, Float.
//TO_INT_OPERATOR    // For Float.
//TO_FLOAT_OPERATOR  // For Int.

// --- BinaryNode ---

template <typename T, typename U, typename V>
class BinaryNode : public OperatorNode<T> {
 public:
  using Value = T;
  using Arg1 = U;
  using Arg2 = V;

  BinaryNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : OperatorNode<Value>(),
        arg1_(static_cast<TypedNode<Bool> *>(arg1.release())),
        arg2_(static_cast<TypedNode<Bool> *>(arg2.release())),
        arg1_values_(),
        arg2_values_() {}
  virtual ~BinaryNode() {}

 protected:
  unique_ptr<TypedNode<Arg1>> arg1_;
  unique_ptr<TypedNode<Arg2>> arg2_;
  Array<Arg1> arg1_values_;
  Array<Arg2> arg2_values_;

  bool fill_arg1_values(Error *error, const RecordSubset &records) {
    return fill_node_arg_values(error, records, arg1_.get(), &arg1_values_);
  }
  bool fill_arg2_values(Error *error, const RecordSubset &records) {
    return fill_node_arg_values(error, records, arg2_.get(), &arg2_values_);
  }
};

// ---- XXXNode ----

class LogicalAndNode : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  LogicalAndNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Bool, Bool, Bool>(std::move(arg1), std::move(arg2)),
        temp_records_() {}

  bool filter(Error *error,
              const RecordSubset &input_records,
              RecordSubset *output_records);
  bool evaluate(Error *error,
                const RecordSubset &records,
                Subarray<Bool> *results);

 private:
  RecordSet temp_records_;
};

bool LogicalAndNode::filter(Error *error,
                            const RecordSubset &input_records,
                            RecordSubset *output_records) {
  return arg1_->filter(error, input_records, output_records) &&
         arg2_->filter(error, *output_records, output_records);
}

bool LogicalAndNode::evaluate(Error *error,
                              const RecordSubset &records,
                              Subarray<Bool> *results) {
  // TODO
  return false;
}

// TODO: Other binary operators.
//  // Logical operators.
//  LOGICAL_OR_OPERATOR,   // For Bool.

//  // Equality operators.
//  EQUAL_OPERATOR,      // For any types.
//  NOT_EQUAL_OPERATOR,  // For any types.

//  // Comparison operators.
//  LESS_OPERATOR,           // Int, Float, Time.
//  LESS_EQUAL_OPERATOR,     // Int, Float, Time.
//  GREATER_OPERATOR,        // Int, Float, Time.
//  GREATER_EQUAL_OPERATOR,  // Int, Float, Time.

//  // Bitwise operators.
//  BITWISE_AND_OPERATOR,  // For Bool, Int.
//  BITWISE_OR_OPERATOR,   // For Bool, Int.
//  BITWISE_XOR_OPERATOR,  // For Bool, Int.

//  // Arithmetic operators.
//  PLUS_OPERATOR,            // For Int, Float.
//  MINUS_OPERATOR,           // For Int, Float.
//  MULTIPLICATION_OPERATOR,  // For Int, Float.
//  DIVISION_OPERATOR,        // For Int, Float.
//  MODULUS_OPERATOR,         // For Int.

}  // namespace expression

}  // namespace grnxx
