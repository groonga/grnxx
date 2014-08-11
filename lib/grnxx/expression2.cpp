#include "grnxx/expression.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
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
                      ArrayCRef<Record> input_records,
                      ArrayRef<Record> *output_records) = 0;

  // Adjust scores of records.
  //
  // Evaluates the expression for the given record set and replaces their
  // scores with the evaluation results.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool adjust(Error *error, ArrayRef<Record> records) = 0;
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
                      ArrayCRef<Record> input_records,
                      ArrayRef<Record> *output_records);
  virtual bool adjust(Error *error, ArrayRef<Record> records);

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into "*results".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool evaluate(Error *error,
                        ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;
};

template <typename T>
bool TypedNode<T>::filter(Error *error,
                          ArrayCRef<Record> input_records,
                          ArrayRef<Record> *output_records) {
  // Only TypedNode<Bool> supports filter().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool TypedNode<Bool>::filter(Error *error,
                             ArrayCRef<Record> input_records,
                             ArrayRef<Record> *output_records) {
  // TODO: This implementation should be overridden by derived classes.
  Array<Bool> results;
  if (!results.resize(error, input_records.size())) {
    return false;
  }
  if (!evaluate(error, input_records, results)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (results[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
  return true;
}

template <typename T>
bool TypedNode<T>::adjust(Error *error,
                          ArrayRef<Record> records) {
  // Only TypedNode<Float> supports adjust().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool TypedNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  // TODO: This implementation should be overridden by derived classes.
  Array<Float> scores;
  if (!scores.resize(error, records.size())) {
    return false;
  }
  if (!evaluate(error, records, scores)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, scores[i]);
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
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  T datum_;
};

template <typename T>
bool DatumNode<T>::evaluate(Error *error,
                            ArrayCRef<Record> records,
                            ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = datum_;
  }
  return true;
}

template <>
class DatumNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit DatumNode(Value datum)
      : TypedNode<Value>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  Value datum_;
};

bool DatumNode<Bool>::filter(Error *error,
                             ArrayCRef<Record> input_records,
                             ArrayRef<Record> *output_records) {
  if (datum_) {
    if (input_records != *output_records) {
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
    }
  } else {
    *output_records = output_records->ref(0, 0);
  }
  return true;
}

bool DatumNode<Bool>::evaluate(Error *error,
                               ArrayCRef<Record> records,
                               ArrayRef<Value> results) {
  // TODO: Fill results per 64 bits.
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, datum_);
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

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Float> results);

 private:
  Float datum_;
};

bool DatumNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, datum_);
  }
  return true;
}

bool DatumNode<Float>::evaluate(Error *error,
                                ArrayCRef<Record> records,
                                ArrayRef<Float> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = datum_;
  }
  return true;
}

template <>
class DatumNode<Text> : public TypedNode<Text> {
 public:
  using Value = Text;

  // TODO: This may throw an exception.
  explicit DatumNode(Value datum)
      : TypedNode<Value>(),
        datum_(datum.data(), datum.size()) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  std::string datum_;
};

bool DatumNode<Text>::evaluate(Error *error,
                               ArrayCRef<Record> records,
                               ArrayRef<Value> results) {
  Text datum(datum_.data(), datum_.size());
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = datum;
  }
  return true;
}

// -- RowIDNode --


class RowIDNode : public TypedNode<Int> {
 public:
  using Value = Int;

  RowIDNode() : TypedNode<Value>() {}

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool RowIDNode::evaluate(Error *error,
                         ArrayCRef<Record> records,
                         ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = records.get_row_id(i);
  }
  return true;
}

// -- ScoreNode --

class ScoreNode : public TypedNode<Float> {
 public:
  using Value = Float;

  ScoreNode() : TypedNode<Value>() {}

  NodeType node_type() const {
    return SCORE_NODE;
  }

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool ScoreNode::adjust(Error *error, ArrayRef<Record> records) {
  // Nothing to do.
  return true;
}

bool ScoreNode::evaluate(Error *error,
                         ArrayCRef<Record> records,
                         ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = records.get_score(i);
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
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  const ColumnImpl<T> *column_;
};

template <typename T>
bool ColumnNode<T>::evaluate(Error *error,
                             ArrayCRef<Record> records,
                             ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = column_->get(records.get_row_id(i));
  }
  return true;
}

template <>
class ColumnNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit ColumnNode(const Column *column)
      : TypedNode<Value>(),
        column_(static_cast<const ColumnImpl<Value> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  const ColumnImpl<Value> *column_;
};

bool ColumnNode<Bool>::filter(Error *error,
                              ArrayCRef<Record> input_records,
                              ArrayRef<Record> *output_records) {
  Int dest = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (column_->get(input_records.get_row_id(i))) {
      output_records->set(dest, input_records.get(i));
      ++dest;
    }
  }
  *output_records = output_records->ref(0, dest);
  return true;
}

bool ColumnNode<Bool>::evaluate(Error *error,
                                ArrayCRef<Record> records,
                                ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, column_->get(records.get_row_id(i)));
  }
  return true;
}

template <>
class ColumnNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  explicit ColumnNode(const Column *column)
      : TypedNode<Value>(),
        column_(static_cast<const ColumnImpl<Value> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  const ColumnImpl<Value> *column_;
};

bool ColumnNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, column_->get(records.get_row_id(i)));
  }
  return true;
}

bool ColumnNode<Float>::evaluate(Error *error,
                                 ArrayCRef<Record> records,
                                 ArrayRef<Value> results) {
  for (Int i = 0; i < records.size(); ++i) {
    results[i] = column_->get(records.get_row_id(i));
  }
  return true;
}

// -- OperatorNode --

template <typename T>
class OperatorNode : public TypedNode<T> {
 public:
  using Value = T;

  OperatorNode() : TypedNode<Value>() {}
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
bool fill_node_arg_values(Error *error, ArrayCRef<Record> records,
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
        if (!arg->evaluate(error, records.ref(old_size),
                           arg_values->ref(old_size))) {
          return false;
        }
      }
      break;
    }
    default: {
      if (!arg->evaluate(error, records, arg_values->ref(0, records.size()))) {
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
        arg_(static_cast<TypedNode<Arg> *>(arg.release())),
        arg_values_() {}
  virtual ~UnaryNode() {}

 protected:
  unique_ptr<TypedNode<Arg>> arg_;
  Array<Arg> arg_values_;

  bool fill_arg_values(Error *error, ArrayCRef<Record> records) {
    return fill_node_arg_values(error, records, arg_.get(), &arg_values_);
  }
};

// ---- LogicalNotNode ----

class LogicalNotNode : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;
  using Arg = Bool;

  explicit LogicalNotNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)),
        temp_records_() {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

bool LogicalNotNode::filter(Error *error,
                            ArrayCRef<Record> input_records,
                            ArrayRef<Record> *output_records) {
  // Apply an argument filter to "input_records" and store the result to
  // "temp_records_". Then, appends a sentinel to the end.
  if (!temp_records_.resize(error, input_records.size() + 1)) {
    return false;
  }
  ArrayRef<Record> ref = temp_records_;
  if (!arg_->filter(error, input_records, &ref)) {
    return false;
  }
  temp_records_.set_row_id(ref.size(), NULL_ROW_ID);

  // Extract records which appear in "input_records" and don't appear in "ref".
  Int count = 0;
  for (Int i = 0, j = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) == ref.get_row_id(j)) {
      ++j;
      continue;
    }
    output_records->set(count, input_records.get(i));
    ++count;
  }
  *output_records = output_records->ref(0, count);
  return true;
}

bool LogicalNotNode::evaluate(Error *error,
                              ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  // Apply an argument filter to "records" and store the result to
  // "temp_records_". Then, appends a sentinel to the end.
  if (!temp_records_.resize(error, records.size() + 1)) {
    return false;
  }
  ArrayRef<Record> ref = temp_records_;
  if (!arg_->filter(error, records, &ref)) {
    return false;
  }
  temp_records_.set_row_id(ref.size(), NULL_ROW_ID);

  // Compare records in "records" and "ref".
  Int count = 0;
  for (Int i = 0; i < records.size(); ++i) {
    if (records.get_row_id(i) == ref.get_row_id(count)) {
      results.set(i, false);
      ++count;
    } else {
      results.set(i, true);
    }
  }
  return true;
}

// ---- BitwiseNotNode ----

template <typename T> class BitwiseNotNode;

template <>
class BitwiseNotNode<Bool> : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;
  using Arg = Bool;

  explicit BitwiseNotNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseNotNode<Bool>::filter(Error *error,
                                  ArrayCRef<Record> input_records,
                                  ArrayRef<Record> *output_records) {
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
  *output_records = output_records->ref(0, count);
  return true;
}

bool BitwiseNotNode<Bool>::evaluate(Error *error,
                                    ArrayCRef<Record> records,
                                    ArrayRef<Value> results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, !results.get(i));
  }
  return true;
}

template <>
class BitwiseNotNode<Int> : public UnaryNode<Int, Int> {
 public:
  using Value = Int;
  using Arg = Int;

  explicit BitwiseNotNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseNotNode<Int>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, ~results.get(i));
  }
  return true;
}

// ---- PositiveNode ----

// Nothing to do.

// ---- NegativeNode ----

template <typename T> class NegativeNode;

template <>
class NegativeNode<Int> : public UnaryNode<Int, Int> {
 public:
  using Value = Int;
  using Arg = Int;

  explicit NegativeNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool NegativeNode<Int>::evaluate(Error *error,
                                 ArrayCRef<Record> records,
                                 ArrayRef<Value> results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, -results.get(i));
  }
  return true;
}

template <>
class NegativeNode<Float> : public UnaryNode<Float, Float> {
 public:
  using Value = Float;
  using Arg = Float;

  explicit NegativeNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool NegativeNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  if (!fill_arg_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, arg_values_[i]);
  }
  return true;
}

bool NegativeNode<Float>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!arg_->evaluate(error, records, results)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, -results.get(i));
  }
  return true;
}

// ---- ToIntNode ----

class ToIntNode : public UnaryNode<Int, Float> {
 public:
  using Value = Int;
  using Arg = Float;

  explicit ToIntNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool ToIntNode::evaluate(Error *error,
                         ArrayCRef<Record> records,
                         ArrayRef<Value> results) {
  if (!fill_arg_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, static_cast<Value>(arg_values_[i]));
  }
  return true;
}

// ---- ToFloatNode ----

class ToFloatNode : public UnaryNode<Float, Int> {
 public:
  using Value = Float;
  using Arg = Int;

  explicit ToFloatNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool ToFloatNode::evaluate(Error *error,
                           ArrayCRef<Record> records,
                           ArrayRef<Value> results) {
  if (!fill_arg_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, static_cast<Value>(arg_values_[i]));
  }
  return true;
}

// --- BinaryNode ---

template <typename T, typename U, typename V>
class BinaryNode : public OperatorNode<T> {
 public:
  using Value = T;
  using Arg1 = U;
  using Arg2 = V;

  BinaryNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : OperatorNode<Value>(),
        arg1_(static_cast<TypedNode<Arg1> *>(arg1.release())),
        arg2_(static_cast<TypedNode<Arg2> *>(arg2.release())),
        arg1_values_(),
        arg2_values_() {}
  virtual ~BinaryNode() {}

 protected:
  unique_ptr<TypedNode<Arg1>> arg1_;
  unique_ptr<TypedNode<Arg2>> arg2_;
  Array<Arg1> arg1_values_;
  Array<Arg2> arg2_values_;

  bool fill_arg1_values(Error *error, ArrayCRef<Record> records) {
    return fill_node_arg_values(error, records, arg1_.get(), &arg1_values_);
  }
  bool fill_arg2_values(Error *error, ArrayCRef<Record> records) {
    return fill_node_arg_values(error, records, arg2_.get(), &arg2_values_);
  }
};

// ---- LogicalAndNode ----

class LogicalAndNode : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  LogicalAndNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

bool LogicalAndNode::filter(Error *error,
                            ArrayCRef<Record> input_records,
                            ArrayRef<Record> *output_records) {
  return arg1_->filter(error, input_records, output_records) &&
         arg2_->filter(error, *output_records, output_records);
}

bool LogicalAndNode::evaluate(Error *error,
                              ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  // Apply argument filters to "records" and store the result to
  // "temp_records_". Then, appends a sentinel to the end.
  if (!temp_records_.resize(error, records.size() + 1)) {
    return false;
  }
  ArrayRef<Record> ref = temp_records_;
  if (!arg1_->filter(error, records, &ref) ||
      !arg2_->filter(error, ref, &ref)) {
    return false;
  }
  temp_records_.set_row_id(ref.size(), NULL_ROW_ID);

  // Compare records in "records" and "ref".
  Int count = 0;
  for (Int i = 0; i < records.size(); ++i) {
    if (records.get_row_id(i) == ref.get_row_id(count)) {
      results.set(i, true);
      ++count;
    } else {
      results.set(i, false);
    }
  }
  return true;
}

// ---- LogicalOrNode ----

class LogicalOrNode : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  LogicalOrNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

bool LogicalOrNode::filter(Error *error,
                            ArrayCRef<Record> input_records,
                            ArrayRef<Record> *output_records) {
  // Apply the 1st argument filter to "input_records" and store the result into
  // "temp_records_", Then, appends a sentinel to the end.
  if (!temp_records_.resize(error, input_records.size() + 2)) {
    return false;
  }
  ArrayRef<Record> ref1 = temp_records_;
  if (!arg1_->filter(error, input_records, &ref1)) {
    return false;
  }
  if (ref1.size() == 0) {
    // There are no arg1-true records.
    return arg2_->filter(error, input_records, output_records);
  } else if (ref1.size() == temp_records_.size()) {
    // There are no arg1-false records.
    if (input_records != *output_records) {
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
    }
    return true;
  }
  temp_records_.set_row_id(ref1.size(), NULL_ROW_ID);

  // Append arg1-false records to the end of "temp_records_".
  // Then, applies the 2nd argument filter to it and appends a sentinel.
  ArrayRef<Record> ref2 =
      temp_records_.ref(ref1.size() + 1, input_records.size() - ref1.size());
  Int arg1_count = 0;
  Int arg2_count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) == ref1.get_row_id(arg1_count)) {
      ++arg1_count;
    } else {
      ref2.set(arg2_count, input_records.get(i));
      ++arg2_count;
    }
  }
  if (!arg2_->filter(error, ref2, &ref2)) {
    return false;
  }
  if (ref2.size() == 0) {
    // There are no arg2-true records.
    for (Int i = 0; i < ref1.size(); ++i) {
      output_records->set(i, ref1.get(i));
    }
    *output_records = output_records->ref(0, ref1.size());
    return true;
  } else if (ref2.size() == arg2_count) {
    // There are no arg2-false records.
    if (input_records != *output_records) {
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
      *output_records = output_records->ref(0, input_records.size());
    }
    return true;
  }
  temp_records_.set_row_id(ref1.size() + 1 + ref2.size(), NULL_ROW_ID);

  // Merge the arg1-true records and the arg2-true records.
  arg1_count = 0;
  arg2_count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) == ref1.get_row_id(arg1_count)) {
      output_records->set(arg1_count + arg2_count, input_records.get(i));
      ++arg1_count;
    } else if (input_records.get_row_id(i) == ref2.get_row_id(arg2_count)) {
      output_records->set(arg1_count + arg2_count, input_records.get(i));
      ++arg2_count;
    }
  }
  *output_records = output_records->ref(0, arg1_count + arg2_count);
  return true;
}

bool LogicalOrNode::evaluate(Error *error,
                              ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  // Apply the 1st argument filter to "records" and store the result into
  // "temp_records_", Then, appends a sentinel to the end.
  if (!temp_records_.resize(error, records.size() + 2)) {
    return false;
  }
  ArrayRef<Record> ref1 = temp_records_;
  if (!arg1_->filter(error, records, &ref1)) {
    return false;
  }
  if (ref1.size() == 0) {
    // There are no arg1-true records.
    return arg2_->evaluate(error, records, results);
  } else if (ref1.size() == temp_records_.size()) {
    // There are no arg1-false records.
    // TODO: Fill the array per 64 bits.
    for (Int i = 0; i < records.size(); ++i) {
      results.set(i, true);
    }
    return true;
  }
  temp_records_.set_row_id(ref1.size(), NULL_ROW_ID);

  // Append arg1-false records to the end of "temp_records_".
  // Then, applies the 2nd argument filter to it and appends a sentinel.
  ArrayRef<Record> ref2 =
      temp_records_.ref(ref1.size() + 1, records.size() - ref1.size());
  Int arg1_count = 0;
  Int arg2_count = 0;
  for (Int i = 0; i < records.size(); ++i) {
    if (records.get_row_id(i) == ref1.get_row_id(arg1_count)) {
      ++arg1_count;
    } else {
      ref2.set(arg2_count, records.get(i));
      ++arg2_count;
    }
  }
  if (!arg2_->filter(error, ref2, &ref2)) {
    return false;
  }
  if (ref2.size() == 0) {
    // There are no arg2-true records.
    arg1_count = 0;
    for (Int i = 0; i < records.size(); ++i) {
      if (records.get_row_id(i) == ref1.get_row_id(arg1_count)) {
        results.set(i, true);
        ++arg1_count;
      } else {
        results.set(i, false);
      }
    }
    return true;
  } else if (ref2.size() == arg2_count) {
    // There are no arg2-false records.
    // TODO: Fill the array per 64 bits.
    for (Int i = 0; i < records.size(); ++i) {
      results.set(i, true);
    }
    return true;
  }
  temp_records_.set_row_id(ref1.size() + 1 + ref2.size(), NULL_ROW_ID);

  // Merge the arg1-true records and the arg2-true records.
  arg1_count = 0;
  arg2_count = 0;
  for (Int i = 0; i < records.size(); ++i) {
    if (records.get_row_id(i) == ref1.get_row_id(arg1_count)) {
      results.set(i, true);
      ++arg1_count;
    } else if (records.get_row_id(i) == ref2.get_row_id(arg2_count)) {
      results.set(i, true);
      ++arg2_count;
    } else {
      results.set(i, false);
    }
  }
  return true;
}

// ---- ComparisonNode ----

template <typename T>
class ComparisonNode
    : public BinaryNode<Bool, typename T::Arg, typename T::Arg> {
 public:
  using Comparer = T;
  using Value = Bool;
  using Arg1 = typename T::Arg;
  using Arg2 = typename T::Arg;

  ComparisonNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        comparer_() {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 protected:
  Comparer comparer_;
};

template <typename T>
bool ComparisonNode<T>::filter(Error *error,
                               ArrayCRef<Record> input_records,
                               ArrayRef<Record> *output_records) {
  if (!this->fill_arg1_values(error, input_records) ||
      !this->fill_arg2_values(error, input_records)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (comparer_(this->arg1_values_[i], this->arg2_values_[i])) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
  return true;
}

template <typename T>
bool ComparisonNode<T>::evaluate(Error *error,
                                 ArrayCRef<Record> records,
                                 ArrayRef<Value> results) {
  if (!this->fill_arg1_values(error, records) ||
      !this->fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, comparer_(this->arg1_values_[i], this->arg2_values_[i]));
  }
  return true;
}

// ----- EqualNode -----

struct Equal {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 == arg2;
    }
  };
};

template <typename T>
using EqualNode = ComparisonNode<Equal::Comparer<T>>;

// ----- NotEqualNode -----

struct NotEqual {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 != arg2;
    }
  };
};

template <typename T>
using NotEqualNode = ComparisonNode<NotEqual::Comparer<T>>;

// ----- LessNode -----

struct Less {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 < arg2;
    }
  };
};

template <typename T>
using LessNode = ComparisonNode<Less::Comparer<T>>;

// ----- LessEqualNode -----

struct LessEqual {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 <= arg2;
    }
  };
};

template <typename T>
using LessEqualNode = ComparisonNode<LessEqual::Comparer<T>>;

// ----- GreaterNode -----

struct Greater {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 > arg2;
    }
  };
};

template <typename T>
using GreaterNode = ComparisonNode<Greater::Comparer<T>>;

// ----- GreaterEqualNode -----

struct GreaterEqual {
  template <typename T>
  struct Comparer {
    using Arg = T;
    Bool operator()(Arg arg1, Arg arg2) const {
      return arg1 >= arg2;
    }
  };
};

template <typename T>
using GreaterEqualNode = ComparisonNode<GreaterEqual::Comparer<T>>;

// ---- BitwiseAndNode ----

// TODO: BitwiseAnd/Or/XorNode should be implemented on the same base class?

template <typename T> class BitwiseAndNode;

template <>
class BitwiseAndNode<Bool> : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  BitwiseAndNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseAndNode<Bool>::filter(Error *error,
                                  ArrayCRef<Record> input_records,
                                  ArrayRef<Record> *output_records) {
  if (!fill_arg1_values(error, input_records) ||
      !fill_arg2_values(error, input_records)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (this->arg1_values_[i] & this->arg2_values_[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
  return true;
}

bool BitwiseAndNode<Bool>::evaluate(Error *error,
                                    ArrayCRef<Record> records,
                                    ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] & this->arg2_values_[i]);
  }
  return true;
}

template <>
class BitwiseAndNode<Int> : public BinaryNode<Bool, Int, Int> {
 public:
  using Value = Bool;
  using Arg1 = Int;
  using Arg2 = Int;

  BitwiseAndNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseAndNode<Int>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] & this->arg2_values_[i]);
  }
  return true;
}

// ---- BitwiseOrNode ----

template <typename T> class BitwiseOrNode;

template <>
class BitwiseOrNode<Bool> : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  BitwiseOrNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseOrNode<Bool>::filter(Error *error,
                                 ArrayCRef<Record> input_records,
                                 ArrayRef<Record> *output_records) {
  if (!fill_arg1_values(error, input_records) ||
      !fill_arg2_values(error, input_records)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (this->arg1_values_[i] | this->arg2_values_[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
  return true;
}

bool BitwiseOrNode<Bool>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] | this->arg2_values_[i]);
  }
  return true;
}

template <>
class BitwiseOrNode<Int> : public BinaryNode<Bool, Int, Int> {
 public:
  using Value = Bool;
  using Arg1 = Int;
  using Arg2 = Int;

  BitwiseOrNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseOrNode<Int>::evaluate(Error *error,
                                  ArrayCRef<Record> records,
                                  ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] | this->arg2_values_[i]);
  }
  return true;
}

// ---- BitwiseXorNode ----

template <typename T> class BitwiseXorNode;

template <>
class BitwiseXorNode<Bool> : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  BitwiseXorNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseXorNode<Bool>::filter(Error *error,
                                  ArrayCRef<Record> input_records,
                                  ArrayRef<Record> *output_records) {
  if (!fill_arg1_values(error, input_records) ||
      !fill_arg2_values(error, input_records)) {
    return false;
  }
  Int count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (this->arg1_values_[i] ^ this->arg2_values_[i]) {
      output_records->set(count, input_records.get(i));
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
  return true;
}

bool BitwiseXorNode<Bool>::evaluate(Error *error,
                                    ArrayCRef<Record> records,
                                    ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] ^ this->arg2_values_[i]);
  }
  return true;
}

template <>
class BitwiseXorNode<Int> : public BinaryNode<Bool, Int, Int> {
 public:
  using Value = Bool;
  using Arg1 = Int;
  using Arg2 = Int;

  BitwiseXorNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool BitwiseXorNode<Int>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] ^ this->arg2_values_[i]);
  }
  return true;
}

// ---- PlusOperator ----

template <typename T> class PlusNode;

template <>
class PlusNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  PlusNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool PlusNode<Int>::evaluate(Error *error,
                             ArrayCRef<Record> records,
                             ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] + this->arg2_values_[i]);
  }
  return true;
}

template <>
class PlusNode<Float> : public BinaryNode<Float, Float, Float> {
 public:
  using Value = Float;
  using Arg1 = Float;
  using Arg2 = Float;

  PlusNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool PlusNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, this->arg1_values_[i] + this->arg2_values_[i]);
  }
  return true;
}

bool PlusNode<Float>::evaluate(Error *error,
                               ArrayCRef<Record> records,
                               ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] + this->arg2_values_[i]);
  }
  return true;
}

// ---- MinusOperator ----

template <typename T> class MinusNode;

template <>
class MinusNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  MinusNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool MinusNode<Int>::evaluate(Error *error,
                              ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] - this->arg2_values_[i]);
  }
  return true;
}

template <>
class MinusNode<Float> : public BinaryNode<Float, Float, Float> {
 public:
  using Value = Float;
  using Arg1 = Float;
  using Arg2 = Float;

  MinusNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool MinusNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, this->arg1_values_[i] - this->arg2_values_[i]);
  }
  return true;
}

bool MinusNode<Float>::evaluate(Error *error,
                                ArrayCRef<Record> records,
                                ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] - this->arg2_values_[i]);
  }
  return true;
}

// ---- MultiplicationOperator ----

template <typename T> class MultiplicationNode;

template <>
class MultiplicationNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  MultiplicationNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool MultiplicationNode<Int>::evaluate(Error *error,
                                       ArrayCRef<Record> records,
                                       ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] * this->arg2_values_[i]);
  }
  return true;
}

template <>
class MultiplicationNode<Float> : public BinaryNode<Float, Float, Float> {
 public:
  using Value = Float;
  using Arg1 = Float;
  using Arg2 = Float;

  MultiplicationNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool MultiplicationNode<Float>::adjust(Error *error,
                                       ArrayRef<Record> records) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, this->arg1_values_[i] * this->arg2_values_[i]);
  }
  return true;
}

bool MultiplicationNode<Float>::evaluate(Error *error,
                                         ArrayCRef<Record> records,
                                         ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] * this->arg2_values_[i]);
  }
  return true;
}

// ---- DivisionOperator ----

template <typename T> class DivisionNode;

template <>
class DivisionNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  DivisionNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool DivisionNode<Int>::evaluate(Error *error,
                                 ArrayCRef<Record> records,
                                 ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    if (this->arg2_values_[i] == 0) {
      GRNXX_ERROR_SET(error, DIVISION_BY_ZERO, "Division by zero");
      return false;
    } else if ((this->arg2_values_[i] == -1) &&
               (this->arg1_values_[i] == numeric_limits<Int>::min())) {
      GRNXX_ERROR_SET(error, DIVISION_OVERFLOW, "Division overflow");
      return false;
    }
    results.set(i, this->arg1_values_[i] / this->arg2_values_[i]);
  }
  return true;
}

template <>
class DivisionNode<Float> : public BinaryNode<Float, Float, Float> {
 public:
  using Value = Float;
  using Arg1 = Float;
  using Arg2 = Float;

  DivisionNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool DivisionNode<Float>::adjust(Error *error,
                                 ArrayRef<Record> records) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, this->arg1_values_[i] / this->arg2_values_[i]);
  }
  return true;
}

bool DivisionNode<Float>::evaluate(Error *error,
                                   ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results.set(i, this->arg1_values_[i] / this->arg2_values_[i]);
  }
  return true;
}

// ---- ModulusOperator ----

template <typename T> class ModulusNode;

template <>
class ModulusNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  ModulusNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool ModulusNode<Int>::evaluate(Error *error,
                                ArrayCRef<Record> records,
                                ArrayRef<Value> results) {
  if (!fill_arg1_values(error, records) ||
      !fill_arg2_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    if (this->arg2_values_[i] == 0) {
      GRNXX_ERROR_SET(error, DIVISION_BY_ZERO, "Division by zero");
      return false;
    } else if ((this->arg2_values_[i] == -1) &&
               (this->arg1_values_[i] == numeric_limits<Int>::min())) {
      GRNXX_ERROR_SET(error, DIVISION_OVERFLOW, "Division overflow");
      return false;
    }
    results.set(i, this->arg1_values_[i] % this->arg2_values_[i]);
  }
  return true;
}

}  // namespace expression

}  // namespace grnxx
