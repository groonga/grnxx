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

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records) {
    // Other than TypedNode<Bool> don't support filter().
    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
    return false;
  }

  bool adjust(Error *error, ArrayRef<Record> records) {
    // Other than TypedNode<Float> don't support adjust().
    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
    return false;
  }

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

template <>
class TypedNode<Bool> : public Node {
 public:
  using Value = Bool;

  TypedNode() : Node() {}
  virtual ~TypedNode() {}

  DataType data_type() const {
    return TypeTraits<Value>::data_type();
  }

  // Derived classes must override this member function.
  virtual bool filter(Error *error,
                      ArrayCRef<Record> input_records,
                      ArrayRef<Record> *output_records) = 0;

  bool adjust(Error *error, ArrayRef<Record> records) {
    // Other than TypedNode<Float> don't support adjust().
    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
    return false;
  }

  virtual bool evaluate(Error *error,
                        ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;
};

//template <>
//bool TypedNode<Bool>::filter(Error *error,
//                             ArrayCRef<Record> input_records,
//                             ArrayRef<Record> *output_records) {
//  // TODO: This implementation should be overridden by derived classes.
//  Array<Bool> results;
//  if (!results.resize(error, input_records.size())) {
//    return false;
//  }
//  if (!evaluate(error, input_records, results)) {
//    return false;
//  }
//  Int count = 0;
//  for (Int i = 0; i < input_records.size(); ++i) {
//    if (results[i]) {
//      output_records->set(count, input_records.get(i));
//      ++count;
//    }
//  }
//  *output_records = output_records->ref(0, count);
//  return true;
//}

template <>
class TypedNode<Float> : public Node {
 public:
  using Value = Float;

  TypedNode() : Node() {}
  virtual ~TypedNode() {}

  DataType data_type() const {
    return TypeTraits<Value>::data_type();
  }

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records) {
    // Other than TypedNode<Bool> don't support filter().
    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
    return false;
  }

  // Derived classes must override this member function.
  virtual bool adjust(Error *error, ArrayRef<Record> records) = 0;

  virtual bool evaluate(Error *error,
                        ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;
};

//template <>
//bool TypedNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
//  // TODO: This implementation should be overridden by derived classes.
//  Array<Float> scores;
//  if (!scores.resize(error, records.size())) {
//    return false;
//  }
//  if (!evaluate(error, records, scores)) {
//    return false;
//  }
//  for (Int i = 0; i < records.size(); ++i) {
//    records.set_score(i, scores[i]);
//  }
//  return true;
//}

// -- DatumNode --

template <typename T>
class DatumNode : public TypedNode<T> {
 public:
  using Value = T;

  static unique_ptr<Node> create(Error *error, Value datum) {
    unique_ptr<Node> node(new (nothrow) DatumNode(datum));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit DatumNode(Value datum)
      : TypedNode<Value>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = datum_;
    }
    return true;
  }

 private:
  T datum_;
};

template <>
class DatumNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  static unique_ptr<Node> create(Error *error, Value datum) {
    unique_ptr<Node> node(new (nothrow) DatumNode(datum));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error, Value datum) {
    unique_ptr<Node> node(new (nothrow) DatumNode(datum));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit DatumNode(Value datum)
      : TypedNode<Float>(),
        datum_(datum) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool adjust(Error *error, ArrayRef<Record> records) {
    for (Int i = 0; i < records.size(); ++i) {
      records.set_score(i, datum_);
    }
    return true;
  }
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = datum_;
    }
    return true;
  }

 private:
  Float datum_;
};

template <>
class DatumNode<Text> : public TypedNode<Text> {
 public:
  using Value = Text;

  static unique_ptr<Node> create(Error *error, Value datum) try {
    return unique_ptr<Node>(new DatumNode(datum));
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }

  explicit DatumNode(Value datum)
      : TypedNode<Value>(),
        datum_(datum.data(), datum.size()) {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    Text datum(datum_.data(), datum_.size());
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = datum;
    }
    return true;
  }

 private:
  std::string datum_;
};

// -- RowIDNode --

class RowIDNode : public TypedNode<Int> {
 public:
  using Value = Int;

  static unique_ptr<Node> create(Error *error, Value datum) {
    unique_ptr<Node> node(new (nothrow) RowIDNode);
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  RowIDNode() : TypedNode<Value>() {}

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = records.get_row_id(i);
    }
    return true;
  }
};

// -- ScoreNode --

class ScoreNode : public TypedNode<Float> {
 public:
  using Value = Float;

  static unique_ptr<Node> create(Error *error, Value datum) {
    unique_ptr<Node> node(new (nothrow) ScoreNode);
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  ScoreNode() : TypedNode<Value>() {}

  NodeType node_type() const {
    return SCORE_NODE;
  }

  bool adjust(Error *error, ArrayRef<Record> records) {
    // Nothing to do.
    return true;
  }
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = records.get_score(i);
    }
    return true;
  }
};

// -- ColumnNode --

template <typename T>
class ColumnNode : public TypedNode<T> {
 public:
  using Value = T;

  static unique_ptr<Node> create(Error *error, const Column *column) {
    unique_ptr<Node> node(new (nothrow) ColumnNode(column));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit ColumnNode(const Column *column)
      : TypedNode<Value>(),
        column_(static_cast<const ColumnImpl<Value> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = column_->get(records.get_row_id(i));
    }
    return true;
  }

 private:
  const ColumnImpl<T> *column_;
};

template <>
class ColumnNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  static unique_ptr<Node> create(Error *error, const Column *column) {
    unique_ptr<Node> node(new (nothrow) ColumnNode(column));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results.set(i, column_->get(records.get_row_id(i)));
    }
    return true;
  }

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

template <>
class ColumnNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  static unique_ptr<Node> create(Error *error, const Column *column) {
    unique_ptr<Node> node(new (nothrow) ColumnNode(column));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit ColumnNode(const Column *column)
      : TypedNode<Value>(),
        column_(static_cast<const ColumnImpl<Value> *>(column)) {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool adjust(Error *error, ArrayRef<Record> records) {
    for (Int i = 0; i < records.size(); ++i) {
      records.set_score(i, column_->get(records.get_row_id(i)));
    }
    return true;
  }
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results) {
    for (Int i = 0; i < records.size(); ++i) {
      results[i] = column_->get(records.get_row_id(i));
    }
    return true;
  }

 private:
  const ColumnImpl<Value> *column_;
};

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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) LogicalNotNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) BitwiseNotNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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
    if (!arg_values_[i]) {
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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseNotNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) NegativeNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) NegativeNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit NegativeNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool NegativeNode<Float>::adjust(Error *error, ArrayRef<Record> records) {
  if (!arg_->adjust(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, -records.get_score(i));
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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) ToIntNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error, unique_ptr<Node> &&arg) {
    unique_ptr<Node> node(new (nothrow) ToFloatNode(std::move(arg)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  explicit ToFloatNode(unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}

  bool adjust(Error *error, ArrayRef<Record> records);
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

bool ToFloatNode::adjust(Error *error, ArrayRef<Record> records) {
  if (!fill_arg_values(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    records.set_score(i, static_cast<Value>(arg_values_[i]));
  }
  return true;
}

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) LogicalAndNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

  LogicalAndNode(unique_ptr<Node> &&arg1, unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}

  bool filter(Error *error,
              ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records) {
    return arg1_->filter(error, input_records, output_records) &&
           arg2_->filter(error, *output_records, output_records);
  }
  bool evaluate(Error *error,
                ArrayCRef<Record> records,
                ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) LogicalOrNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) ComparisonNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

// TODO: EqualNode for Bool should be specialized.

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

// TODO: NotEqualNode for Bool should be specialized.

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseAndNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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
class BitwiseAndNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseAndNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseOrNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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
class BitwiseOrNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseOrNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseXorNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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
class BitwiseXorNode<Int> : public BinaryNode<Int, Int, Int> {
 public:
  using Value = Int;
  using Arg1 = Int;
  using Arg2 = Int;

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) BitwiseXorNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) PlusNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) PlusNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) MinusNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) MinusNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) MultiplicationNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) MultiplicationNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) DivisionNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) DivisionNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

  static unique_ptr<Node> create(Error *error,
                                 unique_ptr<Node> &&arg1,
                                 unique_ptr<Node> &&arg2) {
    unique_ptr<Node> node(
        new (nothrow) ModulusNode(std::move(arg1), std::move(arg2)));
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    }
    return node;
  }

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

using namespace expression;

// -- Expression --

Expression::~Expression() {}

DataType Expression::data_type() const {
  return root_->data_type();
}

//bool Expression::filter(Error *error, Array<Record> *records, Int offset) {
//  ArrayRef<Record> output_records = records->ref();
//  if (!filter(error, *records, &output_records)) {
//    return false;
//  }
//  return records->resize(error, output_records.size());
//}

bool Expression::filter(Error *error,
                        Array<Record> *records,
                        Int input_offset,
                        Int output_offset,
                        Int output_limit) {
  ArrayCRef<Record> input = records->ref(input_offset);
  ArrayRef<Record> output = records->ref(input_offset);
  Int count = 0;
  while ((input.size() > 0) && (output_limit > 0)) {
    Int next_size = (input.size() < block_size_) ? input.size() : block_size_;
    ArrayCRef<Record> next_input = input.ref(0, next_size);
    ArrayRef<Record> next_output = output.ref(0, next_size);
    if (!root_->filter(error, next_input, &next_output)) {
      return false;
    }
    input = input.ref(next_size);

    if (output_offset > 0) {
      if (output_offset >= next_output.size()) {
        output_offset -= next_output.size();
        next_output = next_output.ref(0, 0);
      } else {
        for (Int i = output_offset; i < next_output.size(); ++i) {
          next_output.set(i - output_offset, next_output[i]);
        }
        next_output = next_output.ref(0, next_output.size() - output_offset);
        output_offset = 0;
      }
    }
    if (next_output.size() > output_limit) {
      next_output = next_output.ref(0, output_limit);
    }
    output_limit -= next_output.size();

    output = output.ref(next_output.size());
    count += next_output.size();
  }
  records->resize(nullptr, input_offset + count);
  return true;

}

bool Expression::filter(Error *error,
                        ArrayCRef<Record> input_records,
                        ArrayRef<Record> *output_records) {
  ArrayCRef<Record> input = input_records;
  ArrayRef<Record> output = *output_records;
  Int count = 0;
  while (input.size() > block_size_) {
    ArrayCRef<Record> input_block = input.ref(0, block_size_);
    ArrayRef<Record> output_block = output.ref(0, block_size_);
    if (!root_->filter(error, input_block, &output_block)) {
      return false;
    }
    input = input.ref(block_size_);
    output = output.ref(output_block.size());
    count += output_block.size();
  }
  if (!root_->filter(error, input, &output)) {
    return false;
  }
  count += output.size();
  *output_records = output_records->ref(0, count);
  return true;
}

bool Expression::adjust(Error *error, Array<Record> *records, Int offset) {
  return adjust(error, records->ref(offset));
}

bool Expression::adjust(Error *error, ArrayRef<Record> records) {
  while (records.size() > block_size_) {
    if (!root_->adjust(error, records.ref(0, block_size_))) {
      return false;
    }
    records = records.ref(block_size_);
  }
  return root_->adjust(error, records);
}

template <typename T>
bool Expression::evaluate(Error *error,
                          ArrayCRef<Record> records,
                          Array<T> *results) {
  if (!results->resize(error, records.size())) {
    return false;
  }
  return evaluate(error, records, results->ref());
}

#define GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(type) \
  template bool Expression::evaluate(Error *error, \
                                     ArrayCRef<Record> records, \
                                     Array<type> *results)
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(Bool);
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(Int);
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(Float);
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(Time);
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(GeoPoint);
GRNXX_INSTANTIATE_EXPRESSION_EVALUATE(Text);
#undef GRNXX_INSTANTIATE_EXPRESSION_EVALUATE

template <typename T>
bool Expression::evaluate(Error *error,
                          ArrayCRef<Record> records,
                          ArrayRef<T> results) {
  if (TypeTraits<T>::data_type() != data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid data type");
    return false;
  }
  if (records.size() != results.size()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Size conflict: "
                    "#records = %" PRIi64 ", #results = %" PRIi64,
                    records.size(), results.size());
    return false;
  }
  auto typed_root = static_cast<TypedNode<T> *>(root_.get());
  while (records.size() > block_size_) {
    ArrayCRef<Record> input = records.ref(0, block_size_);
    ArrayRef<T> output = results.ref(0, block_size_);
    if (!typed_root->evaluate(error, input, output)) {
      return false;
    }
    records = records.ref(block_size_);
    results = results.ref(block_size_);
  }
  return typed_root->evaluate(error, records, results);
}

unique_ptr<Expression> Expression::create(Error *error,
                                          const Table *table,
                                          unique_ptr<Node> &&root,
                                          const ExpressionOptions &options) {
  if (options.block_size <= 0) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT,
                    "Invalid argument: block_size = %" PRIi64,
                    options.block_size);
    return nullptr;
  }
  unique_ptr<Expression> expression(
      new (nothrow) Expression(table, std::move(root), options.block_size));
  if (!expression) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return expression;
}

Expression::Expression(const Table *table,
                       unique_ptr<Node> &&root,
                       Int block_size)
    : table_(table),
      root_(std::move(root)),
      block_size_(block_size) {}

// -- ExpressionBuilder --

unique_ptr<ExpressionBuilder> ExpressionBuilder::create(Error *error,
                                                        const Table *table) {
  unique_ptr<ExpressionBuilder> builder(
      new (nothrow) ExpressionBuilder(table));
  if (!builder) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return builder;
}

ExpressionBuilder::~ExpressionBuilder() {}

bool ExpressionBuilder::push_datum(Error *error, const Datum &datum) {
  // Reserve a space for a new node.
  if (!stack_.reserve(error, stack_.size() + 1)) {
    return false;
  }
  unique_ptr<Node> node = create_datum_node(error, datum);
  if (!node) {
    return false;
  }
  // This push_back() must not fail because a space is already reserved.
  stack_.push_back(nullptr, std::move(node));
  return true;
}

bool ExpressionBuilder::push_column(Error *error, String name) {
  // Reserve a space for a new node.
  if (!stack_.reserve(error, stack_.size() + 1)) {
    return false;
  }
  unique_ptr<Node> node = create_column_node(error, name);
  if (!node) {
    return false;
  }
  // This push_back() must not fail because a space is already reserved.
  stack_.push_back(nullptr, std::move(node));
  return true;
}

bool ExpressionBuilder::push_operator(Error *error,
                                      OperatorType operator_type) {
  switch (operator_type) {
    case LOGICAL_NOT_OPERATOR:
    case BITWISE_NOT_OPERATOR:
    case POSITIVE_OPERATOR:
    case NEGATIVE_OPERATOR:
    case TO_INT_OPERATOR:
    case TO_FLOAT_OPERATOR: {
      return push_unary_operator(error, operator_type);
    }
    case LOGICAL_AND_OPERATOR:
    case LOGICAL_OR_OPERATOR:
    case EQUAL_OPERATOR:
    case NOT_EQUAL_OPERATOR:
    case LESS_OPERATOR:
    case LESS_EQUAL_OPERATOR:
    case GREATER_OPERATOR:
    case GREATER_EQUAL_OPERATOR:
    case BITWISE_AND_OPERATOR:
    case BITWISE_OR_OPERATOR:
    case BITWISE_XOR_OPERATOR:
    case PLUS_OPERATOR:
    case MINUS_OPERATOR:
    case MULTIPLICATION_OPERATOR:
    case DIVISION_OPERATOR:
    case MODULUS_OPERATOR: {
      return push_binary_operator(error, operator_type);
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
}

void ExpressionBuilder::clear() {
  stack_.clear();
}

unique_ptr<Expression> ExpressionBuilder::release(
    Error *error,
    const ExpressionOptions &options) {
  if (stack_.size() != 1) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Incomplete expression");
    return nullptr;
  }
  unique_ptr<Node> root = std::move(stack_[0]);
  stack_.clear();
  return Expression::create(error, table_, std::move(root), options);
}

ExpressionBuilder::ExpressionBuilder(const Table *table)
    : table_(table),
      stack_() {}

unique_ptr<Node> ExpressionBuilder::create_datum_node(
    Error *error,
    const Datum &datum) {
  switch (datum.type()) {
    case BOOL_DATA: {
      return DatumNode<Bool>::create(error, datum.force_bool());
    }
    case INT_DATA: {
      return DatumNode<Int>::create(error, datum.force_int());
    }
    case FLOAT_DATA: {
      return DatumNode<Float>::create(error, datum.force_float());
    }
    case TIME_DATA: {
      return DatumNode<Time>::create(error, datum.force_time());
    }
    case GEO_POINT_DATA: {
      return DatumNode<GeoPoint>::create(error, datum.force_geo_point());
    }
    case TEXT_DATA: {
      return DatumNode<Text>::create(error, datum.force_text());
    }
    default: {
      // TODO: Other types are not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

unique_ptr<Node> ExpressionBuilder::create_column_node(
    Error *error,
    String name) {
  if (name == "_id") {
    // Create a node associated with row IDs of records.
    unique_ptr<Node> node(new (nothrow) RowIDNode());
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return nullptr;
    }
    return node;
  }

  if (name == "_score") {
    // Create a node associated with scores of records.
    unique_ptr<Node> node(new (nothrow) ScoreNode());
    if (!node) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return nullptr;
    }
    return node;
  }

  // Create a node associated with a column.
  Column *column = table_->find_column(error, name);
  if (!column) {
    return nullptr;
  }
  switch (column->data_type()) {
    case BOOL_DATA: {
      return ColumnNode<Bool>::create(error, column);
    }
    case INT_DATA: {
      return ColumnNode<Int>::create(error, column);
    }
    case FLOAT_DATA: {
      return ColumnNode<Float>::create(error, column);
    }
    case TIME_DATA: {
      return ColumnNode<Time>::create(error, column);
    }
    case GEO_POINT_DATA: {
      return ColumnNode<GeoPoint>::create(error, column);
    }
    case TEXT_DATA: {
      return ColumnNode<Text>::create(error, column);
    }
    default: {
      // TODO: Other types are not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

bool ExpressionBuilder::push_unary_operator(Error *error,
                                            OperatorType operator_type) {
  if (stack_.size() < 1) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough operands");
    return false;
  }
  unique_ptr<Node> arg = std::move(stack_[stack_.size() - 1]);
  stack_.resize(nullptr, stack_.size() - 1);
  unique_ptr<Node> node =
      create_unary_node(error, operator_type, std::move(arg));
  if (!node) {
    return false;
  }
  stack_.push_back(error, std::move(node));
  return true;
}

bool ExpressionBuilder::push_binary_operator(Error *error,
                                             OperatorType operator_type) {
  if (stack_.size() < 2) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough operands");
    return false;
  }
  unique_ptr<Node> arg1 = std::move(stack_[stack_.size() - 2]);
  unique_ptr<Node> arg2 = std::move(stack_[stack_.size() - 1]);
  stack_.resize(nullptr, stack_.size() - 2);
  unique_ptr<Node> node = create_binary_node(error, operator_type,
                                             std::move(arg1), std::move(arg2));
  if (!node) {
    return false;
  }
  stack_.push_back(nullptr, std::move(node));
  return true;
}

unique_ptr<Node> ExpressionBuilder::create_unary_node(
    Error *error,
    OperatorType operator_type,
    unique_ptr<Node> &&arg) {
  switch (operator_type) {
    case LOGICAL_NOT_OPERATOR: {
      if (arg->data_type() != BOOL_DATA) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return LogicalNotNode::create(error, std::move(arg));
    }
    case BITWISE_NOT_OPERATOR: {
      switch (arg->data_type()) {
        case BOOL_DATA: {
          return BitwiseNotNode<Bool>::create(error, std::move(arg));
        }
        case INT_DATA: {
          return BitwiseNotNode<Int>::create(error, std::move(arg));
        }
        default: {
          GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
          return nullptr;
        }
      }
    }
    case POSITIVE_OPERATOR: {
      if ((arg->data_type() != INT_DATA) && (arg->data_type() != FLOAT_DATA)) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      // Positive operator does nothing.
      return std::move(arg);
    }
    case NEGATIVE_OPERATOR: {
      switch (arg->data_type()) {
        case INT_DATA: {
          return NegativeNode<Int>::create(error, std::move(arg));
        }
        case FLOAT_DATA: {
          return NegativeNode<Float>::create(error, std::move(arg));
        }
        default: {
          GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
          return nullptr;
        }
      }
    }
    case TO_INT_OPERATOR: {
      if (arg->data_type() != FLOAT_DATA) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return ToIntNode::create(error, std::move(arg));
    }
    case TO_FLOAT_OPERATOR: {
      if (arg->data_type() != INT_DATA) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return ToFloatNode::create(error, std::move(arg));
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

unique_ptr<Node> ExpressionBuilder::create_binary_node(
    Error *error,
    OperatorType operator_type,
    unique_ptr<Node> &&arg1,
    unique_ptr<Node> &&arg2) {
  switch (operator_type) {
    case LOGICAL_AND_OPERATOR: {
      if ((arg1->data_type() != BOOL_DATA) ||
          (arg2->data_type() != BOOL_DATA)) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return LogicalAndNode::create(error, std::move(arg1), std::move(arg2));
    }
    case LOGICAL_OR_OPERATOR: {
      if ((arg1->data_type() != BOOL_DATA) ||
          (arg2->data_type() != BOOL_DATA)) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return LogicalOrNode::create(error, std::move(arg1), std::move(arg2));
    }
    case EQUAL_OPERATOR: {
      return create_equality_test_node<Equal>(
          error, std::move(arg1), std::move(arg2));
    }
    case NOT_EQUAL_OPERATOR: {
      return create_equality_test_node<NotEqual>(
          error, std::move(arg1), std::move(arg2));
    }
    case LESS_OPERATOR: {
      return create_comparison_node<Less>(
          error, std::move(arg1), std::move(arg2));
    }
    case LESS_EQUAL_OPERATOR: {
      return create_comparison_node<LessEqual>(
          error, std::move(arg1), std::move(arg2));
    }
    case GREATER_OPERATOR: {
      return create_comparison_node<Greater>(
          error, std::move(arg1), std::move(arg2));
    }
    case GREATER_EQUAL_OPERATOR: {
      return create_comparison_node<GreaterEqual>(
          error, std::move(arg1), std::move(arg2));
    }
    case BITWISE_AND_OPERATOR:
    case BITWISE_OR_OPERATOR:
    case BITWISE_XOR_OPERATOR: {
      switch (arg1->data_type()) {
        case BOOL_DATA: {
          return create_bitwise_node<Bool>(
              error, operator_type, std::move(arg1), std::move(arg2));
        }
        case INT_DATA: {
          return create_bitwise_node<Int>(
              error, operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
          return nullptr;
        }
      }
    }
    case PLUS_OPERATOR:
    case MINUS_OPERATOR:
    case MULTIPLICATION_OPERATOR:
    case DIVISION_OPERATOR: {
      switch (arg1->data_type()) {
        case INT_DATA: {
          return create_arithmetic_node<Int>(
              error, operator_type, std::move(arg1), std::move(arg2));
        }
        case FLOAT_DATA: {
          return create_arithmetic_node<Float>(
              error, operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
          return nullptr;
        }
      }
    }
    case MODULUS_OPERATOR: {
      if ((arg1->data_type() != INT_DATA) ||
          (arg2->data_type() != INT_DATA)) {
        GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
        return nullptr;
      }
      return ModulusNode<Int>::create(error, std::move(arg1), std::move(arg2));
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

template <typename T>
unique_ptr<Node> ExpressionBuilder::create_equality_test_node(
    Error *error,
    unique_ptr<Node> &&arg1,
    unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return nullptr;
  }
  switch (arg1->data_type()) {
    case BOOL_DATA: {
      return ComparisonNode<typename T:: template Comparer<Bool>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case INT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Int>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case FLOAT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Float>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case TIME_DATA: {
      return ComparisonNode<typename T:: template Comparer<Time>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case GEO_POINT_DATA: {
      return ComparisonNode<typename T:: template Comparer<GeoPoint>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case TEXT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Text>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    // TODO: Support other types.
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
}

template <typename T>
unique_ptr<Node> ExpressionBuilder::create_comparison_node(
    Error *error,
    unique_ptr<Node> &&arg1,
    unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return nullptr;
  }
  switch (arg1->data_type()) {
    case INT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Int>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case FLOAT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Float>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case TIME_DATA: {
      return ComparisonNode<typename T:: template Comparer<Time>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case TEXT_DATA: {
      return ComparisonNode<typename T:: template Comparer<Text>>::create(
          error, std::move(arg1), std::move(arg2));
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return nullptr;
    }
  }
}

template <typename T>
unique_ptr<Node> ExpressionBuilder::create_bitwise_node(
    Error *error,
    OperatorType operator_type,
    unique_ptr<Node> &&arg1,
    unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return nullptr;
  }
  switch (operator_type) {
    case BITWISE_AND_OPERATOR: {
      return BitwiseAndNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case BITWISE_OR_OPERATOR: {
      return BitwiseOrNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case BITWISE_XOR_OPERATOR: {
      return BitwiseXorNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid operator");
      return nullptr;
    }
  }
}

template <typename T>
unique_ptr<Node> ExpressionBuilder::create_arithmetic_node(
    Error *error,
    OperatorType operator_type,
    unique_ptr<Node> &&arg1,
    unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return nullptr;
  }
  switch (operator_type) {
    case PLUS_OPERATOR: {
      return PlusNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case MINUS_OPERATOR: {
      return MinusNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case MULTIPLICATION_OPERATOR: {
      return MultiplicationNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    case DIVISION_OPERATOR: {
      return DivisionNode<T>::create(
          error, std::move(arg1), std::move(arg2));
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return nullptr;
    }
  }
}

}  // namespace grnxx
