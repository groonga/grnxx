#include "grnxx/impl/expression.hpp"

#include <new>

#include "grnxx/impl/column.hpp"

namespace grnxx {
namespace impl {
namespace expression {

enum NodeType {
  CONSTANT_NODE,
  ROW_ID_NODE,
  SCORE_NODE,
  COLUMN_NODE,
  OPERATOR_NODE
};

// -- Node --

class Node {
 public:
  Node() = default;
  virtual ~Node() = default;

  // Return the node type.
  virtual NodeType node_type() const = 0;
  // Return the result data type.
  virtual DataType data_type() const = 0;
  // Return the reference table.
  virtual const Table *reference_table() const {
    return nullptr;
  }

  // -- Public API (grnxx/expression.hpp) --

  virtual void filter(ArrayCRef<Record>, ArrayRef<Record> *) {
    // Other than TypedNode<Bool> don't support filter().
    throw "Not supported";  // TODO
  }
  virtual void adjust(ArrayRef<Record>) {
    // Other than TypedNode<Float> don't support adjust().
    throw "Not supported";
  }
};


// -- TypedNode --

template <typename T>
class TypedNode : public Node {
 public:
  using Value = T;

  TypedNode() = default;
  virtual ~TypedNode() = default;

  DataType data_type() const {
    return T::type();
  }

//  void filter(ArrayCRef<Record>, ArrayRef<Record> *) {
//    // Other than TypedNode<Bool> don't support filter().
//    throw "Not supported";
//  }

//  void adjust(ArrayRef<Record>) {
//    // Other than TypedNode<Float> don't support adjust().
//    throw "Not supported";
//  }

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into "*results".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;
};

template <>
class TypedNode<Bool> : public Node {
 public:
  using Value = Bool;

  TypedNode() = default;
  virtual ~TypedNode() = default;

  DataType data_type() const {
    return Value::type();
  }

  // NOTE: Derived classes should provide better implementations.
  virtual void filter(ArrayCRef<Record> input_records,
                      ArrayRef<Record> *output_records);

//  void adjust(ArrayRef<Record>) {
//    // Other than TypedNode<Float> don't support adjust().
//    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
//    return false;
//  }

  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;

 private:
  Array<Value> values_for_filter_;
};

void TypedNode<Bool>::filter(ArrayCRef<Record> input_records,
                             ArrayRef<Record> *output_records) {
  if (values_for_filter_.size() < input_records.size()) {
    values_for_filter_.resize(input_records.size());
  }
  evaluate(input_records, values_for_filter_.ref(0, input_records.size()));
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (values_for_filter_[i]) {
      (*output_records)[count] = input_records[i];
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
}

template <>
class TypedNode<Float> : public Node {
 public:
  using Value = Float;

  TypedNode() = default;
  virtual ~TypedNode() = default;

  DataType data_type() const {
    return Value::type();
  }

//  void filter(ArrayCRef<Record>, ArrayRef<Record> *) {
//    // Other than TypedNode<Bool> don't support filter().
//    GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
//    return false;
//  }

  // NOTE: Derived classes should provide better implementations.
  virtual void adjust(ArrayRef<Record> records);

  virtual void evaluate(ArrayCRef<Record> records,
                        ArrayRef<Value> results) = 0;

 private:
  Array<Float> values_for_adjust_;
};

void TypedNode<Float>::adjust(ArrayRef<Record> records) {
  if (values_for_adjust_.size() < records.size()) {
    values_for_adjust_.resize(records.size());
  }
  evaluate(records, values_for_adjust_.ref(0, records.size()));
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].score = values_for_adjust_[i];
  }
}

// -- ConstantNode --

template <typename T>
class ConstantNode : public TypedNode<T> {
 public:
  using Value = T;

  explicit ConstantNode(const Value &value)
      : TypedNode<Value>(),
        value_(value) {}
  ~ConstantNode() = default;

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = value_;
    }
  }

 private:
  Value value_;
};

template <>
class ConstantNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit ConstantNode(Value value) : TypedNode<Value>(), value_(value) {}
  ~ConstantNode() = default;

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Value value_;
};

void ConstantNode<Bool>::filter(ArrayCRef<Record> input_records,
                                ArrayRef<Record> *output_records) {
  if (value_) {
    // Don't copy records if the input/output addresses are the same.
    if (input_records.data() != output_records->data()) {
      for (size_t i = 0; i < input_records.size(); ++i) {
        (*output_records)[i] = input_records[i];
      }
    }
  } else {
    *output_records = output_records->ref(0, 0);
  }
}

void ConstantNode<Bool>::evaluate(ArrayCRef<Record> records,
                                  ArrayRef<Value> results) {
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = value_;
  }
}

template <>
class ConstantNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  explicit ConstantNode(Value value) : TypedNode<Float>(), value_(value) {}
  ~ConstantNode() = default;

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void adjust(ArrayRef<Record> records) {
    for (size_t i = 0; i < records.size(); ++i) {
      records[i].score = value_;
    }
  }
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = value_;
    }
  }

 private:
  Float value_;
};

// TODO: Text will have ownership in future.
template <>
class ConstantNode<Text> : public TypedNode<Text> {
 public:
  using Value = Text;

  explicit ConstantNode(const Value &value)
      : TypedNode<Value>(),
        value_() {
    value_.assign(value.data(), value.size().value());
  }

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = Text(value_.data(), value_.size());
    }
  }

 private:
  String value_;
};

// -- RowIDNode --

class RowIDNode : public TypedNode<Int> {
 public:
  using Value = Int;

  RowIDNode() = default;
  ~RowIDNode() = default;

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = records[i].row_id;
    }
  }
};

// -- ScoreNode --

class ScoreNode : public TypedNode<Float> {
 public:
  using Value = Float;

  ScoreNode() = default;
  ~ScoreNode() = default;

  NodeType node_type() const {
    return SCORE_NODE;
  }

  void adjust(ArrayRef<Record>) {
    // Nothing to do.
  }
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = records[i].score;
    }
  }
};

// -- ColumnNode --

template <typename T>
class ColumnNode : public TypedNode<T> {
 public:
  using Value = T;

  explicit ColumnNode(const ColumnBase *column)
      : TypedNode<Value>(),
        column_(static_cast<const impl::Column<Value> *>(column)) {}
  ~ColumnNode() = default;

  NodeType node_type() const {
    return COLUMN_NODE;
  }
  const Table *reference_table() const {
    return column_->_reference_table();
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    column_->read(records, results);
  }

 private:
  const impl::Column<Value> *column_;
};

template <>
class ColumnNode<Bool> : public TypedNode<Bool> {
 public:
  using Value = Bool;

  explicit ColumnNode(const ColumnBase *column)
      : TypedNode<Value>(),
        column_(static_cast<const impl::Column<Value> *>(column)) {}
  ~ColumnNode() = default;

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    column_->read(records, results);
  }

 private:
  const impl::Column<Value> *column_;
};

void ColumnNode<Bool>::filter(ArrayCRef<Record> input_records,
                              ArrayRef<Record> *output_records) {
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (column_->get(input_records[i].row_id)) {
      (*output_records)[count] = input_records[i];
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
}

template <>
class ColumnNode<Float> : public TypedNode<Float> {
 public:
  using Value = Float;

  explicit ColumnNode(const ColumnBase *column)
      : TypedNode<Value>(),
        column_(static_cast<const impl::Column<Value> *>(column)) {}
  ~ColumnNode() = default;

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  void adjust(ArrayRef<Record> records) {
    for (size_t i = 0; i < records.size(); ++i) {
      records[i].score = column_->get(records[i].row_id);
    }
  }
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    column_->read(records, results);
  }

 private:
  const impl::Column<Value> *column_;
};

// -- OperatorNode --

template <typename T>
class OperatorNode : public TypedNode<T> {
 public:
  using Value = T;

  OperatorNode() = default;
  virtual ~OperatorNode() = default;

  NodeType node_type() const {
    return OPERATOR_NODE;
  }
};

// Evaluate "*arg" for "records".
//
// The evaluation results are stored into "*arg_values".
//
// On failure, throws an exception.
template <typename T>
void fill_node_arg_values(ArrayCRef<Record> records,
                          TypedNode<T> *arg,
                          Array<T> *arg_values) {
  size_t old_size = arg_values->size();
  if (old_size < records.size()) {
    arg_values->resize(records.size());
  }
  switch (arg->node_type()) {
    case CONSTANT_NODE: {
      if (old_size < records.size()) {
        arg->evaluate(records.cref(old_size), arg_values->ref(old_size));
      }
      break;
    }
    default: {
      arg->evaluate(records, arg_values->ref(0, records.size()));
      break;
    }
  }
}

// --- UnaryNode ---

template <typename T, typename U>
class UnaryNode : public OperatorNode<T> {
 public:
  using Value = T;
  using Arg = U;

  explicit UnaryNode(std::unique_ptr<Node> &&arg)
      : OperatorNode<Value>(),
        arg_(static_cast<TypedNode<Arg> *>(arg.release())),
        arg_values_() {}
  virtual ~UnaryNode() = default;

 protected:
  std::unique_ptr<TypedNode<Arg>> arg_;
  Array<Arg> arg_values_;

  void fill_arg_values(ArrayCRef<Record> records) {
    fill_node_arg_values(records, arg_.get(), &arg_values_);
  }
};

// ---- LogicalNotNode ----

class LogicalNotNode : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;
  using Arg = Bool;

  explicit LogicalNotNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)),
        temp_records_() {}
  ~LogicalNotNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

void LogicalNotNode::filter(ArrayCRef<Record> input_records,
                            ArrayRef<Record> *output_records) {
  // TODO: Find the best implementation.

  // Apply an argument filter to "input_records" and store the result to
  // "temp_records_". Then, appends a sentinel to the end.
  temp_records_.resize(input_records.size() + 1);
  ArrayRef<Record> ref = temp_records_.ref();
  arg_->filter(input_records, &ref);
  temp_records_[ref.size()].row_id = Int::na();

  // Extract records which appear in "input_records" and don't appear in "ref".
  size_t count = 0;
  for (size_t i = 0, j = 0; i < input_records.size(); ++i) {
    if (input_records[i].row_id == ref[i].row_id) {
      ++j;
      continue;
    }
    (*output_records)[count] = input_records[i];
    ++count;
  }
  *output_records = output_records->ref(0, count);
}

void LogicalNotNode::evaluate(ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  arg_->evaluate(records, results);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = !results[i];
  }
}

// ---- BitwiseNotNode ----

template <typename T> class BitwiseNotNode;

template <>
class BitwiseNotNode<Bool> : public UnaryNode<Bool, Bool> {
 public:
  using Value = Bool;
  using Arg = Bool;

  explicit BitwiseNotNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~BitwiseNotNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void BitwiseNotNode<Bool>::filter(ArrayCRef<Record> input_records,
                                  ArrayRef<Record> *output_records) {
  fill_arg_values(input_records);
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (!arg_values_[i]) {
      (*output_records)[count] = input_records[i];
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
}

void BitwiseNotNode<Bool>::evaluate(ArrayCRef<Record> records,
                                    ArrayRef<Value> results) {
  arg_->evaluate(records, results);
  // TODO: Should be processed per 8 bytes.
  //       Check the 64-bit boundary and do it!
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = !results[i];
  }
}

template <>
class BitwiseNotNode<Int> : public UnaryNode<Int, Int> {
 public:
  using Value = Int;
  using Arg = Int;

  explicit BitwiseNotNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~BitwiseNotNode() = default;

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void BitwiseNotNode<Int>::evaluate(ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  arg_->evaluate(records, results);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = ~results[i];
  }
}

// ---- PositiveNode ----

// Nothing to do.

// ---- NegativeNode ----

template <typename T>
class NegativeNode : public UnaryNode<T, T> {
 public:
  using Value = T;
  using Arg = T;

  explicit NegativeNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~NegativeNode() = default;

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

template <typename T>
void NegativeNode<T>::evaluate(ArrayCRef<Record> records,
                               ArrayRef<Value> results) {
  this->arg_->evaluate(records, results);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = -results[i];
  }
}

template <>
class NegativeNode<Float> : public UnaryNode<Float, Float> {
 public:
  using Value = Float;
  using Arg = Float;

  explicit NegativeNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~NegativeNode() = default;

  void adjust(ArrayRef<Record> records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void NegativeNode<Float>::adjust(ArrayRef<Record> records) {
  arg_->adjust(records);
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].score = -records[i].score;
  }
}

void NegativeNode<Float>::evaluate(ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  arg_->evaluate(records, results);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = -results[i];
  }
}

// ---- ToIntNode ----

class ToIntNode : public UnaryNode<Int, Float> {
 public:
  using Value = Int;
  using Arg = Float;

  explicit ToIntNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~ToIntNode() = default;

  void evaluate(ArrayCRef<Record> records,
                ArrayRef<Value> results);
};

void ToIntNode::evaluate(ArrayCRef<Record> records,
                         ArrayRef<Value> results) {
  fill_arg_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = arg_values_[i].to_int();
  }
}

// ---- ToFloatNode ----

class ToFloatNode : public UnaryNode<Float, Int> {
 public:
  using Value = Float;
  using Arg = Int;

  explicit ToFloatNode(std::unique_ptr<Node> &&arg)
      : UnaryNode<Value, Arg>(std::move(arg)) {}
  ~ToFloatNode() = default;

  void adjust(ArrayRef<Record> records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void ToFloatNode::adjust(ArrayRef<Record> records) {
  fill_arg_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].score = arg_values_[i].to_float();
  }
}

void ToFloatNode::evaluate(ArrayCRef<Record> records,
                           ArrayRef<Value> results) {
  fill_arg_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = arg_values_[i].to_float();
  }
}

}  // namespace expression

using namespace expression;

// -- Expression --

Expression::Expression(const Table *table,
                       std::unique_ptr<Node> &&root,
                       const ExpressionOptions &options)
    : table_(table),
      root_(std::move(root)),
      block_size_(options.block_size) {}

Expression::~Expression() {}

DataType Expression::data_type() const {
  return root_->data_type();
}

void Expression::filter(Array<Record> *records,
                        size_t input_offset,
                        size_t output_offset,
                        size_t output_limit) {
  ArrayCRef<Record> input = records->cref(input_offset);
  ArrayRef<Record> output = records->ref(input_offset);
  size_t count = 0;
  while ((input.size() > 0) && (output_limit > 0)) {
    size_t next_size = (input.size() < block_size_) ?
                       input.size() : block_size_;
    ArrayCRef<Record> next_input = input.cref(0, next_size);
    ArrayRef<Record> next_output = output.ref(0, next_size);
    root_->filter(next_input, &next_output);
    input = input.cref(next_size);

    if (output_offset > 0) {
      if (output_offset >= next_output.size()) {
        output_offset -= next_output.size();
        next_output = next_output.ref(0, 0);
      } else {
        for (size_t i = output_offset; i < next_output.size(); ++i) {
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
  records->resize(input_offset + count);
}

void Expression::filter(ArrayCRef<Record> input_records,
                        ArrayRef<Record> *output_records) {
  ArrayCRef<Record> input = input_records;
  ArrayRef<Record> output = *output_records;
  size_t count = 0;
  while (input.size() > block_size_) {
    ArrayCRef<Record> input_block = input.cref(0, block_size_);
    ArrayRef<Record> output_block = output.ref(0, block_size_);
    root_->filter(input_block, &output_block);
    input = input.cref(block_size_);
    output = output.ref(output_block.size());
    count += output_block.size();
  }
  root_->filter(input, &output);
  count += output.size();
  *output_records = output_records->ref(0, count);
}

void Expression::adjust(Array<Record> *records, size_t offset) {
  adjust(records->ref(offset));
}

void Expression::adjust(ArrayRef<Record> records) {
  while (records.size() > block_size_) {
    root_->adjust(records.ref(0, block_size_));
    records = records.ref(block_size_);
  }
  root_->adjust(records);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Bool> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Int> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Float> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<GeoPoint> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Text> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Bool> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Int> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Float> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<GeoPoint> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Text> results) {
  _evaluate(records, results);
}

template <typename T>
void Expression::_evaluate(ArrayCRef<Record> records, Array<T> *results) {
  results->resize(records.size());
  _evaluate(records, results->ref());
}

template <typename T>
void Expression::_evaluate(ArrayCRef<Record> records, ArrayRef<T> results) {
  if (T::type() != data_type()) {
    throw "Data type conflict";  // TODO
  }
  if (records.size() != results.size()) {
    throw "Size conflict";  // TODO
  }
  TypedNode<T> *typed_root = static_cast<TypedNode<T> *>(root_.get());
  while (records.size() > block_size_) {
    ArrayCRef<Record> input = records.cref(0, block_size_);
    ArrayRef<T> output = results.ref(0, block_size_);
    typed_root->evaluate(input, output);
    records = records.cref(block_size_);
    results = results.ref(block_size_);
  }
  typed_root->evaluate(records, results);
}

// -- ExpressionBuilder --

ExpressionBuilder::ExpressionBuilder(const Table *table)
    : table_(table),
      node_stack_(),
      subexpression_builder_() {}

ExpressionBuilder::~ExpressionBuilder() {}

void ExpressionBuilder::push_constant(const Datum &datum) {
  if (subexpression_builder_) {
    subexpression_builder_->push_constant(datum);
  } else {
    node_stack_.push_back(std::unique_ptr<Node>(create_constant_node(datum)));
  }
}

void ExpressionBuilder::push_row_id() try {
  if (subexpression_builder_) {
    subexpression_builder_->push_row_id();
  } else {
    node_stack_.push_back(std::unique_ptr<Node>(new RowIDNode()));
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void ExpressionBuilder::push_score() try {
  if (subexpression_builder_) {
    subexpression_builder_->push_score();
  } else {
    node_stack_.push_back(std::unique_ptr<Node>(new ScoreNode()));
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void ExpressionBuilder::push_column(const String &name) {
  if (subexpression_builder_) {
    subexpression_builder_->push_column(name);
  } else {
    node_stack_.push_back(std::unique_ptr<Node>(create_column_node(name)));
  }
}

void ExpressionBuilder::push_operator(OperatorType operator_type) {
  if (subexpression_builder_) {
    subexpression_builder_->push_operator(operator_type);
  } else {
    switch (operator_type) {
      case LOGICAL_NOT_OPERATOR:
      case BITWISE_NOT_OPERATOR:
      case POSITIVE_OPERATOR:
      case NEGATIVE_OPERATOR:
      case TO_INT_OPERATOR:
      case TO_FLOAT_OPERATOR: {
        return push_unary_operator(operator_type);
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
      case MODULUS_OPERATOR:
      case SUBSCRIPT_OPERATOR: {
        return push_binary_operator(operator_type);
      }
      default: {
        throw "Not supported yet";  // TODO
      }
    }
  }
}

void ExpressionBuilder::begin_subexpression() try {
  if (subexpression_builder_) {
    subexpression_builder_->begin_subexpression();
  } else if (node_stack_.is_empty()) {
    throw "No operand";  // TODO
  } else {
    std::unique_ptr<Node> &latest_node = node_stack_.back();
    if (!latest_node->reference_table()) {
      throw "Reference not available";  // TODO
    }
    subexpression_builder_.reset(
        new ExpressionBuilder(latest_node->reference_table()));
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void ExpressionBuilder::end_subexpression(const ExpressionOptions &options) {
  if (!subexpression_builder_) {
    throw "No subexpression";  // TODO
  }
  if (subexpression_builder_->subexpression_builder_) {
    subexpression_builder_->end_subexpression(options);
  } else {
    if (subexpression_builder_->node_stack_.size() != 1) {
      throw "Incomplete subexpression";  // TODO
    }
    node_stack_.push_back(std::move(subexpression_builder_->node_stack_[0]));
    push_dereference(options);
    subexpression_builder_.reset();
  }
}

void ExpressionBuilder::clear() {
  node_stack_.clear();
  subexpression_builder_.reset();
}

std::unique_ptr<ExpressionInterface> ExpressionBuilder::release(
    const ExpressionOptions &options) try {
  if (subexpression_builder_) {
    throw "Incomplete subexpression";  // TODO
  }
  if (node_stack_.size() != 1) {
    throw "Incomplete expression";  // TODO
  }
  std::unique_ptr<Node> root = std::move(node_stack_[0]);
  node_stack_.clear();
  return std::unique_ptr<ExpressionInterface>(
      new Expression(table_, std::move(root), options));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void ExpressionBuilder::push_unary_operator(OperatorType operator_type) {
  if (node_stack_.size() == 0) {
    throw "No operand";  // TODO
  }
  std::unique_ptr<Node> arg = std::move(node_stack_.back());
  node_stack_.pop_back();
  std::unique_ptr<Node> node(
      create_unary_node(operator_type, std::move(arg)));
  node_stack_.push_back(std::move(node));
}

void ExpressionBuilder::push_binary_operator(OperatorType operator_type) {
  if (node_stack_.size() < 2) {
    throw "Not enough operands";  // TODO
  }
  std::unique_ptr<Node> arg1 = std::move(node_stack_[node_stack_.size() - 2]);
  std::unique_ptr<Node> arg2 = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 2);
  std::unique_ptr<Node> node(
      create_binary_node(operator_type, std::move(arg1), std::move(arg2)));
  node_stack_.push_back(std::move(node));
}

void ExpressionBuilder::push_dereference(const ExpressionOptions &options) {
  throw "Not supported yet";  // TODO
}

Node *ExpressionBuilder::create_constant_node(
    const Datum &datum) try {
  switch (datum.type()) {
    case BOOL_DATA: {
      return new ConstantNode<Bool>(datum.as_bool());
    }
    case INT_DATA: {
      return new ConstantNode<Int>(datum.as_int());
    }
    case FLOAT_DATA: {
      return new ConstantNode<Float>(datum.as_float());
    }
    case GEO_POINT_DATA: {
      return new ConstantNode<GeoPoint>(datum.as_geo_point());
    }
    case TEXT_DATA: {
      return new ConstantNode<Text>(datum.as_text());
    }
//    case BOOL_VECTOR_DATA: {
//      return new ConstantNode<Vector<Bool>>(datum.as_bool_vector());
//    }
//    case INT_VECTOR_DATA: {
//      return new ConstantNode<Vector<Int>>(datum.as_int_vector());
//    }
//    case FLOAT_VECTOR_DATA: {
//      return new ConstantNode<Vector<Float>>(datum.as_float_vector());
//    }
//    case GEO_POINT_VECTOR_DATA: {
//      return new ConstantNode<Vector<GeoPoint>>(datum.as_geo_point_vector());
//    }
//    case TEXT_VECTOR_DATA: {
//      return new ConstantNode<Vector<Text>>(datum.as_text_vector());
//    }
    default: {
      throw "Not supported yet";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

Node *ExpressionBuilder::create_column_node(
    const String &name) try {
  ColumnBase *column = table_->find_column(name);
  if (!column) {
    throw "Column not found";  // TODO
  }
  switch (column->data_type()) {
    case BOOL_DATA: {
      return new ColumnNode<Bool>(column);
    }
    case INT_DATA: {
      return new ColumnNode<Int>(column);
    }
    case FLOAT_DATA: {
      return new ColumnNode<Float>(column);
    }
    case GEO_POINT_DATA: {
      return new ColumnNode<GeoPoint>(column);
    }
    case TEXT_DATA: {
      return new ColumnNode<Text>(column);
    }
//    case BOOL_VECTOR_DATA: {
//      return new ColumnNode<Vector<Bool>>(column);
//    }
//    case INT_VECTOR_DATA: {
//      return new ColumnNode<Vector<Int>>(column);
//    }
//    case FLOAT_VECTOR_DATA: {
//      return new ColumnNode<Vector<Float>>(column);
//    }
//    case GEO_POINT_VECTOR_DATA: {
//      return new ColumnNode<Vector<GeoPoint>>(column);
//    }
//    case TEXT_VECTOR_DATA: {
//      return new ColumnNode<Vector<Text>>(column);
//    }
    default: {
      throw "Not supported yet";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

Node *ExpressionBuilder::create_unary_node(
    OperatorType operator_type,
    std::unique_ptr<Node> &&arg) try {
  switch (operator_type) {
    case LOGICAL_NOT_OPERATOR: {
      switch (arg->data_type()) {
        case BOOL_DATA: {
          return new LogicalNotNode(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case BITWISE_NOT_OPERATOR: {
      switch (arg->data_type()) {
        case BOOL_DATA: {
          return new BitwiseNotNode<Bool>(std::move(arg));
        }
        case INT_DATA: {
          return new BitwiseNotNode<Int>(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case POSITIVE_OPERATOR: {
      switch (arg->data_type()) {
        case INT_DATA:
        case FLOAT_DATA: {
          // A positive operator does nothing.
          return arg.release();
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case NEGATIVE_OPERATOR: {
      switch (arg->data_type()) {
        case INT_DATA: {
          return new NegativeNode<Int>(std::move(arg));
        }
        case FLOAT_DATA: {
          return new NegativeNode<Float>(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case TO_INT_OPERATOR: {
      switch (arg->data_type()) {
        case FLOAT_DATA: {
          return new ToIntNode(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case TO_FLOAT_OPERATOR: {
      switch (arg->data_type()) {
        case INT_DATA: {
          return new ToFloatNode(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    default: {
      throw "Not supported yet";
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

Node *ExpressionBuilder::create_binary_node(
    OperatorType operator_type,
    std::unique_ptr<Node> &&arg1,
    std::unique_ptr<Node> &&arg2) try {
  throw "Not supported yet";  // TODO
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace impl
}  // namespace grnxx
