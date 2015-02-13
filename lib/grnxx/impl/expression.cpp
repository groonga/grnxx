#include "grnxx/impl/expression.hpp"

#include <new>

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

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into "*results".
  //
  // On failure, throws an exception.
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
    if (values_for_filter_[i].is_true()) {
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
  if (value_.is_true()) {
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
    value_.assign(value.raw_data(), value.raw_size());
  }
  ~ConstantNode() = default;

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

// TODO: Vector will have ownership in future.
template <typename T>
class ConstantNode<Vector<T>> : public TypedNode<Vector<T>> {
 public:
  using Value = Vector<T>;

  explicit ConstantNode(const Value &value)
      : TypedNode<Value>(),
        value_() {
    size_t value_size = value.raw_size();
    value_.resize(value_size);
    for (size_t i = 0; i < value_size; ++i) {
      value_[i] = value[i];
    }
  }
  ~ConstantNode() = default;

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = Value(value_.data(), value_.size());
    }
  }

 private:
  Array<T> value_;
};

// TODO: Vector will have ownership in future.
template <>
class ConstantNode<Vector<Text>> : public TypedNode<Vector<Text>> {
 public:
  using Value = Vector<Text>;

  explicit ConstantNode(const Value &value)
      : TypedNode<Value>(),
        value_(),
        bodies_() {
    size_t value_size = value.raw_size();
    value_.resize(value_size);
    bodies_.resize(value_size);
    for (size_t i = 0; i < value_size; ++i) {
      bodies_[i].assign(value[i].raw_data(), value[i].raw_size());
      value_[i] = Text(bodies_[i].data(), bodies_[i].size());
    }
  }
  ~ConstantNode() = default;

  NodeType node_type() const {
    return CONSTANT_NODE;
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results) {
    for (size_t i = 0; i < records.size(); ++i) {
      results[i] = Value(value_.data(), value_.size());
    }
  }

 private:
  Array<Text> value_;
  Array<String> bodies_;
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
    if (column_->get(input_records[i].row_id).is_true()) {
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

  // Fill "arg_values_" with the evaluation results of "arg_".
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
    if (input_records[i].row_id.match(ref[j].row_id)) {
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
    if (!arg_values_[i].is_true()) {
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

// --- BinaryNode ---

template <typename T, typename U, typename V>
class BinaryNode : public OperatorNode<T> {
 public:
  using Value = T;
  using Arg1 = U;
  using Arg2 = V;

  BinaryNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : OperatorNode<Value>(),
        arg1_(static_cast<TypedNode<Arg1> *>(arg1.release())),
        arg2_(static_cast<TypedNode<Arg2> *>(arg2.release())),
        arg1_values_(),
        arg2_values_() {}
  virtual ~BinaryNode() = default;

 protected:
  std::unique_ptr<TypedNode<Arg1>> arg1_;
  std::unique_ptr<TypedNode<Arg2>> arg2_;
  Array<Arg1> arg1_values_;
  Array<Arg2> arg2_values_;

  // Fill "arg1_values_" with the evaluation results of "arg1_".
  void fill_arg1_values(ArrayCRef<Record> records) {
    fill_node_arg_values(records, arg1_.get(), &arg1_values_);
  }
  // Fill "arg2_values_" with the evaluation results of "arg2_".
  void fill_arg2_values(ArrayCRef<Record> records) {
    fill_node_arg_values(records, arg2_.get(), &arg2_values_);
  }
};

// ---- LogicalAndNode ----

class LogicalAndNode : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  LogicalAndNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}
  ~LogicalAndNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records) {
    arg1_->filter(input_records, output_records);
    arg2_->filter(*output_records, output_records);
  }
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

void LogicalAndNode::evaluate(ArrayCRef<Record> records,
                              ArrayRef<Value> results) {
  // Evaluate "arg1" for all the records.
  // Then, evaluate "arg2" for non-false records.
  arg1_->evaluate(records, results);
  if (temp_records_.size() < records.size()) {
    temp_records_.resize(records.size());
  }
  size_t count = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (!results[i].is_false()) {
      temp_records_[count] = records[i];
      ++count;
    }
  }
  if (count == 0) {
    // Nothing to do.
    return;
  }
  fill_arg2_values(temp_records_.cref(0, count));

  // Merge the evaluation results.
  count = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (!results[i].is_false()) {
      results[i] &= arg2_values_[count];
      ++count;
    }
  }
}

// ---- LogicalOrNode ----

class LogicalOrNode : public BinaryNode<Bool, Bool, Bool> {
 public:
  using Value = Bool;
  using Arg1 = Bool;
  using Arg2 = Bool;

  LogicalOrNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}
  ~LogicalOrNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

void LogicalOrNode::filter(ArrayCRef<Record> input_records,
                           ArrayRef<Record> *output_records) {
  // Evaluate "arg1" for all the records.
  // Then, evaluate "arg2" for non-true records.
  fill_arg1_values(input_records);
  if (temp_records_.size() < input_records.size()) {
    temp_records_.resize(input_records.size());
  }
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (!arg1_values_[i].is_true()) {
      temp_records_[count] = input_records[i];
      ++count;
    }
  }
  if (count == 0) {
    if (input_records.data() != output_records->data()) {
      for (size_t i = 0; i < input_records.size(); ++i) {
        (*output_records)[i] = input_records[i];
      }
    }
    return;
  }
  fill_arg2_values(temp_records_.cref(0, count));

  // Merge the evaluation results.
  count = 0;
  size_t output_count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (arg1_values_[i].is_true()) {
      (*output_records)[output_count] = input_records[i];
      ++output_count;
    } else {
      if (arg2_values_[count].is_true()) {
        (*output_records)[output_count] = input_records[i];
        ++output_count;
      }
      ++count;
    }
  }
  *output_records = output_records->ref(0, output_count);
}

void LogicalOrNode::evaluate(ArrayCRef<Record> records,
                             ArrayRef<Value> results) {
  // Evaluate "arg1" for all the records.
  // Then, evaluate "arg2" for non-true records.
  arg1_->evaluate(records, results);
  if (temp_records_.size() < records.size()) {
    temp_records_.resize(records.size());
  }
  size_t count = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (!results[i].is_true()) {
      temp_records_[count] = records[i];
      ++count;
    }
  }
  if (count == 0) {
    // Nothing to do.
    return;
  }
  fill_arg2_values(temp_records_.cref(0, count));

  // Merge the evaluation results.
  count = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (!results[i].is_true()) {
      results[i] |= arg2_values_[count];
      ++count;
    }
  }
}

// ---- GenericBinaryNode ----

template <typename T,
          typename U = typename T::Value,
          typename V = typename T::Arg1,
          typename W = typename T::Arg2>
class GenericBinaryNode : public BinaryNode<U, V, W> {
 public:
  using Operator = T;
  using Value = U;
  using Arg1 = V;
  using Arg2 = W;

  GenericBinaryNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        operator_() {}
  ~GenericBinaryNode() = default;

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Operator operator_;
};

template <typename T, typename U, typename V, typename W>
void GenericBinaryNode<T, U, V, W>::evaluate(ArrayCRef<Record> records,
                                             ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = operator_(this->arg1_values_[i], this->arg2_values_[i]);
  }
}

template <typename T, typename V, typename W>
class GenericBinaryNode<T, Bool, V, W> : public BinaryNode<Bool, V, W> {
 public:
  using Operator = T;
  using Value = Bool;
  using Arg1 = V;
  using Arg2 = W;

  GenericBinaryNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        operator_() {}
  ~GenericBinaryNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Operator operator_;
};

template <typename T, typename V, typename W>
void GenericBinaryNode<T, Bool, V, W>::filter(
    ArrayCRef<Record> input_records,
    ArrayRef<Record> *output_records) {
  this->fill_arg1_values(input_records);
  this->fill_arg2_values(input_records);
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (operator_(this->arg1_values_[i], this->arg2_values_[i]).is_true()) {
      (*output_records)[count] = input_records[i];
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
}

template <typename T, typename V, typename W>
void GenericBinaryNode<T, Bool, V, W>::evaluate(ArrayCRef<Record> records,
                                                ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  // TODO: Should be processed per 64 bits.
  //       Check the 64-bit boundary and do it!
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = operator_(this->arg1_values_[i], this->arg2_values_[i]);
  }
}

template <typename T, typename V, typename W>
class GenericBinaryNode<T, Float, V, W> : public BinaryNode<Float, V, W> {
 public:
  using Operator = T;
  using Value = Float;
  using Arg1 = V;
  using Arg2 = W;

  GenericBinaryNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        operator_() {}
  ~GenericBinaryNode() = default;

  void adjust(ArrayRef<Record> records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Operator operator_;
};

template <typename T, typename V, typename W>
void GenericBinaryNode<T, Float, V, W>::evaluate(ArrayCRef<Record> records,
                                                 ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = operator_(this->arg1_values_[i], this->arg2_values_[i]);
  }
}

template <typename T, typename V, typename W>
void GenericBinaryNode<T, Float, V, W>::adjust(ArrayRef<Record> records) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].score = operator_(this->arg1_values_[i], this->arg2_values_[i]);
  }
}

// ----- EqualNode -----

template <typename T>
struct EqualOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 == arg2;
  }
};

template <typename T>
using EqualNode = GenericBinaryNode<EqualOperator<T>>;

// ----- NotEqualNode -----

template <typename T>
struct NotEqualOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 != arg2;
  }
};

template <typename T>
using NotEqualNode = GenericBinaryNode<NotEqualOperator<T>>;

// ----- LessNode -----

template <typename T>
struct LessOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 < arg2;
  }
};

template <typename T>
using LessNode = GenericBinaryNode<LessOperator<T>>;

// ----- LessEqualNode -----

template <typename T>
struct LessEqualOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 <= arg2;
  }
};

template <typename T>
using LessEqualNode = GenericBinaryNode<LessEqualOperator<T>>;

// ----- GreaterNode -----

template <typename T>
struct GreaterOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 > arg2;
  }
};

template <typename T>
using GreaterNode = GenericBinaryNode<GreaterOperator<T>>;

// ----- GreaterEqualNode -----

template <typename T>
struct GreaterEqualOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1 >= arg2;
  }
};

template <typename T>
using GreaterEqualNode = GenericBinaryNode<GreaterEqualOperator<T>>;

// ----- BitwiseAndNode -----

template <typename T>
struct BitwiseAndOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 & arg2;
  }
};

template <typename T>
using BitwiseAndNode = GenericBinaryNode<BitwiseAndOperator<T>>;

// ----- BitwiseOrNode -----

template <typename T>
struct BitwiseOrOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 | arg2;
  }
};

template <typename T>
using BitwiseOrNode = GenericBinaryNode<BitwiseOrOperator<T>>;

// ----- BitwiseXorNode -----

template <typename T>
struct BitwiseXorOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 ^ arg2;
  }
};

template <typename T>
using BitwiseXorNode = GenericBinaryNode<BitwiseXorOperator<T>>;

// ----- PlusNode -----

template <typename T>
struct PlusOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 + arg2;
  }
};

template <typename T>
using PlusNode = GenericBinaryNode<PlusOperator<T>>;

// ----- MinusNode -----

template <typename T>
struct MinusOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 - arg2;
  }
};

template <typename T>
using MinusNode = GenericBinaryNode<MinusOperator<T>>;

// ----- MultiplicationNode -----

template <typename T>
struct MultiplicationOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 * arg2;
  }
};

template <typename T>
using MultiplicationNode = GenericBinaryNode<MultiplicationOperator<T>>;

// ----- DivisionNode -----

template <typename T>
struct DivisionOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 / arg2;
  }
};

template <typename T>
using DivisionNode = GenericBinaryNode<DivisionOperator<T>>;

// ----- ModulusNode -----

template <typename T>
struct ModulusOperator {
  using Value = T;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(Arg1 arg1, Arg2 arg2) const {
    return arg1 % arg2;
  }
};

template <typename T>
using ModulusNode = GenericBinaryNode<ModulusOperator<T>>;

// ----- StartsWithNode -----

template <typename T>
struct StartsWithOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1.starts_with(arg2);
  }
};

template <typename T>
using StartsWithNode = GenericBinaryNode<StartsWithOperator<T>>;

// ----- EndsWithNode -----

template <typename T>
struct EndsWithOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1.ends_with(arg2);
  }
};

template <typename T>
using EndsWithNode = GenericBinaryNode<EndsWithOperator<T>>;

// ----- ContainsNode -----

template <typename T>
struct ContainsOperator {
  using Value = Bool;
  using Arg1 = T;
  using Arg2 = T;
  Value operator()(const Arg1 &arg1, const Arg2 &arg2) const {
    return arg1.contains(arg2);
  }
};

template <typename T>
using ContainsNode = GenericBinaryNode<ContainsOperator<T>>;

// ---- SubscriptNode ----

template <typename T>
class SubscriptNode : public BinaryNode<T, Vector<T>, Int> {
 public:
  using Value = T;
  using Arg1 = Vector<T>;
  using Arg2 = Int;

  SubscriptNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}
  ~SubscriptNode() = default;

  const Table *reference_table() const {
    return this->arg1_->reference_table();
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

template <typename T>
void SubscriptNode<T>::evaluate(ArrayCRef<Record> records,
                                ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = this->arg1_values_[i][this->arg2_values_[i]];
  }
}

template <>
class SubscriptNode<Bool> : public BinaryNode<Bool, Vector<Bool>, Int> {
 public:
  using Value = Bool;
  using Arg1 = Vector<Bool>;
  using Arg2 = Int;

  SubscriptNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}
  ~SubscriptNode() = default;

  void filter(ArrayCRef<Record> input_records,
              ArrayRef<Record> *output_records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void SubscriptNode<Bool>::filter(ArrayCRef<Record> input_records,
                                 ArrayRef<Record> *output_records) {
  this->fill_arg1_values(input_records);
  this->fill_arg2_values(input_records);
  size_t count = 0;
  for (size_t i = 0; i < input_records.size(); ++i) {
    if (this->arg1_values_[i][this->arg2_values_[i]].is_true()) {
      (*output_records)[count] = input_records[i];
      ++count;
    }
  }
  *output_records = output_records->ref(0, count);
}

void SubscriptNode<Bool>::evaluate(ArrayCRef<Record> records,
                                   ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = this->arg1_values_[i][this->arg2_values_[i]];
  }
}

template <>
class SubscriptNode<Float> : public BinaryNode<Float, Vector<Float>, Int> {
 public:
  using Value = Float;
  using Arg1 = Vector<Float>;
  using Arg2 = Int;

  SubscriptNode(std::unique_ptr<Node> &&arg1, std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)) {}
  ~SubscriptNode() = default;

  void adjust(ArrayRef<Record> records);
  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);
};

void SubscriptNode<Float>::adjust(ArrayRef<Record> records) {
  fill_arg1_values(records);
  fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].score = this->arg1_values_[i][this->arg2_values_[i]];
  }
}

void SubscriptNode<Float>::evaluate(ArrayCRef<Record> records,
                                    ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  this->fill_arg2_values(records);
  for (size_t i = 0; i < records.size(); ++i) {
    results[i] = this->arg1_values_[i][this->arg2_values_[i]];
  }
}

// ---- DereferenceNode ----

template <typename T>
class DereferenceNode : public BinaryNode<T, Int, T> {
 public:
  using Value = T;
  using Arg1 = Int;
  using Arg2 = T;

  DereferenceNode(std::unique_ptr<Node> &&arg1,
                  std::unique_ptr<Node> &&arg2)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_() {}
  ~DereferenceNode() = default;

  const Table *reference_table() const {
    return this->arg1_->reference_table();
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
};

template <typename T>
void DereferenceNode<T>::evaluate(ArrayCRef<Record> records,
                                  ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  if (temp_records_.size() < records.size()) {
    temp_records_.resize(records.size());
  }
  for (size_t i = 0; i < records.size(); ++i) {
    temp_records_[i].row_id = this->arg1_values_[i];
    temp_records_[i].score = records[i].score;
  }
  this->arg2_->evaluate(temp_records_.ref(0, records.size()), results);
}

// ---- VectorDereferenceNode ----

template <typename T>
class VectorDereferenceNode : public BinaryNode<Vector<T>, Vector<Int>, T> {
 public:
  using Value = Vector<T>;
  using Arg1 = Vector<Int>;
  using Arg2 = T;

  VectorDereferenceNode(std::unique_ptr<Node> &&arg1,
                        std::unique_ptr<Node> &&arg2,
                        const ExpressionOptions &options)
      : BinaryNode<Value, Arg1, Arg2>(std::move(arg1), std::move(arg2)),
        temp_records_(),
        result_pools_(),
        block_size_(options.block_size) {}
  ~VectorDereferenceNode() = default;

  const Table *reference_table() const {
    return this->arg1_->reference_table();
  }

  void evaluate(ArrayCRef<Record> records, ArrayRef<Value> results);

 private:
  Array<Record> temp_records_;
  Array<Array<Arg2>> result_pools_;
  size_t block_size_;
};

template <typename T>
void VectorDereferenceNode<T>::evaluate(ArrayCRef<Record> records,
                                        ArrayRef<Value> results) {
  this->fill_arg1_values(records);
  size_t total_size = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (!this->arg1_values_[i].is_na()) {
      total_size += this->arg1_values_[i].raw_size();
    }
  }
  temp_records_.resize(block_size_);
  Array<Arg2> result_pool;
  result_pool.resize(total_size);
  size_t offset = 0;
  size_t count = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (this->arg1_values_[i].is_na()) {
      continue;
    }
    Float score = records[i].score;
    size_t value_size = this->arg1_values_[i].raw_size();
    for (size_t j = 0; j < value_size; ++j) {
      temp_records_[count] = Record(this->arg1_values_[i][j], score);
      ++count;
      if (count >= block_size_) {
        this->arg2_->evaluate(temp_records_, result_pool.ref(offset, count));
        offset += count;
        count = 0;
      }
    }
  }
  if (count != 0) {
    this->arg2_->evaluate(temp_records_.ref(0, count),
                          result_pool.ref(offset, count));
  }
  offset = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    if (this->arg1_values_[i].is_na()) {
      results[i] = Value::na();
    } else {
      size_t size = this->arg1_values_[i].raw_size();
      results[i] = Value(&result_pool[offset], size);
      offset += size;
    }
  }
  result_pools_.push_back(std::move(result_pool));
}

}  // namespace expression

using namespace expression;

// -- Expression --

Expression::Expression(const Table *table,
                       std::unique_ptr<Node> &&root,
                       const ExpressionOptions &options)
    : ExpressionInterface(),
      table_(table),
      root_(std::move(root)),
      block_size_(options.block_size) {}

Expression::~Expression() {}

DataType Expression::data_type() const {
  return root_->data_type();
}

bool Expression::is_row_id() const {
  return root_->node_type() == ROW_ID_NODE;
}

bool Expression::is_score() const {
  return root_->node_type() == SCORE_NODE;
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
                          Array<Vector<Bool>> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Vector<Int>> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Vector<Float>> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Vector<GeoPoint>> *results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          Array<Vector<Text>> *results) {
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

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Vector<Bool>> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Vector<Int>> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Vector<Float>> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Vector<GeoPoint>> results) {
  _evaluate(records, results);
}

void Expression::evaluate(ArrayCRef<Record> records,
                          ArrayRef<Vector<Text>> results) {
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
    : ExpressionBuilderInterface(),
      table_(table),
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
    ColumnBase *column = table_->find_column(name);
    if (!column) {
      throw "Column not found";  // TODO
    }
    node_stack_.push_back(std::unique_ptr<Node>(create_column_node(column)));
  }
}

void ExpressionBuilder::push_operator(OperatorType operator_type) {
  if (subexpression_builder_) {
    subexpression_builder_->push_operator(operator_type);
  } else {
    switch (operator_type) {
      case GRNXX_LOGICAL_NOT:
      case GRNXX_BITWISE_NOT:
      case GRNXX_POSITIVE:
      case GRNXX_NEGATIVE:
      case GRNXX_TO_INT:
      case GRNXX_TO_FLOAT: {
        return push_unary_operator(operator_type);
      }
      case GRNXX_LOGICAL_AND:
      case GRNXX_LOGICAL_OR:
      case GRNXX_EQUAL:
      case GRNXX_NOT_EQUAL:
      case GRNXX_LESS:
      case GRNXX_LESS_EQUAL:
      case GRNXX_GREATER:
      case GRNXX_GREATER_EQUAL:
      case GRNXX_BITWISE_AND:
      case GRNXX_BITWISE_OR:
      case GRNXX_BITWISE_XOR:
      case GRNXX_PLUS:
      case GRNXX_MINUS:
      case GRNXX_MULTIPLICATION:
      case GRNXX_DIVISION:
      case GRNXX_MODULUS:
      case GRNXX_STARTS_WITH:
      case GRNXX_ENDS_WITH:
      case GRNXX_CONTAINS:
      case GRNXX_SUBSCRIPT: {
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
  if (node_stack_.size() < 2) {
    throw "Not enough operands";  // TODO
  }
  std::unique_ptr<Node> arg1 = std::move(node_stack_[node_stack_.size() - 2]);
  std::unique_ptr<Node> arg2 = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 2);
  std::unique_ptr<Node> node(
      create_dereference_node(std::move(arg1), std::move(arg2), options));
  node_stack_.push_back(std::move(node));
}

Node *ExpressionBuilder::create_constant_node(
    const Datum &datum) try {
  switch (datum.type()) {
    case GRNXX_BOOL: {
      return new ConstantNode<Bool>(datum.as_bool());
    }
    case GRNXX_INT: {
      return new ConstantNode<Int>(datum.as_int());
    }
    case GRNXX_FLOAT: {
      return new ConstantNode<Float>(datum.as_float());
    }
    case GRNXX_GEO_POINT: {
      return new ConstantNode<GeoPoint>(datum.as_geo_point());
    }
    case GRNXX_TEXT: {
      return new ConstantNode<Text>(datum.as_text());
    }
    case GRNXX_BOOL_VECTOR: {
      return new ConstantNode<Vector<Bool>>(datum.as_bool_vector());
    }
    case GRNXX_INT_VECTOR: {
      return new ConstantNode<Vector<Int>>(datum.as_int_vector());
    }
    case GRNXX_FLOAT_VECTOR: {
      return new ConstantNode<Vector<Float>>(datum.as_float_vector());
    }
    case GRNXX_GEO_POINT_VECTOR: {
      return new ConstantNode<Vector<GeoPoint>>(datum.as_geo_point_vector());
    }
    case GRNXX_TEXT_VECTOR: {
      return new ConstantNode<Vector<Text>>(datum.as_text_vector());
    }
    default: {
      throw "Not supported yet";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

Node *ExpressionBuilder::create_column_node(ColumnBase *column) try {
  switch (column->data_type()) {
    case GRNXX_BOOL: {
      return new ColumnNode<Bool>(column);
    }
    case GRNXX_INT: {
      return new ColumnNode<Int>(column);
    }
    case GRNXX_FLOAT: {
      return new ColumnNode<Float>(column);
    }
    case GRNXX_GEO_POINT: {
      return new ColumnNode<GeoPoint>(column);
    }
    case GRNXX_TEXT: {
      return new ColumnNode<Text>(column);
    }
    case GRNXX_BOOL_VECTOR: {
      return new ColumnNode<Vector<Bool>>(column);
    }
    case GRNXX_INT_VECTOR: {
      return new ColumnNode<Vector<Int>>(column);
    }
    case GRNXX_FLOAT_VECTOR: {
      return new ColumnNode<Vector<Float>>(column);
    }
    case GRNXX_GEO_POINT_VECTOR: {
      return new ColumnNode<Vector<GeoPoint>>(column);
    }
    case GRNXX_TEXT_VECTOR: {
      return new ColumnNode<Vector<Text>>(column);
    }
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
    case GRNXX_LOGICAL_NOT: {
      switch (arg->data_type()) {
        case GRNXX_BOOL: {
          return new LogicalNotNode(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_BITWISE_NOT: {
      switch (arg->data_type()) {
        case GRNXX_BOOL: {
          return new BitwiseNotNode<Bool>(std::move(arg));
        }
        case GRNXX_INT: {
          return new BitwiseNotNode<Int>(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_POSITIVE: {
      switch (arg->data_type()) {
        case GRNXX_INT:
        case GRNXX_FLOAT: {
          // A positive operator does nothing.
          return arg.release();
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_NEGATIVE: {
      switch (arg->data_type()) {
        case GRNXX_INT: {
          return new NegativeNode<Int>(std::move(arg));
        }
        case GRNXX_FLOAT: {
          return new NegativeNode<Float>(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_TO_INT: {
      switch (arg->data_type()) {
        case GRNXX_FLOAT: {
          return new ToIntNode(std::move(arg));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_TO_FLOAT: {
      switch (arg->data_type()) {
        case GRNXX_INT: {
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
  switch (operator_type) {
    case GRNXX_LOGICAL_AND: {
      if ((arg1->data_type() != GRNXX_BOOL) ||
          (arg2->data_type() != GRNXX_BOOL)) {
        throw "Invalid data type";  // TODO
      }
      return new LogicalAndNode(std::move(arg1), std::move(arg2));
    }
    case GRNXX_LOGICAL_OR: {
      if ((arg1->data_type() != GRNXX_BOOL) ||
          (arg2->data_type() != GRNXX_BOOL)) {
        throw "Invalid data type";  // TODO
      }
      return new LogicalOrNode(std::move(arg1), std::move(arg2));
    }
    case GRNXX_EQUAL:
    case GRNXX_NOT_EQUAL: {
      switch (arg1->data_type()) {
        case GRNXX_BOOL: {
          return create_equality_test_node<Bool>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_INT: {
          return create_equality_test_node<Int>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT: {
          return create_equality_test_node<Float>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_GEO_POINT: {
          return create_equality_test_node<GeoPoint>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_TEXT: {
          return create_equality_test_node<Text>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_BOOL_VECTOR: {
          return create_equality_test_node<Vector<Bool>>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_INT_VECTOR: {
          return create_equality_test_node<Vector<Int>>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT_VECTOR: {
          return create_equality_test_node<Vector<Float>>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_GEO_POINT_VECTOR: {
          return create_equality_test_node<Vector<GeoPoint>>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_TEXT_VECTOR: {
          return create_equality_test_node<Vector<Text>>(
            operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_LESS:
    case GRNXX_LESS_EQUAL:
    case GRNXX_GREATER:
    case GRNXX_GREATER_EQUAL: {
      switch (arg1->data_type()) {
        case GRNXX_INT: {
          return create_comparison_node<Int>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT: {
          return create_comparison_node<Float>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_TEXT: {
          return create_comparison_node<Text>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_BITWISE_AND:
    case GRNXX_BITWISE_OR:
    case GRNXX_BITWISE_XOR: {
      switch (arg1->data_type()) {
        case GRNXX_BOOL: {
          return create_bitwise_binary_node<Bool>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_INT: {
          return create_bitwise_binary_node<Int>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_PLUS:
    case GRNXX_MINUS:
    case GRNXX_MULTIPLICATION:
    case GRNXX_DIVISION:
    case GRNXX_MODULUS: {
      switch (arg1->data_type()) {
        case GRNXX_INT: {
          return create_arithmetic_node<Int>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT: {
          return create_arithmetic_node<Float>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_STARTS_WITH:
    case GRNXX_ENDS_WITH:
    case GRNXX_CONTAINS: {
      switch (arg1->data_type()) {
        case GRNXX_TEXT: {
          return create_search_node<Text>(
              operator_type, std::move(arg1), std::move(arg2));
        }
        // TODO: Search nodes can support Vector.
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_SUBSCRIPT: {
      return create_subscript_node(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Not supported yet";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// Create a node associated with an equality test operator.
template <typename T>
Node *ExpressionBuilder::create_equality_test_node(
    OperatorType operator_type,
    std::unique_ptr<Node> &&arg1,
    std::unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    throw "Data type conflict";  // TODO
  }
  switch (operator_type) {
    case GRNXX_EQUAL: {
      return new EqualNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_NOT_EQUAL: {
      return new NotEqualNode<T>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid operator";  // TODO
    }
  }
}

// Create a node associated with a comparison operator.
template <typename T>
Node *ExpressionBuilder::create_comparison_node(OperatorType operator_type,
                                                std::unique_ptr<Node> &&arg1,
                                                std::unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    throw "Data type conflict";  // TODO
  }
  switch (operator_type) {
    case GRNXX_LESS: {
      return new LessNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_LESS_EQUAL: {
      return new LessEqualNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_GREATER: {
      return new GreaterNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_GREATER_EQUAL: {
      return new GreaterEqualNode<T>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid operator";  // TODO
    }
  }
}

template <typename T>
Node *ExpressionBuilder::create_bitwise_binary_node(
    OperatorType operator_type,
    std::unique_ptr<Node> &&arg1,
    std::unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    throw "Data type conflict";  // TODO
  }
  switch (operator_type) {
    case GRNXX_BITWISE_AND: {
      return new BitwiseAndNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_BITWISE_OR: {
      return new BitwiseOrNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_BITWISE_XOR: {
      return new BitwiseXorNode<T>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid operator";  // TODO
    }
  }
}

template <typename T>
Node *ExpressionBuilder::create_arithmetic_node(
    OperatorType operator_type,
    std::unique_ptr<Node> &&arg1,
    std::unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    throw "Data type conflict";  // TODO
  }
  switch (operator_type) {
    case GRNXX_PLUS: {
      return new PlusNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_MINUS: {
      return new MinusNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_MULTIPLICATION: {
      return new MultiplicationNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_DIVISION: {
      return new DivisionNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_MODULUS: {
      return new ModulusNode<T>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid operator";  // TODO
    }
  }
}

template <typename T>
Node *ExpressionBuilder::create_search_node(OperatorType operator_type,
                                            std::unique_ptr<Node> &&arg1,
                                            std::unique_ptr<Node> &&arg2) {
  if (arg1->data_type() != arg2->data_type()) {
    throw "Data type conflict";  // TODO
  }
  switch (operator_type) {
    case GRNXX_STARTS_WITH: {
      return new StartsWithNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_ENDS_WITH: {
      return new EndsWithNode<T>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_CONTAINS: {
      return new ContainsNode<T>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid operator";  // TODO
    }
  }
}

Node *ExpressionBuilder::create_subscript_node(std::unique_ptr<Node> &&arg1,
                                               std::unique_ptr<Node> &&arg2) {
  if (arg2->data_type() != GRNXX_INT) {
    throw "Invalid data type";  // TODO
  }
  switch (arg1->data_type()) {
    case GRNXX_BOOL_VECTOR: {
      return new SubscriptNode<Bool>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_INT_VECTOR: {
      return new SubscriptNode<Int>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_FLOAT_VECTOR: {
      return new SubscriptNode<Float>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_GEO_POINT_VECTOR: {
      return new SubscriptNode<GeoPoint>(std::move(arg1), std::move(arg2));
    }
    case GRNXX_TEXT_VECTOR: {
      return new SubscriptNode<Text>(std::move(arg1), std::move(arg2));
    }
    default: {
      throw "Invalid data type";  // TODO
    }
  }
}

Node *ExpressionBuilder::create_dereference_node(
    std::unique_ptr<Node> &&arg1,
    std::unique_ptr<Node> &&arg2,
    const ExpressionOptions &options) {
  switch (arg1->data_type()) {
    case GRNXX_INT: {
      switch (arg2->data_type()) {
        case GRNXX_BOOL: {
          return new DereferenceNode<Bool>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_INT: {
          return new DereferenceNode<Int>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT: {
          return new DereferenceNode<Float>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_GEO_POINT: {
          return new DereferenceNode<GeoPoint>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_TEXT: {
          return new DereferenceNode<Text>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_BOOL_VECTOR: {
          return new DereferenceNode<Vector<Bool>>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_INT_VECTOR: {
          return new DereferenceNode<Vector<Int>>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_FLOAT_VECTOR: {
          return new DereferenceNode<Vector<Float>>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_GEO_POINT_VECTOR: {
          return new DereferenceNode<Vector<GeoPoint>>(
              std::move(arg1), std::move(arg2));
        }
        case GRNXX_TEXT_VECTOR: {
          return new DereferenceNode<Vector<Text>>(
              std::move(arg1), std::move(arg2));
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    case GRNXX_INT_VECTOR: {
      switch (arg2->data_type()) {
        case GRNXX_BOOL: {
          return new VectorDereferenceNode<Bool>(
              std::move(arg1), std::move(arg2), options);
        }
        case GRNXX_INT: {
          return new VectorDereferenceNode<Int>(
              std::move(arg1), std::move(arg2), options);
        }
        case GRNXX_FLOAT: {
          return new VectorDereferenceNode<Float>(
              std::move(arg1), std::move(arg2), options);
        }
        case GRNXX_GEO_POINT: {
          return new VectorDereferenceNode<GeoPoint>(
              std::move(arg1), std::move(arg2), options);
        }
        case GRNXX_TEXT: {
          return new VectorDereferenceNode<Text>(
              std::move(arg1), std::move(arg2), options);
        }
        default: {
          throw "Invalid data type";  // TODO
        }
      }
    }
    default: {
      throw "Invalid data type";  // TODO
    }
  }
}

// -- ExpressionParser --

ExpressionTokenType ExpressionToken::get_operator_token_type(
    OperatorType operator_type) {
  switch (operator_type) {
    case GRNXX_LOGICAL_NOT:
    case GRNXX_BITWISE_NOT:
    case GRNXX_POSITIVE:
    case GRNXX_NEGATIVE:
    case GRNXX_TO_INT:
    case GRNXX_TO_FLOAT: {
      return UNARY_OPERATOR_TOKEN;
    }
    case GRNXX_LOGICAL_AND:
    case GRNXX_LOGICAL_OR:
    case GRNXX_EQUAL:
    case GRNXX_NOT_EQUAL:
    case GRNXX_LESS:
    case GRNXX_LESS_EQUAL:
    case GRNXX_GREATER:
    case GRNXX_GREATER_EQUAL:
    case GRNXX_BITWISE_AND:
    case GRNXX_BITWISE_OR:
    case GRNXX_BITWISE_XOR:
    case GRNXX_PLUS:
    case GRNXX_MINUS:
    case GRNXX_MULTIPLICATION:
    case GRNXX_DIVISION:
    case GRNXX_MODULUS:
    case GRNXX_STARTS_WITH:
    case GRNXX_ENDS_WITH:
    case GRNXX_CONTAINS:
    case GRNXX_SUBSCRIPT: {
      return BINARY_OPERATOR_TOKEN;
    }
    default: {
      throw "Unsupported operator type";
    }
  }
}

int ExpressionToken::get_operator_priority(OperatorType operator_type) {
  switch (operator_type) {
    case GRNXX_LOGICAL_NOT:
    case GRNXX_BITWISE_NOT:
    case GRNXX_POSITIVE:
    case GRNXX_NEGATIVE:
    case GRNXX_TO_INT:
    case GRNXX_TO_FLOAT: {
      return 3;
    }
    case GRNXX_LOGICAL_AND: {
      return 13;
    }
    case GRNXX_LOGICAL_OR: {
      return 14;
    }
    case GRNXX_EQUAL:
    case GRNXX_NOT_EQUAL: {
      return 9;
    }
    case GRNXX_LESS:
    case GRNXX_LESS_EQUAL:
    case GRNXX_GREATER:
    case GRNXX_GREATER_EQUAL: {
      return 8;
    }
    case GRNXX_BITWISE_AND: {
      return 10;
    }
    case GRNXX_BITWISE_OR: {
      return 12;
    }
    case GRNXX_BITWISE_XOR: {
      return 11;
    }
    case GRNXX_PLUS:
    case GRNXX_MINUS: {
      return 6;
    }
    case GRNXX_MULTIPLICATION:
    case GRNXX_DIVISION:
    case GRNXX_MODULUS: {
      return 5;
    }
    case GRNXX_STARTS_WITH:
    case GRNXX_ENDS_WITH:
    case GRNXX_CONTAINS: {
      return 7;
    }
    case GRNXX_SUBSCRIPT: {
      return 2;
    }
    default: {
      throw "Unsupported operator type";
    }
  }
}

std::unique_ptr<ExpressionInterface> ExpressionParser::parse(
    const TableInterface *table,
    const String &query) try {
  std::unique_ptr<ExpressionParser> parser(new ExpressionParser(table));
  parser->tokenize(query);
  parser->analyze();
  return parser->builder_->release();
} catch (const std::bad_alloc &) {
	throw "Memory allocation failed";
}

ExpressionParser::ExpressionParser(const TableInterface *table)
    : table_(table),
      tokens_(),
      stack_(),
      builder_() {}

void ExpressionParser::tokenize(const String &query) {
  String rest = query;
  while (rest.size() != 0) {
    // Ignore white-space characters.
    auto delim_pos = rest.find_first_not_of(" \t\r\n");
    if (delim_pos == rest.npos) {
      break;
    }
    rest = rest.substring(delim_pos);
    switch (rest[0]) {
      case '!': {
        if ((rest.size() >= 2) && (rest[1] == '=')) {
          tokens_.push_back(ExpressionToken("!=", GRNXX_NOT_EQUAL));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken("!", GRNXX_LOGICAL_NOT));
          rest = rest.substring(1);
        }
        break;
      }
      case '~': {
        tokens_.push_back(ExpressionToken("~", GRNXX_BITWISE_NOT));
        rest = rest.substring(1);
        break;
      }
      case '=': {
        if ((rest.size() >= 2) && (rest[1] == '=')) {
          tokens_.push_back(ExpressionToken("==", GRNXX_EQUAL));
          rest = rest.substring(2);
        } else {
          throw "Invalid query";
        }
        break;
      }
      case '<': {
        if ((rest.size() >= 2) && (rest[1] == '=')) {
          tokens_.push_back(ExpressionToken("<=", GRNXX_LESS_EQUAL));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken("<", GRNXX_LESS));
          rest = rest.substring(1);
        }
        break;
      }
      case '>': {
        if ((rest.size() >= 2) && (rest[1] == '=')) {
          tokens_.push_back(ExpressionToken(">=", GRNXX_GREATER_EQUAL));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken(">", GRNXX_GREATER));
          rest = rest.substring(1);
        }
        break;
      }
      case '&': {
        if ((rest.size() >= 2) && (rest[1] == '&')) {
          tokens_.push_back(ExpressionToken("&&", GRNXX_LOGICAL_AND));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken("&", GRNXX_BITWISE_AND));
          rest = rest.substring(1);
        }
        break;
      }
      case '|': {
        if ((rest.size() >= 2) && (rest[1] == '|')) {
          tokens_.push_back(ExpressionToken("||", GRNXX_LOGICAL_OR));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken("|", GRNXX_BITWISE_OR));
          rest = rest.substring(1);
        }
        break;
      }
      case '^': {
        tokens_.push_back(ExpressionToken("^", GRNXX_BITWISE_XOR));
        rest = rest.substring(1);
        break;
      }
      case '+': {
        tokens_.push_back(ExpressionToken("+", GRNXX_PLUS));
        rest = rest.substring(1);
        break;
      }
      case '-': {
        tokens_.push_back(ExpressionToken("-", GRNXX_MINUS));
        rest = rest.substring(1);
        break;
      }
      case '*': {
        tokens_.push_back(ExpressionToken("*", GRNXX_MULTIPLICATION));
        rest = rest.substring(1);
        break;
      }
      case '/': {
        tokens_.push_back(ExpressionToken("/", GRNXX_DIVISION));
        rest = rest.substring(1);
        break;
      }
      case '%': {
        tokens_.push_back(ExpressionToken("%", GRNXX_MODULUS));
        rest = rest.substring(1);
        break;
      }
      case '@': {
        if ((rest.size() >= 2) && (rest[1] == '^')) {
          tokens_.push_back(ExpressionToken("@^", GRNXX_STARTS_WITH));
          rest = rest.substring(2);
        } else if ((rest.size() >= 2) && (rest[1] == '$')) {
          tokens_.push_back(ExpressionToken("@$", GRNXX_ENDS_WITH));
          rest = rest.substring(2);
        } else {
          tokens_.push_back(ExpressionToken("@", GRNXX_CONTAINS));
          rest = rest.substring(1);
        }
        break;
      }
      case '.': {
        tokens_.push_back(ExpressionToken(".", DEREFERENCE_TOKEN));
        rest = rest.substring(1);
        break;
      }
      case '(': {
        tokens_.push_back(ExpressionToken("(", LEFT_ROUND_BRACKET));
        rest = rest.substring(1);
        break;
      }
      case ')': {
        tokens_.push_back(ExpressionToken(")", RIGHT_ROUND_BRACKET));
        rest = rest.substring(1);
        break;
      }
      case '[': {
        tokens_.push_back(ExpressionToken("[", LEFT_SQUARE_BRACKET));
        rest = rest.substring(1);
        break;
      }
      case ']': {
        tokens_.push_back(ExpressionToken("]", RIGHT_SQUARE_BRACKET));
        rest = rest.substring(1);
        break;
      }
      case '"': {
        auto end = rest.find_first_of('"', 1);
        if (end == rest.npos) {
          throw "Invalid query";
        }
        tokens_.push_back(
            ExpressionToken(rest.substring(0, end + 1), CONSTANT_TOKEN));
        rest = rest.substring(end + 1);
        break;
      }
      case '0' ... '9': {
        // TODO: Improve this.
        auto end = rest.find_first_not_of("0123456789", 1);
        if (end == rest.npos) {
          end = rest.size();
        } else if (rest[end] == '.') {
          end = rest.find_first_not_of("0123456789", end + 1);
          if (end == rest.npos) {
            end = rest.size();
          }
        }
        String token = rest.substring(0, end);
        tokens_.push_back(ExpressionToken(token, CONSTANT_TOKEN));
        rest = rest.substring(end);
        break;
      }
      case 'A' ... 'Z':
      case 'a' ... 'z': {
        // TODO: Improve this.
        auto end = rest.find_first_not_of("0123456789"
                                          "ABCDEFGHIJKLMNOPQRSTUVWXYZ_"
                                          "abcdefghijklmnopqrstuvwxyz", 1);
        if (end == rest.npos) {
          end = rest.size();
        }
        String token = rest.substring(0, end);
        if ((token == "TRUE") || (token == "FALSE")) {
          tokens_.push_back(ExpressionToken(token, CONSTANT_TOKEN));
        } else {
          tokens_.push_back(ExpressionToken(token, NAME_TOKEN));
        }
        rest = rest.substring(end);
        break;
      }
      default: {
        throw "Invalid query";
      }
    }
  }
}

void ExpressionParser::analyze() {
  if (tokens_.size() == 0) {
    throw "Empty query";
  }
  builder_ = ExpressionBuilderInterface::create(table_);
  push_token(ExpressionToken("(", LEFT_ROUND_BRACKET));
  for (size_t i = 0; i < tokens_.size(); ++i) {
    push_token(tokens_[i]);
  }
  push_token(ExpressionToken(")", RIGHT_ROUND_BRACKET));
}

void ExpressionParser::push_token(const ExpressionToken &token) {
  switch (token.type()) {
    case DUMMY_TOKEN: {
      if ((stack_.size() != 0) && (stack_.back().type() == DUMMY_TOKEN)) {
        throw "Invalid query";
      }
      stack_.push_back(token);
      break;
    }
    case CONSTANT_TOKEN: {
      Datum datum;
      std::string string(token.string().data(), token.string().size());
      if (std::isdigit(static_cast<uint8_t>(string[0]))) {
        if (string.find_first_of('.') == string.npos) {
          datum = grnxx::Int(std::stoll(string));
        } else {
          datum = grnxx::Float(std::stod(string));
        }
      } else {
        const String &string = token.string();
        datum = grnxx::Text(string.substring(1, string.size() - 2));
      }
      push_token(ExpressionToken(token.string(), DUMMY_TOKEN));
      builder_->push_constant(datum);
      break;
    }
    case NAME_TOKEN: {
      push_token(ExpressionToken(token.string(), DUMMY_TOKEN));
      builder_->push_column(token.string());
      break;
    }
    case UNARY_OPERATOR_TOKEN: {
      if ((stack_.size() != 0) && (stack_.back().type() == DUMMY_TOKEN)) {
        // 単項演算子の直前にオペランドがあるのはおかしい．
        throw "Invalid query";
      }
      stack_.push_back(token);
      break;
    }
    case BINARY_OPERATOR_TOKEN: {
      if ((stack_.size() == 0) || (stack_.back().type() != DUMMY_TOKEN)) {
        // 二項演算子の前はダミーでなければならない．
        throw "Invalid query";
      }
      // 前方にある優先度の高い演算子を適用する．
      while (stack_.size() >= 2) {
        ExpressionToken operator_token = stack_[stack_.size() - 2];
        if (operator_token.type() == UNARY_OPERATOR_TOKEN) {
          builder_->push_operator(operator_token.operator_type());
          stack_.pop_back();
          stack_.pop_back();
          push_token(ExpressionToken("", DUMMY_TOKEN));
        } else if ((operator_token.type() == BINARY_OPERATOR_TOKEN) &&
                   (operator_token.priority() <= token.priority())) {
          builder_->push_operator(operator_token.operator_type());
          stack_.pop_back();
          stack_.pop_back();
          stack_.pop_back();
          push_token(ExpressionToken("", DUMMY_TOKEN));
        } else {
          break;
        }
      }
      stack_.push_back(token);
      break;
    }
    case DEREFERENCE_TOKEN: {
      // TODO: Not supported yet.
      throw "Not supported yet";
    }
    case BRACKET_TOKEN: {
      if (token.bracket_type() == LEFT_ROUND_BRACKET) {
        // 開き括弧の直前にダミーがあるのはおかしい．
        if ((stack_.size() != 0) && (stack_.back().type() == DUMMY_TOKEN)) {
          throw "Invalid query";
        }
        stack_.push_back(token);
      } else if (token.bracket_type() == RIGHT_ROUND_BRACKET) {
        // 閉じ括弧の直前にはダミーが必要であり，それより前に開き括弧が必要である．
        if ((stack_.size() < 2) || (stack_.back().type() != DUMMY_TOKEN)) {
          throw "Invalid query 1";
        }
        // 括弧内にある演算子をすべて解決する．
        while (stack_.size() >= 2) {
          ExpressionToken operator_token = stack_[stack_.size() - 2];
          if (operator_token.type() == UNARY_OPERATOR_TOKEN) {
            builder_->push_operator(operator_token.operator_type());
            stack_.pop_back();
            stack_.pop_back();
            push_token(ExpressionToken("", DUMMY_TOKEN));
          } else if (operator_token.type() == BINARY_OPERATOR_TOKEN) {
            builder_->push_operator(operator_token.operator_type());
            stack_.pop_back();
            stack_.pop_back();
            stack_.pop_back();
            push_token(ExpressionToken("", DUMMY_TOKEN));
          } else {
            break;
          }
        }
        if ((stack_.size() < 2) ||
            (stack_[stack_.size() - 2].type() != BRACKET_TOKEN) ||
            (stack_[stack_.size() - 2].bracket_type() != LEFT_ROUND_BRACKET)) {
          throw "Invalid query";
        }
        stack_[stack_.size() - 2] = stack_.back();
        stack_.pop_back();
        break;
      } else {
        // TODO: Not supported yet ([]).
        throw "Not supported yet";
      }
      break;
    }
    default: {
      throw "Invalid query";
    }
  }
}

}  // namespace impl
}  // namespace grnxx
