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

//template <>
//class ConstantNode<Text> : public TypedNode<Text> {
// public:
//  using Value = Text;

//  static unique_ptr<Node> create(Error *error, Value datum) {
//    unique_ptr<ConstantNode> node(new (nothrow) ConstantNode);
//    if (!node) {
//      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
//      return nullptr;
//    }
//    if (!node->datum_.assign(error, datum)) {
//      return nullptr;
//    }
//    return unique_ptr<Node>(node.release());
//  }

//  explicit ConstantNode()
//      : TypedNode<Value>(),
//        datum_() {}

//  NodeType node_type() const {
//    return CONSTANT_NODE;
//  }

//  bool evaluate(Error *,
//                ArrayCRef<Record> records,
//                ArrayRef<Value> results) {
//    for (Int i = 0; i < records.size(); ++i) {
//      results[i] = datum_;
//    }
//    return true;
//  }

// private:
//  String datum_;
//};

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
  // TODO: Node has the data type.
  throw "Not supported yet";  // TODO
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
    // TODO
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

}  // namespace impl
}  // namespace grnxx
