#include "grnxx/expression.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
#include "grnxx/table.hpp"

#include <iostream>  // For debugging.

namespace grnxx {
namespace {

enum NodeType {
  DATUM_NODE,
  ROW_ID_NODE,
  SCORE_NODE,
  COLUMN_NODE,
  OPERATOR_NODE
};

}  // namespace

class ExpressionNode {
 public:
  ExpressionNode() {}
  virtual ~ExpressionNode() {}

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
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool filter(Error *error,
                      const ArrayRef<Record> &input_records,
                      ArrayRef<Record> *output_records) = 0;

  // Adjust scores of records.
  //
  // Evaluates the expression for the given record set and replaces their
  // scores with the evaluation results.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool adjust(Error *error, ArrayRef<Record> *records) = 0;

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into each expression node.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool evaluate(Error *error, const ArrayRef<Record> &records) = 0;
};

namespace {

template <typename T>
class Node : public ExpressionNode {
 public:
  Node() : ExpressionNode(), values_() {}
  virtual ~Node() {}

  DataType data_type() const {
    return TypeTraits<T>::data_type();
  }

  virtual bool filter(Error *error,
                      const ArrayRef<Record> &input_records,
                      ArrayRef<Record> *output_records);
  virtual bool adjust(Error *error, ArrayRef<Record> *records);

  virtual bool evaluate(Error *error, const ArrayRef<Record> &records) = 0;

  T get(Int i) const {
    return values_[i];
  }

 protected:
  Array<T> values_;
};

template <typename T>
bool Node<T>::filter(Error *error,
                     const ArrayRef<Record> &input_records,
                     ArrayRef<Record> *output_records) {
  // Only Node<Bool> supports filter().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool Node<Bool>::filter(Error *error,
                        const ArrayRef<Record> &input_records,
                        ArrayRef<Record> *output_records) {
  if (!evaluate(error, input_records)) {
    return false;
  }
  Int dest = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (values_[i]) {
      output_records->set(dest, input_records.get(i));
      ++dest;
    }
  }
  *output_records = output_records->ref(0, dest);
  return true;
}

template <typename T>
bool Node<T>::adjust(Error *error, ArrayRef<Record> *records) {
  // Only Node<Float> supports adjust().
  GRNXX_ERROR_SET(error, INVALID_OPERATION, "Invalid operation");
  return false;
}

template <>
bool Node<Float>::adjust(Error *error, ArrayRef<Record> *records) {
  if (!evaluate(error, *records)) {
    return false;
  }
  for (Int i = 0; i < records->size(); ++i) {
    records->set_score(i, values_[i]);
  }
  return true;
}

// -- DatumNode --

template <typename T>
class DatumNode : public Node<T> {
 public:
  explicit DatumNode(T datum) : Node<T>(), datum_(datum) {}
  virtual ~DatumNode() {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  T datum_;
};

template <typename T>
bool DatumNode<T>::evaluate(Error *error, const ArrayRef<Record> &records) {
  if (static_cast<size_t>(records.size()) <= this->values_.size()) {
    // The buffer is already filled and there is nothing to do.
    return true;
  }
  return this->values_.resize(error, records.size(), datum_);
}

template <>
class DatumNode<Text> : public Node<Text> {
 public:
  explicit DatumNode(Text datum)
      : Node<Text>(),
        datum_(datum.data(), datum.size()) {}
  virtual ~DatumNode() {}

  NodeType node_type() const {
    return DATUM_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  std::string datum_;
};

bool DatumNode<Text>::evaluate(Error *error, const ArrayRef<Record> &records) {
  if (static_cast<size_t>(records.size()) <= this->values_.size()) {
    // The buffer is already filled and there is nothing to do.
    return true;
  }
  return this->values_.resize(error, records.size(),
                              Text(datum_.data(), datum_.size()));
}

// -- RowIDNode --

class RowIDNode : public Node<Int> {
 public:
  RowIDNode() : Node<Int>() {}
  ~RowIDNode() {}

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records) {
    if (!this->values_.resize(error, records.size())) {
      return false;
    }
    for (Int i = 0; i < records.size(); ++i) {
      this->values_[i] = records.get_row_id(i);
    }
    return true;
  }
};

// -- ScoreNode --

class ScoreNode : public Node<Float> {
 public:
  ScoreNode() : Node<Float>() {}
  ~ScoreNode() {}

  NodeType node_type() const {
    return SCORE_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records) {
    if (!this->values_.resize(error, records.size())) {
      return false;
    }
    for (Int i = 0; i < records.size(); ++i) {
      this->values_[i] = records.get_score(i);
    }
    return true;
  }
};

// -- ColumnNode --

template <typename T>
class ColumnNode : public Node<T> {
 public:
  explicit ColumnNode(const Column *column)
      : Node<T>(),
        column_(static_cast<const ColumnImpl<T> *>(column)) {}
  virtual ~ColumnNode() {}

  NodeType node_type() const {
    return COLUMN_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records) {
    if (!this->values_.resize(error, records.size())) {
      return false;
    }
    for (Int i = 0; i < records.size(); ++i) {
      this->values_.set(i, column_->get(records.get_row_id(i)));
    }
    return true;
  }

 private:
  const ColumnImpl<T> *column_;
};

// -- OperatorNode --

// ---- UnaryNode ----

struct Negative {
  template <typename T>
  struct Functor {
    using Arg = T;
    using Result = T;
    Result operator()(Arg arg) const {
      return -arg;
    };
  };
};

template <typename T>
struct Typecast {
  template <typename U>
  struct Functor {
    using Arg = U;
    using Result = T;
    Result operator()(Arg arg) const {
      return static_cast<Result>(arg);
    };
  };
};

template <typename T>
class UnaryNode : public Node<typename T::Result> {
 public:
  using Operator = T;
  using Arg = typename Operator::Arg;

  UnaryNode(Operator op,
            unique_ptr<ExpressionNode> &&arg)
      : Node<typename Operator::Result>(),
        operator_(op),
        arg_(static_cast<Node<Arg> *>(arg.release())) {}
  virtual ~UnaryNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  Operator operator_;
  unique_ptr<Node<Arg>> arg_;
};

template <typename T>
bool UnaryNode<T>::evaluate(Error *error, const ArrayRef<Record> &records) {
  if (!this->values_.resize(error, records.size())) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  if (!arg_->evaluate(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    this->values_[i] = operator_(arg_->get(i));
  }
  return true;
}

// ---- BinaryNode ----

struct Equal {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs == rhs;
    };
  };
};

struct NotEqual {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs != rhs;
    };
  };
};

struct Less {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs < rhs;
    };
  };
};

struct LessEqual {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs <= rhs;
    };
  };
};

struct Greater {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs > rhs;
    };
  };
};

struct GreaterEqual {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = Bool;
    Bool operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs >= rhs;
    };
  };
};

struct BitwiseAnd {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs & rhs;
    };
  };
};

struct BitwiseOr {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs | rhs;
    };
  };
};

struct BitwiseXor {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs ^ rhs;
    };
  };
};

struct Plus {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs + rhs;
    };
  };
};

struct Minus {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs - rhs;
    };
  };
};

struct Multiplication {
  template <typename T>
  struct Functor {
    using Arg1 = T;
    using Arg2 = T;
    using Result = T;
    T operator()(Arg1 lhs, Arg2 rhs) const {
      return lhs * rhs;
    };
  };
};

template <typename Op>
class BinaryNode : public Node<typename Op::Result> {
 public:
  using Arg1 = typename Op::Arg1;
  using Arg2 = typename Op::Arg2;

  BinaryNode(Op op,
             unique_ptr<ExpressionNode> &&lhs,
             unique_ptr<ExpressionNode> &&rhs)
      : Node<typename Op::Result>(),
        operator_(op),
        lhs_(static_cast<Node<Arg1> *>(lhs.release())),
        rhs_(static_cast<Node<Arg2> *>(rhs.release())) {}
  virtual ~BinaryNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  Op operator_;
  unique_ptr<Node<Arg1>> lhs_;
  unique_ptr<Node<Arg2>> rhs_;
};

template <typename Op>
bool BinaryNode<Op>::evaluate(Error *error, const ArrayRef<Record> &records) {
  if (!this->values_.resize(error, records.size())) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  if (!lhs_->evaluate(error, records) ||
      !rhs_->evaluate(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    this->values_.set(i, operator_(lhs_->get(i), rhs_->get(i)));
  }
  return true;
}

class LogicalAndNode : public Node<Bool> {
 public:
  LogicalAndNode(unique_ptr<ExpressionNode> &&lhs,
                 unique_ptr<ExpressionNode> &&rhs)
      : Node<Bool>(),
        lhs_(static_cast<Node<Bool> *>(lhs.release())),
        rhs_(static_cast<Node<Bool> *>(rhs.release())),
        temp_records_() {}
  virtual ~LogicalAndNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  bool filter(Error *error,
              const ArrayRef<Record> &input_records,
              ArrayRef<Record> *output_records);

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  unique_ptr<Node<Bool>> lhs_;
  unique_ptr<Node<Bool>> rhs_;
  Array<Record> temp_records_;
};

bool LogicalAndNode::filter(Error *error,
                            const ArrayRef<Record> &input_records,
                            ArrayRef<Record> *output_records) {
  return lhs_->filter(error, input_records, output_records) &&
         rhs_->filter(error, *output_records, output_records);
}

bool LogicalAndNode::evaluate(Error *error, const ArrayRef<Record> &records) {
  if (!this->values_.resize(error, records.size())) {
    return false;
  }
  if (!lhs_->evaluate(error, records)) {
    return false;
  }
  // False records must not be evaluated for the 2nd argument.
  temp_records_.clear();
  for (Int i = 0; i < records.size(); ++i) {
    if (lhs_->get(i)) {
      if (!temp_records_.push_back(error, records.get(i))) {
        return false;
      }
    }
  }
  if (!rhs_->evaluate(error, temp_records_)) {
    return false;
  }
  Int j = 0;
  for (Int i = 0; i < records.size(); ++i) {
    this->values_.set(i, lhs_->get(i) ? rhs_->get(j++) : false);
  }
  return true;
}

class LogicalOrNode : public Node<Bool> {
 public:
  LogicalOrNode(unique_ptr<ExpressionNode> &&lhs,
                 unique_ptr<ExpressionNode> &&rhs)
      : Node<Bool>(),
        lhs_(static_cast<Node<Bool> *>(lhs.release())),
        rhs_(static_cast<Node<Bool> *>(rhs.release())),
        left_records_(),
        right_records_() {}
  virtual ~LogicalOrNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  bool filter(Error *error,
              const ArrayRef<Record> &input_records,
              ArrayRef<Record> *output_records);

  bool evaluate(Error *error, const ArrayRef<Record> &records);

 private:
  unique_ptr<Node<Bool>> lhs_;
  unique_ptr<Node<Bool>> rhs_;
  Array<Record> left_records_;
  Array<Record> right_records_;
};

bool LogicalOrNode::filter(Error *error,
                           const ArrayRef<Record> &input_records,
                           ArrayRef<Record> *output_records) {
  // Make a copy of the given record set and apply the left-filter to it.
  if (!left_records_.resize(error, input_records.size())) {
    return false;
  }
  for (Int i = 0; i < input_records.size(); ++i) {
    left_records_.set(i, input_records.get(i));
  }
  ArrayRef<Record> left_record_ref = left_records_;
  if (!lhs_->filter(error, left_record_ref, &left_record_ref)) {
    return false;
  }
  if (!left_records_.resize(error, left_record_ref.size())) {
    return false;
  }
  if (left_records_.size() == 0) {
    // There are no left-true records.
    return rhs_->filter(error, input_records, output_records);
  } else if (left_records_.size() == input_records.size()) {
    // There are no left-false records.
    // This means that all the records pass through the filter.
    if (&input_records != output_records) {
      *output_records = output_records->ref(0, input_records.size());
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
    }
    return true;
  }

  // Enumerate left-false records and apply the right-filter to it.
  if (!right_records_.resize(error,
                             input_records.size() - left_records_.size())) {
    return false;
  }
  Int left_count = 0;
  Int right_count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) ==
        left_records_.get_row_id((left_count))) {
      ++left_count;
    } else {
      right_records_.set(right_count, input_records.get(i));
      ++right_count;
    }
  }
  ArrayRef<Record> right_record_ref = right_records_;
  if (!rhs_->filter(error, right_record_ref, &right_record_ref)) {
    return false;
  }
  if (!right_records_.resize(error, right_record_ref.size())) {
    return false;
  }
  if (right_records_.size() == 0) {
    // There are no right-true records.
    *output_records = output_records->ref(0, left_records_.size());
    for (Int i = 0; i < output_records->size(); ++i) {
      output_records->set(i, left_records_.get(i));
    }
    return true;
  } else if (right_records_.size() == right_count) {
    // There are no right-false records.
    // This means that all the records pass through the filter.
    if (&input_records != output_records) {
      *output_records = output_records->ref(0, input_records.size());
      for (Int i = 0; i < input_records.size(); ++i) {
        output_records->set(i, input_records.get(i));
      }
    }
    return true;
  }

  // Append sentinels.
  if (!left_records_.push_back(error, Record(NULL_ROW_ID, 0.0)) ||
      !right_records_.push_back(error, Record(NULL_ROW_ID, 0.0))) {
    return false;
  }

  // Merge the left-true records and the right-true records.
  left_count = 0;
  right_count = 0;
  for (Int i = 0; i < input_records.size(); ++i) {
    if (input_records.get_row_id(i) ==
        left_records_.get_row_id(left_count)) {
      output_records->set(left_count + right_count, input_records.get(i));
      ++left_count;
    } else if (input_records.get_row_id(i) ==
               right_records_.get_row_id(right_count)) {
      output_records->set(left_count + right_count, input_records.get(i));
      ++right_count;
    }
  }
  *output_records = output_records->ref(0, left_count + right_count);
  return true;
}

bool LogicalOrNode::evaluate(Error *error, const ArrayRef<Record> &records) {
  // TODO: This logic should be tested.
  if (!this->values_.resize(error, records.size())) {
    return false;
  }
  if (!lhs_->evaluate(error, records)) {
    return false;
  }
  // True records must not be evaluated for the 2nd argument.
  left_records_.clear();
  for (Int i = 0; i < records.size(); ++i) {
    if (!lhs_->get(i)) {
      if (!left_records_.push_back(error, records.get(i))) {
        return false;
      }
    }
  }
  if (!rhs_->evaluate(error, left_records_)) {
    return false;
  }
  Int j = 0;
  for (Int i = 0; i < records.size(); ++i) {
    this->values_.set(i, lhs_->get(i) ? true : rhs_->get(j++));
  }
  return true;
}

}  // namespace

// -- Expression --

unique_ptr<Expression> Expression::create(Error *error,
                                          const Table *table,
                                          unique_ptr<ExpressionNode> &&root) {
  unique_ptr<Expression> expression(
      new (nothrow) Expression(table, std::move(root)));
  if (!expression) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return expression;
}

Expression::~Expression() {}

DataType Expression::data_type() const {
  return root_->data_type();
}

bool Expression::filter(Error *error, Array<Record> *records, Int offset) {
  ArrayRef<Record> input_ref = records->ref(offset);
  ArrayRef<Record> output_ref = records->ref(offset);
  while (input_ref.size() > block_size()) {
    ArrayRef<Record> input_block = input_ref.ref(0, block_size());
    if (input_ref.size() == output_ref.size()) {
      if (!root_->filter(error, input_block, &input_block)) {
        return false;
      }
      input_ref = input_ref.ref(block_size());
      output_ref = output_ref.ref(input_block.size());
    } else {
      ArrayRef<Record> output_block = output_ref;
      if (!root_->filter(error, input_block, &output_block)) {
        return false;
      }
      input_ref = input_ref.ref(block_size());
      output_ref = output_ref.ref(output_block.size());
    }
  }
  ArrayRef<Record> output_block = output_ref;
  if (!root_->filter(error, input_ref, &output_block)) {
    return false;
  }
  output_ref = output_ref.ref(output_block.size());
  return records->resize(error, records->size() - output_ref.size());
}

bool Expression::adjust(Error *error, Array<Record> *records, Int offset) {
  ArrayRef<Record> ref = records->ref(offset);
  while (ref.size() > block_size()) {
    ArrayRef<Record> block = ref.ref(0, block_size());
    if (!root_->adjust(error, &block)) {
      return false;
    }
    ref = ref.ref(block_size());
  }
  return root_->adjust(error, &ref);
}

template <typename T>
bool Expression::evaluate(Error *error,
                          const ArrayRef<Record> &records,
                          Array<T> *results) {
  if (TypeTraits<T>::data_type() != data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid data type");
    return false;
  }
  if (!results->resize(error, records.size())) {
    return false;
  }
  ArrayRef<T> ref = *results;
  return evaluate(error, records, &ref);
}

template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<Bool> *results);
template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<Int> *results);
template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<Float> *results);
template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<Time> *results);
template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<GeoPoint> *results);
template bool Expression::evaluate(Error *error,
                                   const ArrayRef<Record> &records,
                                   Array<Text> *results);

template <typename T>
bool Expression::evaluate(Error *error,
                          const ArrayRef<Record> &records,
                          ArrayRef<T> *results) {
  if (TypeTraits<T>::data_type() != data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid data type");
    return false;
  }
  if (records.size() != results->size()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Size conflict: "
                    "#records = %" PRIi64 ", #results = %" PRIi64,
                    records.size(), results->size());
    return false;
  }
  ArrayRef<Record> input = records;
  ArrayRef<T> output = *results;
  while (input.size() > block_size()) {
    ArrayRef<Record> ref = input.ref(0, block_size());
    if (!evaluate_block(error, ref, &output)) {
      return false;
    }
    input = input.ref(block_size());
    output = output.ref(block_size());
  }
  return evaluate_block(error, input, &output);
}

Expression::Expression(const Table *table, unique_ptr<ExpressionNode> &&root)
    : table_(table),
      root_(std::move(root)) {}

template <typename T>
bool Expression::evaluate_block(Error *error,
                                const ArrayRef<Record> &records,
                                ArrayRef<T> *results) {
  Node<T> *node = static_cast<Node<T> *>(root_.get());
  if (!node->evaluate(error, records)) {
    return false;
  }
  for (Int i = 0; i < records.size(); ++i) {
    results->set(i, node->get(i));
  }
  return true;
}

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
  unique_ptr<ExpressionNode> node;
  switch (datum.type()) {
    case BOOL_DATA: {
      node.reset(new (nothrow) DatumNode<Bool>(datum.force_bool()));
      break;
    }
    case INT_DATA: {
      node.reset(new (nothrow) DatumNode<Int>(datum.force_int()));
      break;
    }
    case FLOAT_DATA: {
      node.reset(new (nothrow) DatumNode<Float>(datum.force_float()));
      break;
    }
    case TIME_DATA: {
      node.reset(new (nothrow) DatumNode<Time>(datum.force_time()));
      break;
    }
    case GEO_POINT_DATA: {
      node.reset(new (nothrow) DatumNode<GeoPoint>(datum.force_geo_point()));
      break;
    }
    case TEXT_DATA: {
      node.reset(new (nothrow) DatumNode<Text>(datum.force_text()));
      break;
    }
    default: {
      // TODO: Other types are not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
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
  unique_ptr<ExpressionNode> node;
  if (name == "_id") {
    node.reset(new (nothrow) RowIDNode());
  } else if (name == "_score") {
    node.reset(new (nothrow) ScoreNode());
  } else {
    Column *column = table_->find_column(error, name);
    if (!column) {
      return false;
    }
    switch (column->data_type()) {
      case BOOL_DATA: {
        node.reset(new (nothrow) ColumnNode<Bool>(column));
        break;
      }
      case INT_DATA: {
        node.reset(new (nothrow) ColumnNode<Int>(column));
        break;
      }
      case FLOAT_DATA: {
        node.reset(new (nothrow) ColumnNode<Float>(column));
        break;
      }
      case TIME_DATA: {
        node.reset(new (nothrow) ColumnNode<Time>(column));
        break;
      }
      case GEO_POINT_DATA: {
        node.reset(new (nothrow) ColumnNode<GeoPoint>(column));
        break;
      }
      case TEXT_DATA: {
        node.reset(new (nothrow) ColumnNode<Text>(column));
        break;
      }
      default: {
        // TODO: Other types are not supported yet.
        GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
        return false;
      }
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  // This push_back() must not fail because a space is already reserved.
  stack_.push_back(nullptr, std::move(node));
  return true;
}

bool ExpressionBuilder::push_operator(Error *error,
                                      OperatorType operator_type) {
  switch (operator_type) {
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
    case MULTIPLICATION_OPERATOR: {
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

unique_ptr<Expression> ExpressionBuilder::release(Error *error) {
  if (stack_.size() != 1) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Incomplete expression");
    return nullptr;
  }
  unique_ptr<ExpressionNode> root_node = std::move(stack_[0]);
  stack_.clear();
  return Expression::create(error, table_, std::move(root_node));
}

ExpressionBuilder::ExpressionBuilder(const Table *table) : table_(table) {}

bool ExpressionBuilder::push_unary_operator(Error *error,
                                            OperatorType operator_type) {
  if (stack_.size() < 1) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough operands");
    return false;
  }
  switch (operator_type) {
    case POSITIVE_OPERATOR: {
      return push_positive_operator(error);
    }
    case NEGATIVE_OPERATOR: {
      return push_negative_operator(error);
    }
    case TO_INT_OPERATOR: {
      return push_to_int_operator(error);
    }
    case TO_FLOAT_OPERATOR: {
      return push_to_float_operator(error);
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
}

bool ExpressionBuilder::push_binary_operator(Error *error,
                                             OperatorType operator_type) {
  if (stack_.size() < 2) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough operands");
    return false;
  }
  switch (operator_type) {
    case LOGICAL_AND_OPERATOR: {
      return push_logical_and_operator(error);
    }
    case LOGICAL_OR_OPERATOR: {
      return push_logical_or_operator(error);
    }
    case EQUAL_OPERATOR: {
      return push_equality_operator<Equal>(error);
    }
    case NOT_EQUAL_OPERATOR: {
      return push_equality_operator<NotEqual>(error);
    }
    case LESS_OPERATOR: {
      return push_comparison_operator<Less>(error);
    }
    case LESS_EQUAL_OPERATOR: {
      return push_comparison_operator<LessEqual>(error);
    }
    case GREATER_OPERATOR: {
      return push_comparison_operator<Greater>(error);
    }
    case GREATER_EQUAL_OPERATOR: {
      return push_comparison_operator<GreaterEqual>(error);
    }
    case BITWISE_AND_OPERATOR: {
      return push_bitwise_operator<BitwiseAnd>(error);
    }
    case BITWISE_OR_OPERATOR: {
      return push_bitwise_operator<BitwiseOr>(error);
    }
    case BITWISE_XOR_OPERATOR: {
      return push_bitwise_operator<BitwiseXor>(error);
    }
    case PLUS_OPERATOR: {
      return push_arithmetic_operator<Plus>(error);
    }
    case MINUS_OPERATOR: {
      return push_arithmetic_operator<Minus>(error);
    }
    case MULTIPLICATION_OPERATOR: {
      return push_arithmetic_operator<Multiplication>(error);
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
}

bool ExpressionBuilder::push_positive_operator(Error *error) {
  auto &arg = stack_[stack_.size() - 1];
  switch (arg->data_type()) {
    case INT_DATA:
    case FLOAT_DATA: {
      // Nothing to do because this operator does nothing.
      return true;
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return false;
    }
  }
}

bool ExpressionBuilder::push_negative_operator(Error *error) {
  unique_ptr<ExpressionNode> node;
  auto &arg = stack_[stack_.size() - 1];
  switch (arg->data_type()) {
    case INT_DATA: {
      Negative::Functor<Int> functor;
      node.reset(
          new (nothrow) UnaryNode<decltype(functor)>(functor, std::move(arg)));
      break;
    }
    case FLOAT_DATA: {
      Negative::Functor<Float> functor;
      node.reset(
          new (nothrow) UnaryNode<decltype(functor)>(functor, std::move(arg)));
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.back() = std::move(node);
  return true;
}

bool ExpressionBuilder::push_to_int_operator(Error *error) {
  unique_ptr<ExpressionNode> node;
  auto &arg = stack_[stack_.size() - 1];
  switch (arg->data_type()) {
    case INT_DATA: {
      // Nothing to do because this operator does nothing.
      return true;
    }
    case FLOAT_DATA: {
      Typecast<Int>::Functor<Float> functor;
      node.reset(
          new (nothrow) UnaryNode<decltype(functor)>(functor, std::move(arg)));
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.back() = std::move(node);
  return true;
}

bool ExpressionBuilder::push_to_float_operator(Error *error) {
  unique_ptr<ExpressionNode> node;
  auto &arg = stack_[stack_.size() - 1];
  switch (arg->data_type()) {
    case INT_DATA: {
      Typecast<Float>::Functor<Int> functor;
      node.reset(
          new (nothrow) UnaryNode<decltype(functor)>(functor, std::move(arg)));
      break;
    }
    case FLOAT_DATA: {
      // Nothing to do because this operator does nothing.
      return true;
    }
    default: {
      GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.back() = std::move(node);
  return true;
}

bool ExpressionBuilder::push_logical_and_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if ((lhs->data_type() != BOOL_DATA) ||
      (rhs->data_type() != BOOL_DATA)) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
    return false;
  }
  unique_ptr<ExpressionNode> node(
      new (nothrow) LogicalAndNode(std::move(lhs), std::move(rhs)));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

bool ExpressionBuilder::push_logical_or_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if ((lhs->data_type() != BOOL_DATA) ||
      (rhs->data_type() != BOOL_DATA)) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Invalid data type");
    return false;
  }
  unique_ptr<ExpressionNode> node(
      new (nothrow) LogicalOrNode(std::move(lhs), std::move(rhs)));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

template <typename T>
bool ExpressionBuilder::push_equality_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return false;
  }
  unique_ptr<ExpressionNode> node;
  switch (lhs->data_type()) {
    case BOOL_DATA: {
      typename T::template Functor<Bool> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case INT_DATA: {
      typename T::template Functor<Int> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case FLOAT_DATA: {
      typename T::template Functor<Float> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case TIME_DATA: {
      typename T::template Functor<Time> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case GEO_POINT_DATA: {
      typename T::template Functor<GeoPoint> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case TEXT_DATA: {
      typename T::template Functor<Text> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    // TODO: Support other types.
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

template <typename T>
bool ExpressionBuilder::push_comparison_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return false;
  }
  unique_ptr<ExpressionNode> node;
  switch (lhs->data_type()) {
    case INT_DATA: {
      typename T::template Functor<Int> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case FLOAT_DATA: {
      typename T::template Functor<Float> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case TIME_DATA: {
      typename T::template Functor<Time> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case TEXT_DATA: {
      typename T::template Functor<Text> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    // TODO: Support other comparable types.
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

template <typename T>
bool ExpressionBuilder::push_bitwise_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Data type conflict");
    return false;
  }
  unique_ptr<ExpressionNode> node;
  switch (lhs->data_type()) {
    case BOOL_DATA: {
      typename T::template Functor<Bool> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case INT_DATA: {
      typename T::template Functor<Int> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

template <typename T>
bool ExpressionBuilder::push_arithmetic_operator(Error *error) {
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Type conflict");
    return false;
  }
  unique_ptr<ExpressionNode> node;
  switch (lhs->data_type()) {
    case INT_DATA: {
      typename T::template Functor<Int> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    case FLOAT_DATA: {
      typename T::template Functor<Float> functor;
      node.reset(new (nothrow) BinaryNode<decltype(functor)>(
          functor, std::move(lhs), std::move(rhs)));
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.pop_back();
  stack_.back() = std::move(node);
  return true;
}

}  // namespace grnxx
