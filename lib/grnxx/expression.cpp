#include "grnxx/expression.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
#include "grnxx/record.hpp"
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

  // Filter out false records.
  //
  // Evaluates the expression for the given record set and removes records
  // whose evaluation results are false.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool filter(Error *error, RecordSet *record_set) = 0;

  // Evaluate the expression subtree.
  //
  // The evaluation results are stored into each expression node.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool evaluate(Error *error, const RecordSet &record_set) = 0;
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

  virtual bool filter(Error *error, RecordSet *record_set);

  virtual bool evaluate(Error *error, const RecordSet &record_set);

  T get(Int i) const {
    return values_[i];
  }

 protected:
  std::vector<T> values_;
};

template <typename T>
bool Node<T>::filter(Error *error, RecordSet *record_set) {
  // TODO: Define "This type is not supported" error.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

template <>
bool Node<Bool>::filter(Error *error, RecordSet *record_set) {
  if (!evaluate(error, *record_set)) {
    return false;
  }
  Int dest = 0;
  for (Int i = 0; i < record_set->size(); ++i) {
    if (values_[i]) {
      record_set->set(dest, record_set->get(i));
      ++dest;
    }
  }
  return record_set->resize(error, dest);
}

template <typename T>
bool Node<T>::evaluate(Error *error, const RecordSet &record_set) {
  // TODO: This should be a pure virtual function.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
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

  bool evaluate(Error *error, const RecordSet &record_set);

 private:
  T datum_;
};

template <typename T>
bool DatumNode<T>::evaluate(Error *error, const RecordSet &record_set) {
  try {
    this->values_.resize(record_set.size(), datum_);
    return true;
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
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

  bool evaluate(Error *error, const RecordSet &record_set);

 private:
  std::string datum_;
};

bool DatumNode<Text>::evaluate(Error *error, const RecordSet &record_set) {
  try {
    this->values_.resize(record_set.size(),
                         Text(datum_.data(), datum_.size()));
    return true;
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
}

// -- RowIDNode --

class RowIDNode : public Node<Int> {
 public:
  RowIDNode() : Node<Int>() {}
  ~RowIDNode() {}

  NodeType node_type() const {
    return ROW_ID_NODE;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    try {
      this->values_.resize(record_set.size());
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
    for (Int i = 0; i < record_set.size(); ++i) {
      this->values_[i] = record_set.get(i).row_id;
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

  bool evaluate(Error *error, const RecordSet &record_set) {
    try {
      this->values_.resize(record_set.size());
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
    for (Int i = 0; i < record_set.size(); ++i) {
      this->values_[i] = record_set.get(i).score;
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

  bool evaluate(Error *error, const RecordSet &record_set) {
    try {
      this->values_.resize(record_set.size());
    } catch (...) {
      GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
      return false;
    }
    for (Int i = 0; i < record_set.size(); ++i) {
      this->values_[i] = column_->get(record_set.get(i).row_id);
    }
    return true;
  }

 private:
  const ColumnImpl<T> *column_;
};

// -- OperatorNode --

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

  bool evaluate(Error *error, const RecordSet &record_set);

 private:
  Op operator_;
  unique_ptr<Node<Arg1>> lhs_;
  unique_ptr<Node<Arg2>> rhs_;
};

template <typename Op>
bool BinaryNode<Op>::evaluate(Error *error, const RecordSet &record_set) {
  try {
    this->values_.resize(record_set.size());
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  if (!lhs_->evaluate(error, record_set) ||
      !rhs_->evaluate(error, record_set)) {
    return false;
  }
  for (Int i = 0; i < record_set.size(); ++i) {
    this->values_[i] = operator_(lhs_->get(i), rhs_->get(i));
  }
  return true;
}

class LogicalAndNode : public Node<Bool> {
 public:
  LogicalAndNode(unique_ptr<ExpressionNode> &&lhs,
                 unique_ptr<ExpressionNode> &&rhs)
      : Node<Bool>(),
        lhs_(static_cast<Node<Bool> *>(lhs.release())),
        rhs_(static_cast<Node<Bool> *>(rhs.release())) {}
  virtual ~LogicalAndNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  bool filter(Error *error, RecordSet *record_set);

  bool evaluate(Error *error, const RecordSet &record_set);

 private:
  unique_ptr<Node<Bool>> lhs_;
  unique_ptr<Node<Bool>> rhs_;
};

bool LogicalAndNode::filter(Error *error, RecordSet *record_set) {
  return lhs_->filter(error, record_set) && rhs_->filter(error, record_set);
}

bool LogicalAndNode::evaluate(Error *error, const RecordSet &record_set) {
  // TODO: Don't evaluate rhs entries if lhs entries are false.
  try {
    this->values_.resize(record_set.size());
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  if (!lhs_->evaluate(error, record_set) ||
      !rhs_->evaluate(error, record_set)) {
    return false;
  }
  for (Int i = 0; i < record_set.size(); ++i) {
    this->values_[i] = lhs_->get(i) && rhs_->get(i);
  }
  return true;
}

class LogicalOrNode : public Node<Bool> {
 public:
  LogicalOrNode(unique_ptr<ExpressionNode> &&lhs,
                 unique_ptr<ExpressionNode> &&rhs)
      : Node<Bool>(),
        lhs_(static_cast<Node<Bool> *>(lhs.release())),
        rhs_(static_cast<Node<Bool> *>(rhs.release())) {}
  virtual ~LogicalOrNode() {}

  NodeType node_type() const {
    return OPERATOR_NODE;
  }

  // TODO: filter() must be specialized.
//  bool filter(Error *error, RecordSet *record_set);

  bool evaluate(Error *error, const RecordSet &record_set);

 private:
  unique_ptr<Node<Bool>> lhs_;
  unique_ptr<Node<Bool>> rhs_;
};

bool LogicalOrNode::evaluate(Error *error, const RecordSet &record_set) {
  // TODO: Don't evaluate rhs entries if lhs entries are true.
  try {
    this->values_.resize(record_set.size());
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  if (!lhs_->evaluate(error, record_set) ||
      !rhs_->evaluate(error, record_set)) {
    return false;
  }
  for (Int i = 0; i < record_set.size(); ++i) {
    this->values_[i] = lhs_->get(i) || rhs_->get(i);
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

bool Expression::filter(Error *error, RecordSet *record_set) {
  return root_->filter(error, record_set);
}

Expression::Expression(const Table *table, unique_ptr<ExpressionNode> &&root)
    : table_(table),
      root_(std::move(root)) {}

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
  try {
    stack_.reserve(stack_.size() + 1);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  // TODO: DatumNode::create() should be provided to get error information.
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
  stack_.push_back(std::move(node));
  return true;
}

bool ExpressionBuilder::push_column(Error *error, String name) {
  try {
    stack_.reserve(stack_.size() + 1);
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
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
    // TODO: The following switch should be done in ColumnNode::create() or ...
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
        // TODO: Not supported yet.
        GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
        return false;
      }
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.push_back(std::move(node));
  return true;
}

bool ExpressionBuilder::push_operator(Error *error,
                                      OperatorType operator_type) {
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

bool ExpressionBuilder::push_logical_and_operator(Error *error) {
  if (stack_.size() < 2) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
    return false;
  }
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if ((lhs->data_type() != BOOL_DATA) ||
      (rhs->data_type() != BOOL_DATA)) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong type");
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
  if (stack_.size() < 2) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
    return false;
  }
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if ((lhs->data_type() != BOOL_DATA) ||
      (rhs->data_type() != BOOL_DATA)) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong type");
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
  if (stack_.size() < 2) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
    return false;
  }
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Type conflict");
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
  if (stack_.size() < 2) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
    return false;
  }
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Type conflict");
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
  if (stack_.size() < 2) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
    return false;
  }
  auto &lhs = stack_[stack_.size() - 2];
  auto &rhs = stack_[stack_.size() - 1];
  if (lhs->data_type() != rhs->data_type()) {
    // TODO: Define a better error code.
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Type conflict");
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

}  // namespace grnxx
