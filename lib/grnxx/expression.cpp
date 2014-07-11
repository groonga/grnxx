#include "grnxx/expression.hpp"

#include "grnxx/column_impl.hpp"
#include "grnxx/datum.hpp"
#include "grnxx/error.hpp"
#include "grnxx/record.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

enum ExpressionNodeType {
  EXPRESSION_DATUM_NODE,
  EXPRESSION_ROW_ID_NODE,
  EXPRESSION_SCORE_NODE,
  EXPRESSION_COLUMN_NODE,
  EXPRESSION_OPERATOR_NODE
};

class ExpressionNode {
 public:
  ExpressionNode() {}
  virtual ~ExpressionNode() {}

  virtual ExpressionNodeType node_type() const = 0;
  virtual DataType data_type() const = 0;

  virtual bool filter(Error *error, RecordSet *record_set) {
    // TODO: Set an "This type is not supported" error.
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
    return false;
  }
  virtual bool evaluate(Error *error, const RecordSet &record_set) {
    // TODO: This should be a pure virtual function.
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
    return false;
  }
};

// -- ExpressionDatumNode --

class ExpressionDatumNode : public ExpressionNode {
 public:
  ExpressionDatumNode() : ExpressionNode() {}
  virtual ~ExpressionDatumNode() {}

  ExpressionNodeType node_type() const {
    return EXPRESSION_DATUM_NODE;
  }
};

class ExpressionBoolDatumNode : public ExpressionDatumNode {
 public:
  ExpressionBoolDatumNode(Bool datum)
      : ExpressionDatumNode(),
        datum_(datum) {}
  ~ExpressionBoolDatumNode() {}

  DataType data_type() const {
    return BOOL_DATA;
  }

  bool filter(Error *error, RecordSet *record_set) {
    if (!datum_) {
      record_set->clear();
    }
    return true;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    return true;
  }

  Bool get(Int i) const {
    return datum_;
  }

 private:
  Bool datum_;
};

class ExpressionIntDatumNode : public ExpressionDatumNode {
 public:
  ExpressionIntDatumNode(Int datum)
      : ExpressionDatumNode(),
        datum_(datum) {}
  ~ExpressionIntDatumNode() {}

  DataType data_type() const {
    return INT_DATA;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    return true;
  }

  Int get(Int i) const {
    return datum_;
  }

 private:
  Int datum_;
};

// -- ExpressionRowIDNode --

class ExpressionRowIDNode : public ExpressionNode {
 public:
  ExpressionRowIDNode()
      : ExpressionNode(),
        record_set_(nullptr) {}
  ~ExpressionRowIDNode() {}

  ExpressionNodeType node_type() const {
    return EXPRESSION_ROW_ID_NODE;
  }
  DataType data_type() const {
    return INT_DATA;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    record_set_ = &record_set;
    return true;
  }

  Int get(Int i) const {
    return record_set_->get(i).row_id;
  }

 private:
  const RecordSet *record_set_;
};

// -- ExpressionScoreNode --

class ExpressionScoreNode : public ExpressionNode {
 public:
  ExpressionScoreNode() : ExpressionNode() {}
  ~ExpressionScoreNode() {}

  ExpressionNodeType node_type() const {
    return EXPRESSION_SCORE_NODE;
  }
  DataType data_type() const {
    return FLOAT_DATA;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    record_set_ = &record_set;
    return true;
  }

  Float get(Int i) const {
    return record_set_->get(i).score;
  }

 private:
  const RecordSet *record_set_;
};

// -- ExpressionColumnNode --

class ExpressionColumnNode : public ExpressionNode {
 public:
  ExpressionColumnNode() : ExpressionNode() {}
  virtual ~ExpressionColumnNode() {}

  ExpressionNodeType node_type() const {
    return EXPRESSION_COLUMN_NODE;
  }
};

class ExpressionBoolColumnNode : public ExpressionColumnNode {
 public:
  ExpressionBoolColumnNode(const BoolColumn *column)
      : ExpressionColumnNode(),
        column_(column),
        record_set_(nullptr) {}
  ~ExpressionBoolColumnNode() {}

  DataType data_type() const {
    return column_->data_type();
  }

//  bool filter(Error *error, RecordSet *record_set) {
//    return false;
//  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    record_set_ = &record_set;
    return true;
  }

  Bool get(Int i) const {
    return column_->get(record_set_->get(i).row_id);
  }

 private:
  const BoolColumn *column_;
  const RecordSet *record_set_;
};

class ExpressionIntColumnNode : public ExpressionColumnNode {
 public:
  ExpressionIntColumnNode(const IntColumn *column)
      : ExpressionColumnNode(),
        column_(column),
        record_set_(nullptr) {}
  ~ExpressionIntColumnNode() {}

  DataType data_type() const {
    return column_->data_type();
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    record_set_ = &record_set;
    return true;
  }

  Int get(Int i) const {
    return column_->get(record_set_->get(i).row_id);
  }

 private:
  const IntColumn *column_;
  const RecordSet *record_set_;
};

// -- ExpressionOperatorNode --

// TODO: 演算子の実装はどうしても長くなるので，別ファイルに移行すべきかもしれません．

// TODO: ExpressionOperatorNode<T> （T: 評価結果のデータ型）に data_type と
//       バッファを持たせてインタフェースを統一しないと，再帰的な組み合わせが発生します．

class ExpressionOperatorNode : public ExpressionNode {
 public:
  ExpressionOperatorNode() : ExpressionNode() {}
  virtual ~ExpressionOperatorNode() {}

  ExpressionNodeType node_type() const {
    return EXPRESSION_OPERATOR_NODE;
  }

  virtual OperatorType operator_type() const = 0;
};

template <typename T, typename U>
class ExpressionEqualOperatorNode : public ExpressionOperatorNode {
 public:
  ExpressionEqualOperatorNode() : ExpressionOperatorNode() {}
  virtual ~ExpressionEqualOperatorNode() {}

  DataType data_type() const {
    return BOOL_DATA;
  }
  OperatorType operator_type() const {
    return EQUAL_OPERATOR;
  }

  bool evaluate(Error *error, const RecordSet &record_set) {
    // TODO: Not supported yet.
    GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
    return false;
  }

  Int get(Int i) const {
    return values_[i];
  }

 private:
  unique_ptr<T> lhs_;
  unique_ptr<U> rhs_;
  std::vector<Bool> values_;
};

// -- Expression --

unique_ptr<Expression> Expression::create(Error *error,
                                          unique_ptr<ExpressionNode> &&root) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return nullptr;
}

Expression::~Expression() {}

DataType Expression::data_type() const {
  return root_->data_type();
}

bool Expression::filter(Error *error, RecordSet *record_set) {
  // TODO
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

Expression::Expression(Table *table, unique_ptr<ExpressionNode> &&root)
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
  // TODO: ExpressionDatumNode::create() should be provided to get error
  //       information.
  unique_ptr<ExpressionDatumNode> node;
  switch (datum.type()) {
    case BOOL_DATA: {
      node.reset(new (nothrow) ExpressionBoolDatumNode(datum.force_bool()));
      break;
    }
    case INT_DATA: {
      node.reset(new (nothrow) ExpressionIntDatumNode(datum.force_int()));
      break;
    }
    default: {
      // TODO: Other types are not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Not supported yet");
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
    node.reset(new (nothrow) ExpressionRowIDNode());
  } else if (name == "_score") {
    node.reset(new (nothrow) ExpressionScoreNode());
  } else {
    Column *column = table_->find_column(error, name);
    if (!column) {
      return false;
    }
    // TODO: The following switch should be done in
    //       ExpressionColumnNode::create().
    switch (column->data_type()) {
      case BOOL_DATA: {
        node.reset(new (nothrow) ExpressionBoolColumnNode(
            static_cast<BoolColumn *>(column)));
        break;
      }
      case INT_DATA: {
        node.reset(new (nothrow) ExpressionIntColumnNode(
            static_cast<IntColumn *>(column)));
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
  return true;
}

bool ExpressionBuilder::push_operator(Error *error,
                                      OperatorType operator_type) {
  unique_ptr<ExpressionNode> node;
  switch (operator_type) {
    case EQUAL_OPERATOR: {
      if (stack_.size() != 2) {
        // TODO: Define a better error code.
        GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Not enough operands");
        return false;
      }
      if (stack_[stack_.size() - 1]->data_type() !=
          stack_[stack_.size() - 2]->data_type()) {
        // TODO: Define a better error code.
        GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Type conflict");
        return false;
      }
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
    case NOT_EQUAL_OPERATOR: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
    default: {
      // TODO: Not supported yet.
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return false;
    }
  }
  if (!node) {
    // TODO: Error information should be set in the above switch.
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  return true;
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
  return Expression::create(error, std::move(root_node));
}

ExpressionBuilder::ExpressionBuilder(const Table *table) : table_(table) {}

}  // namespace grnxx
