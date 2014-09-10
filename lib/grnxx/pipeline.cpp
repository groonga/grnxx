#include "grnxx/pipeline.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"

namespace grnxx {
namespace pipeline {

class Node {
 public:
  Node() {}
  virtual ~Node() {}

  virtual Int read_next(Error *error, Array<Record> *records) = 0;
  virtual Int read_all(Error *error, Array<Record> *records);
};

Int Node::read_all(Error *error, Array<Record> *records) {
  Int total_count = 0;
  for ( ; ; ) {
    Int count = read_next(error, records);
    if (count == -1) {
      return -1;
    } else if (count == 0) {
      break;
    }
    total_count += count;
  }
  return total_count;
}

class CursorNode : public Node {
 public:
  explicit CursorNode(unique_ptr<Cursor> &&cursor)
      : Node(),
        cursor_(std::move(cursor)) {}
  ~CursorNode() {}

  Int read_next(Error *error, Array<Record> *records);
  Int read_all(Error *error, Array<Record> *records);

 private:
  unique_ptr<Cursor> cursor_;
};

Int CursorNode::read_next(Error *error, Array<Record> *records) {
  // TODO: The following block size (1024) should be optimized.
  return cursor_->read(error, 1024, records);
}

Int CursorNode::read_all(Error *error, Array<Record> *records) {
  return cursor_->read_all(error, records);
}

class FilterNode : public Node {
 public:
  FilterNode(unique_ptr<Node> &&arg,
             unique_ptr<Expression> &&expression,
             Int offset,
             Int limit)
      : Node(),
        arg_(std::move(arg)),
        expression_(std::move(expression)),
        offset_(offset),
        limit_(limit) {}
  ~FilterNode() {}

  Int read_next(Error *error, Array<Record> *records);

 private:
  unique_ptr<Node> arg_;
  unique_ptr<Expression> expression_;
  Int offset_;
  Int limit_;
};

Int FilterNode::read_next(Error *error, Array<Record> *records) {
  // TODO: The following threshold (1024) should be optimized.
  Int offset = records->size();
  while (limit_ > 0) {
    Int count = arg_->read_next(error, records);
    if (count == -1) {
      return -1;
    } else if (count == 0) {
      break;
    }
    ArrayRef<Record> ref = records->ref(records->size() - count, count);
    if (!expression_->filter(error, ref, &ref)) {
      return -1;
    }
    if (offset_ > 0) {
      if (offset_ >= ref.size()) {
        offset_ -= ref.size();
        ref = ref.ref(0, 0);
      } else {
        for (Int i = offset_; i < ref.size(); ++i) {
          ref.set(i - offset_, ref[i]);
        }
        ref = ref.ref(0, ref.size() - offset_);
        offset_ = 0;
      }
    }
    if (ref.size() > limit_) {
      ref = ref.ref(0, limit_);
    }
    limit_ -= ref.size();
    if (!records->resize(error, records->size() - count + ref.size())) {
      return -1;
    }
    if ((records->size() - offset) >= 1024) {
      break;
    }
  }
  return records->size() - offset;
}

class AdjusterNode : public Node {
 public:
  explicit AdjusterNode(unique_ptr<Node> &&arg,
                        unique_ptr<Expression> &&expression)
      : Node(),
        arg_(std::move(arg)),
        expression_(std::move(expression)) {}
  ~AdjusterNode() {}

  Int read_next(Error *error, Array<Record> *records);

 private:
  unique_ptr<Node> arg_;
  unique_ptr<Expression> expression_;
};

Int AdjusterNode::read_next(Error *error, Array<Record> *records) {
  Int offset = records->size();
  Int count = arg_->read_next(error, records);
  if (count == -1) {
    return -1;
  }
  if (!expression_->adjust(error, records, offset)) {
    return -1;
  }
  return count;
}

class SorterNode : public Node {
 public:
  explicit SorterNode(unique_ptr<Node> &&arg,
                      unique_ptr<Sorter> &&sorter)
      : Node(),
        arg_(std::move(arg)),
        sorter_(std::move(sorter)) {}
  ~SorterNode() {}

  Int read_next(Error *error, Array<Record> *records);

 private:
  unique_ptr<Node> arg_;
  unique_ptr<Sorter> sorter_;
};

Int SorterNode::read_next(Error *error, Array<Record> *records) {
  Int count = arg_->read_all(error, records);
  if (count == -1) {
    return -1;
  } else if (count == 0) {
    return 0;
  }
  if (!sorter_->sort(error, records)) {
    return -1;
  }
  return records->size();
}

}  // namespace pipeline

using namespace pipeline;

Pipeline::~Pipeline() {}

bool Pipeline::flush(Error *error, Array<Record> *records) {
  return root_->read_all(error, records) >= 0;
}

unique_ptr<Pipeline> Pipeline::create(Error *error,
                                      const Table *table,
                                      unique_ptr<PipelineNode> &&root,
                                      const PipelineOptions &) {
  unique_ptr<Pipeline> pipeline(
      new (nothrow) Pipeline(table, std::move(root)));
  if (!pipeline) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  return pipeline;
}

Pipeline::Pipeline(const Table *table,
                   unique_ptr<PipelineNode> &&root)
    : table_(table),
      root_(std::move(root)) {}

unique_ptr<PipelineBuilder> PipelineBuilder::create(Error *error,
                                                    const Table *table) {
  unique_ptr<PipelineBuilder> builder(new (nothrow) PipelineBuilder);
  if (!builder) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  builder->table_ = table;
  return builder;
}

PipelineBuilder::~PipelineBuilder() {}

bool PipelineBuilder::push_cursor(Error *error, unique_ptr<Cursor> &&cursor) {
  // Reserve a space for a new node.
  if (!stack_.reserve(error, stack_.size() + 1)) {
    return false;
  }
  unique_ptr<Node> node(new (nothrow) CursorNode(std::move(cursor)));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  // This push_back() must not fail because a space is already reserved.
  stack_.push_back(nullptr, std::move(node));
  return true;
}

bool PipelineBuilder::push_filter(Error *error,
                                  unique_ptr<Expression> &&expression,
                                  Int offset, Int limit) {
  if (stack_.size() < 1) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough nodes");
    return false;
  }
  unique_ptr<Node> arg = std::move(stack_[stack_.size() - 1]);
  stack_.resize(nullptr, stack_.size() - 1);
  unique_ptr<Node> node(
      new (nothrow) FilterNode(std::move(arg), std::move(expression),
                               offset, limit));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.push_back(error, std::move(node));
  return true;
}

bool PipelineBuilder::push_adjuster(Error *error,
                                    unique_ptr<Expression> &&expression) {
  if (stack_.size() < 1) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough nodes");
    return false;
  }
  unique_ptr<Node> arg = std::move(stack_[stack_.size() - 1]);
  stack_.resize(nullptr, stack_.size() - 1);
  unique_ptr<Node> node(
      new (nothrow) AdjusterNode(std::move(arg), std::move(expression)));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.push_back(error, std::move(node));
  return true;
}

bool PipelineBuilder::push_sorter(Error *error, unique_ptr<Sorter> &&sorter) {
  if (stack_.size() < 1) {
    GRNXX_ERROR_SET(error, INVALID_OPERAND, "Not enough nodes");
    return false;
  }
  unique_ptr<Node> arg = std::move(stack_[stack_.size() - 1]);
  stack_.resize(nullptr, stack_.size() - 1);
  unique_ptr<Node> node(
      new (nothrow) SorterNode(std::move(arg), std::move(sorter)));
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  stack_.push_back(error, std::move(node));
  return true;
}

void PipelineBuilder::clear() {
  stack_.clear();
}

unique_ptr<Pipeline> PipelineBuilder::release(Error *error,
                                              const PipelineOptions &options) {
  if (stack_.size() != 1) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Incomplete pipeline");
    return nullptr;
  }
  unique_ptr<PipelineNode> root = std::move(stack_[0]);
  stack_.clear();
  return Pipeline::create(error, table_, std::move(root), options);
}

PipelineBuilder::PipelineBuilder() : table_(nullptr), stack_() {}

}  // namespace grnxx
