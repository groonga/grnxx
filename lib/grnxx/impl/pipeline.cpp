#include "grnxx/impl/pipeline.hpp"

namespace grnxx {
namespace impl {
namespace pipeline {

// -- Node --

class Node {
 public:
  Node() = default;
  virtual ~Node() = default;

  // Read the next block of records.
  //
  // On success, returns the number of records read.
  // On failure, throws an exception.
  virtual size_t read_next(Array<Record> *records) = 0;

  // Read all the records.
  //
  // On success, returns the number of records read.
  // On failure, throws an exception.
  virtual size_t read_all(Array<Record> *records);
};

size_t Node::read_all(Array<Record> *records) {
  size_t total_count = 0;
  for ( ; ; ) {
    size_t count = read_next(records);
    if (count == 0) {
      break;
    }
    total_count += count;
  }
  return total_count;
}

// --- CursorNode ---

class CursorNode : public Node {
 public:
  explicit CursorNode(std::unique_ptr<Cursor> &&cursor)
      : Node(),
        cursor_(std::move(cursor)) {}
  ~CursorNode() = default;

  size_t read_next(Array<Record> *records);
  size_t read_all(Array<Record> *records);

 private:
  std::unique_ptr<Cursor> cursor_;
};

size_t CursorNode::read_next(Array<Record> *records) {
  // TODO: The following block size (1024) should be optimized.
  return cursor_->read(1024, records);
}

size_t CursorNode::read_all(Array<Record> *records) {
  return cursor_->read_all(records);
}

// --- FilterNode ---

class FilterNode : public Node {
 public:
  FilterNode(std::unique_ptr<Node> &&arg,
             std::unique_ptr<Expression> &&expression,
             size_t offset,
             size_t limit)
      : Node(),
        arg_(std::move(arg)),
        expression_(std::move(expression)),
        offset_(offset),
        limit_(limit) {}
  ~FilterNode() = default;

  size_t read_next(Array<Record> *records);

 private:
  std::unique_ptr<Node> arg_;
  std::unique_ptr<Expression> expression_;
  size_t offset_;
  size_t limit_;
};

size_t FilterNode::read_next(Array<Record> *records) {
  // TODO: The following threshold (1024) should be optimized.
  size_t offset = records->size();
  while (limit_ > 0) {
    size_t count = arg_->read_next(records);
    if (count == 0) {
      break;
    }
    ArrayRef<Record> ref = records->ref(records->size() - count, count);
    expression_->filter(ref, &ref);
    if (offset_ > 0) {
      if (offset_ >= ref.size()) {
        offset_ -= ref.size();
        ref = ref.ref(0, 0);
      } else {
        for (size_t i = offset_; i < ref.size(); ++i) {
          ref[i - offset_] = ref[i];
        }
        ref = ref.ref(0, ref.size() - offset_);
        offset_ = 0;
      }
    }
    if (ref.size() > limit_) {
      ref = ref.ref(0, limit_);
    }
    limit_ -= ref.size();
    records->resize(records->size() - count + ref.size());
    if ((records->size() - offset) >= 1024) {
      break;
    }
  }
  return records->size() - offset;
}

// --- AdjusterNode ---

class AdjusterNode : public Node {
 public:
  explicit AdjusterNode(std::unique_ptr<Node> &&arg,
                        std::unique_ptr<Expression> &&expression)
      : Node(),
        arg_(std::move(arg)),
        expression_(std::move(expression)) {}
  ~AdjusterNode() = default;

  size_t read_next(Array<Record> *records);

 private:
  std::unique_ptr<Node> arg_;
  std::unique_ptr<Expression> expression_;
};

size_t AdjusterNode::read_next(Array<Record> *records) {
  size_t offset = records->size();
  size_t count = arg_->read_next(records);
  expression_->adjust(records, offset);
  return count;
}

// --- SorterNode ---

class SorterNode : public Node {
 public:
  explicit SorterNode(std::unique_ptr<Node> &&arg,
                      std::unique_ptr<Sorter> &&sorter)
      : Node(),
        arg_(std::move(arg)),
        sorter_(std::move(sorter)) {}
  ~SorterNode() = default;

  size_t read_next(Array<Record> *records);

 private:
  std::unique_ptr<Node> arg_;
  std::unique_ptr<Sorter> sorter_;
};

size_t SorterNode::read_next(Array<Record> *records) {
  if (arg_->read_next(records) == 0) {
    return 0;
  }
  sorter_->reset(records);
  do {
    sorter_->progress();
  } while (arg_->read_next(records) != 0);
  sorter_->finish();
  return records->size();
}

// --- MergerNode ---

class MergerNode : public Node {
 public:
  explicit MergerNode(std::unique_ptr<Node> &&arg1,
                      std::unique_ptr<Node> &&arg2,
                      std::unique_ptr<Merger> &&merger)
      : Node(),
        arg1_(std::move(arg1)),
        arg2_(std::move(arg2)),
        merger_(std::move(merger)) {}
  ~MergerNode() = default;

  size_t read_next(Array<Record> *records);

 private:
  std::unique_ptr<Node> arg1_;
  std::unique_ptr<Node> arg2_;
  std::unique_ptr<Merger> merger_;
};

size_t MergerNode::read_next(Array<Record> *records) {
  Array<Record> arg1_records;
  Array<Record> arg2_records;
  arg1_->read_all(&arg1_records);
  arg2_->read_all(&arg2_records);
  if ((arg1_records.size() == 0) && (arg2_records.size() == 0)) {
    return 0;
  }
  merger_->merge(&arg1_records, &arg2_records, records);
  return records->size();
}

}  // namespace pipeline

using namespace pipeline;

Pipeline::Pipeline(const Table *table,
                   std::unique_ptr<Node> &&root,
                   const PipelineOptions &)
    : PipelineInterface(),
      table_(table),
      root_(std::move(root)) {}

void Pipeline::flush(Array<Record> *records) {
  root_->read_all(records);
}

PipelineBuilder::PipelineBuilder(const Table *table)
    : table_(table) {}

void PipelineBuilder::push_cursor(std::unique_ptr<Cursor> &&cursor) try {
  std::unique_ptr<Node> node(new CursorNode(std::move(cursor)));
  node_stack_.push_back(std::move(node));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void PipelineBuilder::push_filter(std::unique_ptr<Expression> &&expression,
                                  size_t offset,
                                  size_t limit) try {
  if (node_stack_.size() < 1) {
    throw "Not enough nodes";  // TODO
  }
  std::unique_ptr<Node> arg = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 1);
  std::unique_ptr<Node> node(
      new FilterNode(std::move(arg), std::move(expression), offset, limit));
  node_stack_.push_back(std::move(node));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void PipelineBuilder::push_adjuster(
    std::unique_ptr<Expression> &&expression) try {
  if (node_stack_.size() < 1) {
    throw "Not enough nodes";  // TODO
  }
  std::unique_ptr<Node> arg = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 1);
  std::unique_ptr<Node> node(
      new AdjusterNode(std::move(arg), std::move(expression)));
  node_stack_.push_back(std::move(node));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void PipelineBuilder::push_sorter(std::unique_ptr<Sorter> &&sorter) try {
  if (node_stack_.size() < 1) {
    throw "Not enough nodes";  // TODO
  }
  std::unique_ptr<Node> arg = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 1);
  std::unique_ptr<Node> node(
      new SorterNode(std::move(arg), std::move(sorter)));
  node_stack_.push_back(std::move(node));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void PipelineBuilder::push_merger(const MergerOptions &options) try {
  if (node_stack_.size() < 2) {
    throw "Not enough nodes";  // TODO
  }
  auto merger = Merger::create(options);
  std::unique_ptr<Node> arg2 = std::move(node_stack_[node_stack_.size() - 2]);
  std::unique_ptr<Node> arg1 = std::move(node_stack_[node_stack_.size() - 1]);
  node_stack_.resize(node_stack_.size() - 2);
  std::unique_ptr<Node> node(
      new MergerNode(std::move(arg1), std::move(arg2), std::move(merger)));
  node_stack_.push_back(std::move(node));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void PipelineBuilder::clear() {
  node_stack_.clear();
}

std::unique_ptr<PipelineInterface> PipelineBuilder::release(
    const PipelineOptions &options) try {
  if (node_stack_.size() != 1) {
    throw "Incomplete pipeline";  // TODO
  }
  std::unique_ptr<Node> root = std::move(node_stack_[0]);
  node_stack_.clear();
  return std::unique_ptr<PipelineInterface>(
      new Pipeline(table_, std::move(root), options));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace impl
}  // namespace grnxx
