#ifndef GRNXX_IMPL_PIPELINE_HPP
#define GRNXX_IMPL_PIPELINE_HPP

#include <memory>

#include "grnxx/array.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/pipeline.hpp"

namespace grnxx {
namespace impl {
namespace pipeline {

class Node;

}  // namespace pipeline

using PipelineInterface = grnxx::Pipeline;
using PipelineBuilderInterface = grnxx::PipelineBuilder;

class Pipeline : public PipelineInterface {
 public:
  using Node = pipeline::Node;

  // -- Public API (grnxx/expression.hpp) --
  explicit Pipeline(const Table *table,
                    std::unique_ptr<Node> &&root,
                    const PipelineOptions &options);
  ~Pipeline() = default;

  const Table *table() const {
    return table_;
  }

  void flush(Array<Record> *records);

 private:
  const Table *table_;
  std::unique_ptr<Node> root_;
};

class PipelineBuilder : public PipelineBuilderInterface {
 public:
  using Node = pipeline::Node;

  // -- Public API (grnxx/expression.hpp) --

  explicit PipelineBuilder(const Table *table);
  ~PipelineBuilder() = default;

  const Table *table() const {
    return table_;
  }

  void push_cursor(std::unique_ptr<Cursor> &&cursor);
  void push_filter(std::unique_ptr<Expression> &&expression,
                   size_t offset,
                   size_t limit);
  void push_adjuster(std::unique_ptr<Expression> &&expression);
  void push_sorter(std::unique_ptr<Sorter> &&sorter);
  void push_merger(const MergerOptions &options);

  void clear();

  std::unique_ptr<PipelineInterface> release(const PipelineOptions &options);

 private:
  const Table *table_;
  Array<std::unique_ptr<Node>> node_stack_;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_PIPELINE_HPP
