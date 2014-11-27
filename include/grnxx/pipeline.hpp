#ifndef GRNXX_PIPELINE_HPP
#define GRNXX_PIPELINE_HPP

#include <limits>
#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/merger.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/table.hpp"

namespace grnxx {

struct PipelineOptions {
};

class Pipeline {
 public:
  Pipeline() = default;
  virtual ~Pipeline() = default;

  virtual const Table *table() const = 0;

  // Read all the records through the pipeline.
  //
  // On success, returns true.
  // On failure, throws an exception.
  virtual void flush(Array<Record> *records) = 0;
};

class PipelineBuilder {
 public:
  // Create an object for building a pipeline.
  //
  // On success, returns the builder.
  // On failure, throws an exception.
  static std::unique_ptr<PipelineBuilder> create(const Table *table);

  PipelineBuilder() = default;
  virtual ~PipelineBuilder() = default;

  // Return the associated table.
  virtual const Table *table() const = 0;

  // Push a cursor.
  //
  // On failure, throws an exception.
  virtual void push_cursor(std::unique_ptr<Cursor> &&cursor) = 0;

  // Push a filter.
  //
  // On failure, throws an exception.
  virtual void push_filter(
      std::unique_ptr<Expression> &&expression,
      size_t offset = 0,
      size_t limit = std::numeric_limits<size_t>::max()) = 0;

  // Push an adjuster.
  //
  // On failure, throws an exception.
  virtual void push_adjuster(std::unique_ptr<Expression> &&expression) = 0;

  // Push a sorter.
  //
  // On failure, throws an exception.
  virtual void push_sorter(std::unique_ptr<Sorter> &&sorter) = 0;

  // Push a merger.
  //
  // On failure, throws an exception.
  virtual void push_merger(const MergerOptions &options = MergerOptions()) = 0;

  // Clear the internal stack.
  virtual void clear() = 0;

  // Complete building a pipeline and clear the internal stack.
  //
  // Fails if the stack is empty or contains more than one nodes.
  //
  // On success, returns the expression.
  // On failure, throws an exception.
  virtual std::unique_ptr<Pipeline> release(
      const PipelineOptions &options = PipelineOptions()) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_PIPELINE_HPP
