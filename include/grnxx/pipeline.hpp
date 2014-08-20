#ifndef GRNXX_PIPELINE_HPP
#define GRNXX_PIPELINE_HPP

#include "grnxx/array.hpp"
#include "grnxx/sorter.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace pipeline {

class Node;

}  // namespace pipeline

using PipelineNode = pipeline::Node;

class Pipeline {
 public:
  ~Pipeline();

  const Table *table() const {
    return table_;
  }

  // Read all the records through the pipeline.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool flush(Error *error, Array<Record> *records);

 private:
  const Table *table_;
  unique_ptr<PipelineNode> root_;

  static unique_ptr<Pipeline> create(Error *error,
                                     const Table *table,
                                     unique_ptr<PipelineNode> &&root,
                                     const PipelineOptions &options);

  Pipeline(const Table *table,
           unique_ptr<PipelineNode> &&root);

  friend class PipelineBuilder;
};

class PipelineBuilder {
 public:
  // Create an object for building a pipeline.
  //
  // On success, returns a poitner to the builder.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<PipelineBuilder> create(Error *error, const Table *table);

  ~PipelineBuilder();

  // Return the associated table.
  const Table *table() const {
    return table_;
  }

  // Push a cursor.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_cursor(Error *error, unique_ptr<Cursor> &&cursor);

  // Push a filter.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_filter(Error *error,
                   unique_ptr<Expression> &&expression,
                   Int offset = 0,
                   Int limit = numeric_limits<Int>::max());

  // Push an adjuster.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_adjuster(Error *error, unique_ptr<Expression> &&expression);

  // Push a sorter.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool push_sorter(Error *error, unique_ptr<Sorter> &&sorter);

  // TODO: Not supported yet.
//  bool push_merger(Error *error, const MergerOptions &options);

  // Clear the internal stack.
  void clear();

  // Complete building a pipeline and clear the internal stack.
  //
  // Fails if the stack is empty or contains more than one nodes.
  //
  // On success, returns a pointer to the expression.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<Pipeline> release(
      Error *error,
      const PipelineOptions &options = PipelineOptions());

 private:
  const Table *table_;
  Array<unique_ptr<PipelineNode>> stack_;

  PipelineBuilder();
};

}  // namespace grnxx

#endif  // GRNXX_PIPELINE_HPP
