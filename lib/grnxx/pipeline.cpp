#include "grnxx/pipeline.hpp"

#include <new>

#include "grnxx/impl/pipeline.hpp"

namespace grnxx {

std::unique_ptr<PipelineBuilder> PipelineBuilder::create(
    const Table *table) try {
  return std::unique_ptr<PipelineBuilder>(
      new impl::PipelineBuilder(static_cast<const impl::Table *>(table)));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace grnxx
