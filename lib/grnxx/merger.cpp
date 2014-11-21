#include "grnxx/merger.hpp"

#include "grnxx/impl/merger.hpp"

namespace grnxx {

std::unique_ptr<Merger> Merger::create(const MergerOptions &options) {
  return std::unique_ptr<Merger>(impl::Merger::create(options));
}

}  // namespace grnxx
