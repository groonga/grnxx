#include "grnxx/sorter.hpp"

#include <new>

#include "grnxx/impl/sorter.hpp"

namespace grnxx {

std::unique_ptr<Sorter> Sorter::create(
    Array<SorterOrder> &&orders,
    const SorterOptions &options) try {
  return std::unique_ptr<Sorter>(
      new impl::Sorter(std::move(orders), options));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace grnxx
