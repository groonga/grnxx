#ifndef GRNXX_IMPL_SORTER_HPP
#define GRNXX_IMPL_SORTER_HPP

#include "grnxx/impl/table.hpp"
#include "grnxx/sorter.hpp"

namespace grnxx {
namespace impl {
namespace sorter {

class Node;

}  // namespace sorter

using SorterInterface = grnxx::Sorter;

class Sorter : public SorterInterface {
 public:
  using Node = sorter::Node;

  // -- Public API (grnxx/sorter.hpp) --

  Sorter(Array<SorterOrder> &&orders, const SorterOptions &options);
  ~Sorter();

  const Table *table() const {
    return table_;
  }
  void reset(Array<Record> *records);
  void progress();
  void finish();
  void sort(Array<Record> *records);

 private:
  const Table *table_;
  Array<std::unique_ptr<Node>> nodes_;
  Array<Record> *records_;
  size_t offset_;
  size_t limit_;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_SORTER_HPP
