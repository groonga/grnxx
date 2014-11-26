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

  // Create a node for sorting records in "order".
  //
  // On success, returns the node.
  // On failure, throws an exception.
  static Node *create_node(SorterOrder &&order);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_SORTER_HPP
