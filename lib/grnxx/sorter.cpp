#include "grnxx/sorter.hpp"

#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/order.hpp"
#include "grnxx/record.hpp"

namespace grnxx {

class SorterNode {
 public:
  static unique_ptr<SorterNode> create(Error *error, Order &&order);

  SorterNode() : next_(nullptr) {}
  virtual ~SorterNode() {}

  // Return the next node.
  SorterNode *next() const {
    return next_.get();
  }
  // Set the next node.
  void set_next(unique_ptr<SorterNode> &&next) {
    next_ = std::move(next);
  }

  // Sort records in [begin, end).
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool sort(Error *error,
                    RecordSet *record_set,
                    Int begin,
                    Int end) = 0;

 private:
  unique_ptr<SorterNode> next_;
};

namespace {

template <typename T>
struct RegularComparer {
  using Value = T;
  bool operator()(T lhs, T rhs) const {
    return lhs < rhs;
  }
};

template <typename T>
struct ReverseComparer {
  using Value = T;
  bool operator()(T lhs, T rhs) const {
    return rhs < lhs;
  }
};

template <typename T>
class Node : public SorterNode {
 public:
  using Value = typename T::Value;

  Node(Order &&order) : SorterNode(), order_(std::move(order)) {}
  ~Node() {}

  bool sort(Error *error, RecordSet *record_set, Int begin, Int end);

 private:
  Order order_;
  std::vector<Value> values_;

  // TODO: A pointer to records is not available.
//  void quick_sort(Record *records, Value *values, Int size,
//                  Int begin, Int end);
//  void insertion_sort(Record *records, Value *values, Int size);
//  void move_pivot_first(Record *records, Value *values, Int size);
};

template <typename T>
bool Node<T>::sort(Error *error, RecordSet *record_set, Int begin, Int end) {
  return false;
}

}  // namespace

unique_ptr<SorterNode> SorterNode::create(Error *error, Order &&order) {
  unique_ptr<SorterNode> node;
  switch (order.expression->data_type()) {
    // TODO: Bool type does not support pointer.
//    case BOOL_DATA: {
//      node.reset(new (nothrow) Node<Bool>(std::move(order)));
//      break;
//    }
    case INT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<Int>>(std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<Int>>(std::move(order)));
      }
      break;
    }
    case FLOAT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(
            new (nothrow) Node<RegularComparer<Float>>(std::move(order)));
      } else {
        node.reset(
            new (nothrow) Node<ReverseComparer<Float>>(std::move(order)));
      }
      break;
    }
    case TIME_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(
            new (nothrow) Node<RegularComparer<Time>>(std::move(order)));
      } else {
        node.reset(
            new (nothrow) Node<ReverseComparer<Time>>(std::move(order)));
      }
      break;
    }
    case TEXT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(
            new (nothrow) Node<RegularComparer<Text>>(std::move(order)));
      } else {
        node.reset(
            new (nothrow) Node<ReverseComparer<Text>>(std::move(order)));
      }
      break;
    }
    default: {
      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
      return nullptr;
    }
  }
  if (!node) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
  }
  return node;
}

Sorter::~Sorter() {}

unique_ptr<Sorter> Sorter::create(
    Error *error,
    unique_ptr<OrderSet> &&order_set,
    const SorterOptions &options) {
  unique_ptr<Sorter> sorter(new (nothrow) Sorter);
  if (!sorter) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  for (Int i = order_set->size() - 1; i >= 0; --i) {
    unique_ptr<SorterNode> node(
        SorterNode::create(error, std::move(order_set->get(i))));
    if (!node) {
      return nullptr;
    }
    node->set_next(std::move(sorter->head_));
    sorter->head_ = std::move(node);
  }
  order_set.reset();
  return sorter;
}

bool Sorter::reset(Error *error, RecordSet *record_set) {
  record_set_ = record_set;
  return true;
}

bool Sorter::progress(Error *error) {
  // TODO: Incremental sorting is not supported yet.
  return true;
}

bool Sorter::finish(Error *error) {
  if (!record_set_) {
    // Nothing to do.
    return true;
  }
  if (record_set_->size() <= 1) {
    return true;
  }
  return head_->sort(error, record_set_, 0, record_set_->size());
}

bool Sorter::sort(Error *error, RecordSet *record_set) {
  return reset(error, record_set) && finish(error);
}

Sorter::Sorter() : record_set_(nullptr), head_() {}

}  // namespace grnxx
