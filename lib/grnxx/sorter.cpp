#include "grnxx/sorter.hpp"

#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"
#include "grnxx/order.hpp"
#include "grnxx/record.hpp"

namespace grnxx {

class SorterNode {
 public:
  static unique_ptr<SorterNode> create(Error *error, Order &&order);

  SorterNode() : next_(), error_(nullptr) {}
  virtual ~SorterNode() {}

  // Set the next node.
  void set_next(unique_ptr<SorterNode> &&next) {
    next_ = std::move(next);
  }
  // Set a reference to an error object.
  void set_error(Error *error) {
    error_ = error;
  }

  // Sort records in [begin, end).
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool sort(Error *error,
                    RecordSubset subset,
                    Int begin,
                    Int end) = 0;

 protected:
  unique_ptr<SorterNode> next_;
  Error *error_;
};

namespace {

template <typename T>
struct RegularComparer {
  using Value = T;
  bool operator()(Value lhs, Value rhs) const {
    return lhs < rhs;
  }
};

template <>
struct RegularComparer<GeoPoint> {
  using Value = GeoPoint;
  bool operator()(Value lhs, Value rhs) const {
    if (lhs.latitude() != rhs.latitude()) {
      return lhs.latitude() < rhs.latitude();
    }
    return lhs.longitude() < rhs.longitude();
  }
};

template <typename T>
struct ReverseComparer {
  using Value = T;
  bool operator()(Value lhs, Value rhs) const {
    return RegularComparer<T>()(rhs, lhs);
  }
};

template <typename T>
class Node : public SorterNode {
 public:
  using PriorTo = T;
  using Value   = typename PriorTo::Value;

  explicit Node(Order &&order)
      : SorterNode(),
        order_(std::move(order)),
        values_(),
        prior_to_() {}
  ~Node() {}

  bool sort(Error *error, RecordSubset records, Int begin, Int end);

 private:
  Order order_;
  Array<Value> values_;
  PriorTo prior_to_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the sorting range becomes small enough.
  bool quick_sort(RecordSubset records, Value *values,
                  Int begin, Int end);

  // Sort records with insertion sort.
  //
  // Insertion sort should be used when there few records.
  bool insertion_sort(RecordSubset records, Value *values);

  // Choose the pivot and move it to the front.
  void move_pivot_first(RecordSubset records, Value *values);
};

template <typename T>
bool Node<T>::sort(Error *error, RecordSubset records, Int begin, Int end) {
  if (!order_.expression->evaluate(error, records, &values_)) {
    return false;
  }
  set_error(error);
  return quick_sort(records, values_.data(), begin, end);
}

template <typename T>
bool Node<T>::quick_sort(RecordSubset records, Value *values,
                         Int begin, Int end) {
  // Use ternary quick sort if there are enough records.
  //
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records, values);
    const Value pivot = values[0];
    Int left = 1;
    Int right = records.size();
    Int pivot_left = 1;
    Int pivot_right = records.size();
    for ( ; ; ) {
      // Move entries based on comparison against the pivot.
      // Prior entries are moved to left.
      // Less prior entries are moved to right.
      // Entries which equal to the pivot are moved to the edges.
      while (left < right) {
        if (prior_to_(pivot, values[left])) {
          break;
        } else if (pivot == values[left]) {
          std::swap(values[left], values[pivot_left]);
          records.swap(left, pivot_left);
          ++pivot_left;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (prior_to_(values[right], pivot)) {
          break;
        } else if (values[right] == pivot) {
          --pivot_right;
          std::swap(values[right], values[pivot_right]);
          records.swap(right, pivot_right);
        }
      }
      if (left >= right) {
        break;
      }
      std::swap(values[left], values[right]);
      records.swap(left, right);
      ++left;
    }

    // Move left pivot-equivalent entries to the left side of the boundary.
    while (pivot_left > 0) {
      --pivot_left;
      --left;
      std::swap(values[pivot_left], values[left]);
      records.swap(pivot_left, left);
    }
    // Move right pivot-equivalent entries to the right side of the boundary.
    while (pivot_right < records.size()) {
      std::swap(values[pivot_right], values[right]);
      records.swap(pivot_right, right);
      ++pivot_right;
      ++right;
    }

    // Apply the next sort condition to the pivot-equivalent records.
    if (this->next_) {
      if (((right - left) >= 2) && (begin < right) && (end > left)) {
        Int next_begin = (begin < left) ? 0 : (begin - left);
        Int next_end = ((end > right) ? right : end) - left;
        if (!this->next_->sort(this->error_,
                               records.subset(left, right - left),
                               next_begin, next_end)) {
          return false;
        }
      }
    }

    // There are the left group and the right group.
    // A recursive call is used for sorting the smaller group.
    // The recursion depth is less than log_2(n) where n is the number of
    // records.
    // The next loop of the current call is used for sorting the larger group.
    if (left < (records.size() - right)) {
      if ((begin < left) && (left >= 2)) {
        Int next_end = (end < left) ? end : left;
        if (!quick_sort(records.subset(0, left), values, begin, next_end)) {
          return false;
        }
      }
      if (end <= right) {
        return true;
      }
      records = records.subset(right);
      values += right;
      begin -= right;
      if (begin < 0) {
        begin = 0;
      }
      end -= right;
    } else {
      if ((end > right) && ((records.size() - right) >= 2)) {
        Int next_begin = (begin < right) ? 0 : (begin - right);
        Int next_end = end - right;
        if (!quick_sort(records.subset(right),
                        values + right, next_begin, next_end)) {
          return false;
        }
      }
      if (begin >= left) {
        return true;
      }
      records = records.subset(0, left);
      if (end > left) {
        end = left;
      }
    }
  }

  if (records.size() >= 2) {
    return insertion_sort(records, values);
  }
  return true;
}

template <typename T>
bool Node<T>::insertion_sort(RecordSubset records, Value *values) {
  for (Int i = 1; i < records.size(); ++i) {
    for (Int j = i; j > 0; --j) {
      if (prior_to_(values[j], values[j - 1])) {
        std::swap(values[j], values[j - 1]);
        records.swap(j, j - 1);
      } else {
        break;
      }
    }
  }

  // Apply the next sorting if there are records having the same value.
  if (this->next_) {
    Int begin = 0;
    for (Int i = 1; i < records.size(); ++i) {
      if (values[i] != values[begin]) {
        if ((i - begin) >= 2) {
          if (!this->next_->sort(this->error_,
                                 records.subset(begin, i - begin),
                                 0, i - begin)) {
            return false;
          }
        }
        begin = i;
      }
    }
    if ((records.size() - begin) >= 2) {
      if (!this->next_->sort(this->error_,
                             records.subset(begin),
                             0, records.size() - begin)) {
        return false;
      }
    }
  }
  return true;
}

template <typename T>
void Node<T>::move_pivot_first(RecordSubset records, Value *values) {
  // Choose the median from values[1], values[1 / size], and values[size - 2].
  // The reason why not using values[0] and values[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  Int first = 1;
  Int middle = records.size() / 2;
  Int last = records.size() - 2;
  if (prior_to_(values[first], values[middle])) {
    // first < middle.
    if (prior_to_(values[middle], values[last])) {
      // first < middle < last.
      std::swap(values[0], values[middle]);
      records.swap(0, middle);
    } else if (prior_to_(values[first], values[last])) {
      // first < last < middle.
      std::swap(values[0], values[last]);
      records.swap(0, last);
    } else {
      // last < first < middle.
      std::swap(values[0], values[first]);
      records.swap(0, first);
    }
  } else if (prior_to_(values[last], values[middle])) {
    // last < middle < first.
    std::swap(values[0], values[middle]);
    records.swap(0, middle);
  } else if (prior_to_(values[last], values[first])) {
    // middle < last < first.
    std::swap(values[0], values[last]);
    records.swap(0, last);
  } else {
    // middle < first < last.
    std::swap(values[0], values[first]);
    records.swap(0, first);
  }
}

}  // namespace

unique_ptr<SorterNode> SorterNode::create(Error *error, Order &&order) {
  unique_ptr<SorterNode> node;
  switch (order.expression->data_type()) {
    // TODO: Bool does not support comparison operators <, >.
    //       Also, Bool does not support pointer.
//    case BOOL_DATA: {
//      if (order.type == REGULAR_ORDER) {
//        node.reset(new (nothrow) Node<RegularComparer<Bool>>(
//            std::move(order)));
//      } else {
//        node.reset(new (nothrow) Node<ReverseComparer<Bool>>(
//            std::move(order)));
//      }
//      break;
//    }
    case INT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<Int>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<Int>>(
            std::move(order)));
      }
      break;
    }
    case FLOAT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<Float>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<Float>>(
            std::move(order)));
      }
      break;
    }
    case TIME_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<Time>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<Time>>(
            std::move(order)));
      }
      break;
    }
    case GEO_POINT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<GeoPoint>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<GeoPoint>>(
            std::move(order)));
      }
      break;
    }
    case TEXT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) Node<RegularComparer<Text>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) Node<ReverseComparer<Text>>(
            std::move(order)));
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
  if ((options.offset < 0) || (options.limit < 0)) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid argument");
    return nullptr;
  }
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
  sorter->offset_ = options.offset;
  sorter->limit_ = options.limit;
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
  if ((offset_ >= record_set_->size()) || (limit_ <= 0)) {
    record_set_->clear();
    return true;
  }
  Int begin = offset_;
  Int end;
  if (limit_ <= (record_set_->size() - offset_)) {
    end = offset_ + limit_;
  } else {
    end = record_set_->size() - offset_;
  }
  if (record_set_->size() <= 1) {
    return true;
  }
  return head_->sort(error, record_set_->subset(), begin, end);
}

bool Sorter::sort(Error *error, RecordSet *record_set) {
  return reset(error, record_set) && finish(error);
}

Sorter::Sorter()
    : head_(),
      record_set_(nullptr),
      offset_(0),
      limit_(0) {}

}  // namespace grnxx
