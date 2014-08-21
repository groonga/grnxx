#include "grnxx/sorter.hpp"

#include <cmath>

#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"

namespace grnxx {
namespace sorter {

// -- Node --

class Node {
 public:
  static unique_ptr<Node> create(Error *error, SortOrder &&order);

  explicit Node(SortOrder &&order) : order_(std::move(order)), next_() {}
  virtual ~Node() {}

  // Set the next node.
  void set_next(unique_ptr<Node> &&next) {
    next_ = std::move(next);
  }

  // Sort records in [begin, end).
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool sort(Error *error,
                    ArrayRef<Record> ref,
                    Int begin,
                    Int end) = 0;

 protected:
  SortOrder order_;
  unique_ptr<Node> next_;
};

// --- TypedNode ---

template <typename T>
class TypedNode : public Node {
 public:
  using Value = T;

  explicit TypedNode(SortOrder &&order)
      : Node(std::move(order)),
        values_() {}
  virtual ~TypedNode() {}

 protected:
  Array<Value> values_;
};

// ---- SeparatorNode ----

template <typename T>
class SeparatorNode : public TypedNode<Bool> {
 public:
  using IsPrior = T;

  explicit SeparatorNode(SortOrder &&order)
      : TypedNode<Bool>(std::move(order)),
        is_prior_() {}
  ~SeparatorNode() {}

  bool sort(Error *error, ArrayRef<Record> records, Int begin, Int end);

 private:
  IsPrior is_prior_;
};

template <typename T>
bool SeparatorNode<T>::sort(Error *error,
                            ArrayRef<Record> records,
                            Int begin,
                            Int end) {
  if (!order_.expression->evaluate(error, records, &values_)) {
    return false;
  }

  // Move prior entries to left and others to right.
  Int left = 0;
  Int right = records.size();
  while (left < right) {
    if (is_prior_(values_[left])) {
      ++left;
    } else {
      // Note that values_[left] will not be used again.
      --right;
      values_.set(left, values_[right]);
      records.swap(left, right);
    }
  }

  // Now "left" equals to "right" and it points to the boundary.
  // The left group is [0, left) and the right group is [left, records.size()).
  if (this->next_) {
    // Apply the next sort condition if blocks contain 2 or more records.
    if ((left >= 2) && (begin < left)) {
      if (!this->next_->sort(error, records.ref(0, left),
                             begin, (end < left) ? end : left)) {
        return false;
      }
    }
    if (((records.size() - left) >= 2) && (end > left)) {
      if (begin < left) {
        begin = 0;
      } else {
        begin -= left;
      }
      end -= left;
      if (!this->next_->sort(error, records.ref(left), begin, end)) {
        return false;
      }
    }
  }

  return true;
}

// ----- RegularIsPrior -----

struct RegularIsPrior {
  bool operator()(Bool arg) const {
    return !arg;
  }
};

// ----- ReverseIsPrior -----

struct ReverseIsPrior {
  bool operator()(Bool arg) const {
    return arg;
  }
};

// ---- QuickSortNode ----

template <typename T>
struct Equal {
  bool operator()(T lhs, T rhs) const {
    return lhs == rhs;
  }
};

template <>
struct Equal<Float> {
  bool operator()(Float lhs, Float rhs) const {
    return (lhs == rhs) || (std::isnan(lhs) && std::isnan(rhs));
  }
};

template <typename T>
class QuickSortNode : public TypedNode<typename T::Value> {
 public:
  using Comparer = T;
  using Value    = typename Comparer::Value;

  explicit QuickSortNode(SortOrder &&order)
      : TypedNode<Value>(std::move(order)),
        comparer_() {}
  ~QuickSortNode() {}

  bool sort(Error *error, ArrayRef<Record> records, Int begin, Int end);

 private:
  Comparer comparer_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the sorting range becomes small enough.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool quick_sort(Error *error,
                  ArrayRef<Record> records,
                  Value *values,
                  Int begin,
                  Int end);

  // Sort records with insertion sort.
  //
  // Insertion sort should be used when there few records.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool insertion_sort(Error *error, ArrayRef<Record> records, Value *values);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records, Value *values);
};

template <typename T>
bool QuickSortNode<T>::sort(Error *error,
                            ArrayRef<Record> records,
                            Int begin,
                            Int end) {
  if (!this->order_.expression->evaluate(error, records, &this->values_)) {
    return false;
  }
  return quick_sort(error, records, this->values_.data(), begin, end);
}

template <typename T>
bool QuickSortNode<T>::quick_sort(Error *error,
                                  ArrayRef<Record> records,
                                  Value *values,
                                  Int begin,
                                  Int end) {
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
        if (comparer_(pivot, values[left])) {
          break;
        } else if (Equal<Value>()(pivot, values[left])) {
          std::swap(values[left], values[pivot_left]);
          records.swap(left, pivot_left);
          ++pivot_left;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (comparer_(values[right], pivot)) {
          break;
        } else if (Equal<Value>()(values[right], pivot)) {
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
        if (!this->next_->sort(error, records.ref(left, right - left),
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
        if (!quick_sort(error, records.ref(0, left), values,
                        begin, next_end)) {
          return false;
        }
      }
      if (end <= right) {
        return true;
      }
      records = records.ref(right);
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
        if (!quick_sort(error, records.ref(right),
                        values + right, next_begin, next_end)) {
          return false;
        }
      }
      if (begin >= left) {
        return true;
      }
      records = records.ref(0, left);
      if (end > left) {
        end = left;
      }
    }
  }

  if (records.size() >= 2) {
    return insertion_sort(error, records, values);
  }
  return true;
}

template <typename T>
bool QuickSortNode<T>::insertion_sort(Error *error,
                                      ArrayRef<Record> records,
                                      Value *values) {
  for (Int i = 1; i < records.size(); ++i) {
    for (Int j = i; j > 0; --j) {
      if (comparer_(values[j], values[j - 1])) {
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
      if (!Equal<Value>()(values[i], values[begin])) {
        if ((i - begin) >= 2) {
          if (!this->next_->sort(error, records.ref(begin, i - begin),
                                 0, i - begin)) {
            return false;
          }
        }
        begin = i;
      }
    }
    if ((records.size() - begin) >= 2) {
      if (!this->next_->sort(error, records.ref(begin),
                             0, records.size() - begin)) {
        return false;
      }
    }
  }
  return true;
}

template <typename T>
void QuickSortNode<T>::move_pivot_first(ArrayRef<Record> records,
                                        Value *values) {
  // Choose the median from values[1], values[1 / size], and values[size - 2].
  // The reason why not using values[0] and values[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  Int first = 1;
  Int middle = records.size() / 2;
  Int last = records.size() - 2;
  if (comparer_(values[first], values[middle])) {
    // first < middle.
    if (comparer_(values[middle], values[last])) {
      // first < middle < last.
      std::swap(values[0], values[middle]);
      records.swap(0, middle);
    } else if (comparer_(values[first], values[last])) {
      // first < last < middle.
      std::swap(values[0], values[last]);
      records.swap(0, last);
    } else {
      // last < first < middle.
      std::swap(values[0], values[first]);
      records.swap(0, first);
    }
  } else if (comparer_(values[last], values[middle])) {
    // last < middle < first.
    std::swap(values[0], values[middle]);
    records.swap(0, middle);
  } else if (comparer_(values[last], values[first])) {
    // middle < last < first.
    std::swap(values[0], values[last]);
    records.swap(0, last);
  } else {
    // middle < first < last.
    std::swap(values[0], values[first]);
    records.swap(0, first);
  }
}

// ----- RegularComparer -----

template <typename T>
struct RegularComparer {
  using Value = T;
  bool operator()(Value lhs, Value rhs) const {
    return lhs < rhs;
  }
};

template <>
struct RegularComparer<Float> {
  using Value = Float;
  bool operator()(Value lhs, Value rhs) const {
    // Numbers are prior to NaN.
    if (std::isnan(lhs)) {
      return false;
    } else if (std::isnan(rhs)) {
      return true;
    }
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

// ----- ReverseComparer -----

template <typename T>
struct ReverseComparer {
  using Value = T;
  bool operator()(Value lhs, Value rhs) const {
    return RegularComparer<T>()(rhs, lhs);
  }
};

}  // namespace sorter

using namespace sorter;

unique_ptr<Node> Node::create(Error *error, SortOrder &&order) {
  unique_ptr<Node> node;
  switch (order.expression->data_type()) {
    case BOOL_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) SeparatorNode<RegularIsPrior>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) SeparatorNode<ReverseIsPrior>(
            std::move(order)));
      }
      break;
    }
    case INT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) QuickSortNode<RegularComparer<Int>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) QuickSortNode<ReverseComparer<Int>>(
            std::move(order)));
      }
      break;
    }
    case FLOAT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) QuickSortNode<RegularComparer<Float>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) QuickSortNode<ReverseComparer<Float>>(
            std::move(order)));
      }
      break;
    }
    case TIME_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) QuickSortNode<RegularComparer<Time>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) QuickSortNode<ReverseComparer<Time>>(
            std::move(order)));
      }
      break;
    }
    case GEO_POINT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) QuickSortNode<RegularComparer<GeoPoint>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) QuickSortNode<ReverseComparer<GeoPoint>>(
            std::move(order)));
      }
      break;
    }
    case TEXT_DATA: {
      if (order.type == REGULAR_ORDER) {
        node.reset(new (nothrow) QuickSortNode<RegularComparer<Text>>(
            std::move(order)));
      } else {
        node.reset(new (nothrow) QuickSortNode<ReverseComparer<Text>>(
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
    Array<SortOrder> &&orders,
    const SorterOptions &options) {
  // Sorting require at least one sort order.
  // Also, expressions must be valid and associated tables must be same.
  if (orders.size() == 0) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Empty sort order");
    return nullptr;
  }
  for (Int i = 0; i < orders.size(); ++i) {
    if (!orders[i].expression) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Empty sort order");
      return nullptr;
    }
  }
  const Table *table = orders[0].expression->table();
  for (Int i = 1; i < orders.size(); ++i) {
    if (orders[i].expression->table() != table) {
      GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Table conflict");
      return nullptr;
    }
  }

  if ((options.offset < 0) || (options.limit < 0)) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid argument");
    return nullptr;
  }
  unique_ptr<Sorter> sorter(new (nothrow) Sorter);
  if (!sorter) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  sorter->table_ = table;
  for (Int i = orders.size() - 1; i >= 0; --i) {
    unique_ptr<Node> node(Node::create(error, std::move(orders[i])));
    if (!node) {
      return nullptr;
    }
    node->set_next(std::move(sorter->head_));
    sorter->head_ = std::move(node);
  }
  orders.clear();
  sorter->offset_ = options.offset;
  sorter->limit_ = options.limit;
  return sorter;
}

bool Sorter::reset(Error *error, Array<Record> *records) {
  records_ = records;
  return true;
}

bool Sorter::progress(Error *error) {
  // TODO: Incremental sorting is not supported yet.
  return true;
}

bool Sorter::finish(Error *error) {
  if (!records_) {
    // Nothing to do.
    return true;
  }
  if ((offset_ >= records_->size()) || (limit_ <= 0)) {
    records_->clear();
    return true;
  }
  Int begin = offset_;
  Int end;
  if (limit_ <= (records_->size() - offset_)) {
    end = offset_ + limit_;
  } else {
    end = records_->size();
  }
  if (records_->size() <= 1) {
    return true;
  }
  if (!head_->sort(error, records_->ref(), begin, end)) {
    return false;
  }
  for (Int i = begin, j = 0; i < end; ++i, ++j) {
    records_->set(j, records_->get(i));
  }
  records_->resize(nullptr, end - begin);
  return true;
}

bool Sorter::sort(Error *error, Array<Record> *records) {
  return reset(error, records) && finish(error);
}

Sorter::Sorter()
    : table_(nullptr),
      head_(),
      records_(nullptr),
      offset_(0),
      limit_(0) {}

}  // namespace grnxx
