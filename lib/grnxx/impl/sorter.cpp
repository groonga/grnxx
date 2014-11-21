#include "grnxx/impl/sorter.hpp"

namespace grnxx {
namespace impl {
namespace sorter {

// -- Node --

class Node {
 public:
  // Create a node for sorting records in "order".
  //
  // On success, returns the node.
  // On failure, throws an exception.
  static Node *create(SorterOrder &&order);

  explicit Node(SorterOrder &&order)
      : order_(std::move(order)),
        next_(nullptr) {}
  virtual ~Node() = default;

  // Set the next node.
  void set_next(Node *next) {
    next_ = next;
  }

  // Sort records in [begin, end).
  //
  // On success, returns true.
  // On failure, throws an exception.
  virtual void sort(ArrayRef<Record> ref, size_t begin, size_t end) = 0;

 protected:
  SorterOrder order_;
  Node *next_;
};

// --- TypedNode ---

template <typename T>
class TypedNode : public Node {
 public:
  using Value = T;

  explicit TypedNode(SorterOrder &&order)
      : Node(std::move(order)),
        values_() {}
  virtual ~TypedNode() = default;

 protected:
  Array<Value> values_;
};

// ---- SeparatorNode ----

class SeparatorNode : public TypedNode<Bool> {
 public:
  explicit SeparatorNode(SorterOrder &&order)
      : TypedNode<Bool>(std::move(order)),
        prior_value_((order.type == SORTER_REGULAR_ORDER) ?
                     Bool::false_value() : Bool::true_value()) {}
  ~SeparatorNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end);

 private:
  uint8_t prior_value_;
};

void SeparatorNode::sort(ArrayRef<Record> records, size_t begin, size_t end) {
  // Sort "records" as follows:
  // - Prior values: [0, posterior_offset)
  // - Posterior values: [posterior_offset, na_offset)
  // - N/A: [na_offset, records.size())
  order_.expression->evaluate(records, &values_);
  size_t posterior_offset = 0;
  size_t na_offset = records.size();
  for (size_t i = 0; i < na_offset; ) {
    if (values_[i].is_na()) {
      std::swap(records[i], records[na_offset - 1]);
      values_[i] = values_[na_offset - 1];
      --na_offset;
    } else {
      if (values_[i].value() == prior_value_) {
        std::swap(records[posterior_offset], records[i]);
        ++posterior_offset;
      }
      ++i;
    }
  }

  // Apply the next sort condition for blocks with 2 or more records.
  if (next_) {
    if ((posterior_offset >= 2) &&
        (posterior_offset > begin)) {
      next_->sort(records.ref(0, posterior_offset),
                  begin,
                  (end < posterior_offset) ? end : posterior_offset);
    }
    if (((na_offset - posterior_offset) >= 2) &&
        (na_offset > begin) &&
        (posterior_offset < end)) {
      next_->sort(records.ref(posterior_offset, na_offset - posterior_offset),
                  (begin < posterior_offset) ? 0 : (begin - posterior_offset),
                  ((end < na_offset) ? end : na_offset) - posterior_offset);
    }
    if (((records.size() - na_offset) >= 2) &&
        (na_offset < end)) {
      next_->sort(records.ref(na_offset),
                  (begin < na_offset) ? 0 : (begin - na_offset),
                  end - na_offset);
    }
  }
}

// ---- QuickSortNode ----

template <typename T>
struct Equal {
  bool operator()(const T &lhs, const T &rhs) const {
    return lhs.match(rhs);
  }
};

template <typename T>
class QuickSortNode : public TypedNode<typename T::Value> {
 public:
  using Comparer = T;
  using Value    = typename Comparer::Value;

  explicit QuickSortNode(SorterOrder &&order)
      : TypedNode<Value>(std::move(order)),
        comparer_() {}
  ~QuickSortNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end);

 private:
  Comparer comparer_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the sorting range becomes small enough.
  //
  // On success, returns true.
  // On failure, throws an exception.
  void quick_sort(ArrayRef<Record> records,
                  Value *values,
                  size_t begin,
                  size_t end);

  // Sort records with insertion sort.
  //
  // Insertion sort should be used when there few records.
  //
  // On success, returns true.
  // On failure, throws an exception.
  void insertion_sort(ArrayRef<Record> records, Value *values);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records, Value *values);
};

template <typename T>
void QuickSortNode<T>::sort(ArrayRef<Record> records,
                            size_t begin,
                            size_t end) {
  this->order_.expression->evaluate(records, &this->values_);
  quick_sort(records, this->values_.buffer(), begin, end);
}

template <typename T>
void QuickSortNode<T>::quick_sort(ArrayRef<Record> records,
                                  Value *values,
                                  size_t begin,
                                  size_t end) {
  // Use ternary quick sort if there are enough records.
  //
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records, values);
    const Value pivot = values[0];
    size_t left = 1;
    size_t right = records.size();
    size_t pivot_left = 1;
    size_t pivot_right = records.size();
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
          std::swap(records[left], records[pivot_left]);
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
          std::swap(records[right], records[pivot_right]);
        }
      }
      if (left >= right) {
        break;
      }
      std::swap(values[left], values[right]);
      std::swap(records[left], records[right]);
      ++left;
    }

    // Move left pivot-equivalent entries to the left side of the boundary.
    while (pivot_left > 0) {
      --pivot_left;
      --left;
      std::swap(values[pivot_left], values[left]);
      std::swap(records[pivot_left], records[left]);
    }
    // Move right pivot-equivalent entries to the right side of the boundary.
    while (pivot_right < records.size()) {
      std::swap(values[pivot_right], values[right]);
      std::swap(records[pivot_right], records[right]);
      ++pivot_right;
      ++right;
    }

    // Apply the next sort condition to the pivot-equivalent records.
    if (this->next_) {
      if (((right - left) >= 2) && (begin < right) && (end > left)) {
        size_t next_begin = (begin < left) ? 0 : (begin - left);
        size_t next_end = ((end > right) ? right : end) - left;
        this->next_->sort(records.ref(left, right - left),
                          next_begin, next_end);
      }
    }

    // There are the left group and the right group.
    // A recursive call is used for sorting the smaller group.
    // The recursion depth is less than log_2(n) where n is the number of
    // records.
    // The next loop of the current call is used for sorting the larger group.
    if (left < (records.size() - right)) {
      if ((begin < left) && (left >= 2)) {
        size_t next_end = (end < left) ? end : left;
        quick_sort(records.ref(0, left), values, begin, next_end);
      }
      if (end <= right) {
        return;
      }
      records = records.ref(right);
      values += right;
      begin = (begin < right) ? 0 : (begin - right);
      end -= right;
    } else {
      if ((end > right) && ((records.size() - right) >= 2)) {
        size_t next_begin = (begin < right) ? 0 : (begin - right);
        size_t next_end = end - right;
        quick_sort(records.ref(right),
                   values + right, next_begin, next_end);
      }
      if (begin >= left) {
        return;
      }
      records = records.ref(0, left);
      if (end > left) {
        end = left;
      }
    }
  }

  if (records.size() >= 2) {
    insertion_sort(records, values);
  }
}

template <typename T>
void QuickSortNode<T>::insertion_sort(ArrayRef<Record> records,
                                      Value *values) {
  for (size_t i = 1; i < records.size(); ++i) {
    for (size_t j = i; j > 0; --j) {
      if (comparer_(values[j], values[j - 1])) {
        std::swap(values[j], values[j - 1]);
        std::swap(records[j], records[j - 1]);
      } else {
        break;
      }
    }
  }

  // Apply the next sorting if there are records having the same value.
  if (this->next_) {
    size_t begin = 0;
    for (size_t i = 1; i < records.size(); ++i) {
      if (!Equal<Value>()(values[i], values[begin])) {
        if ((i - begin) >= 2) {
          this->next_->sort(records.ref(begin, i - begin), 0, i - begin);
        }
        begin = i;
      }
    }
    if ((records.size() - begin) >= 2) {
      this->next_->sort(records.ref(begin), 0, records.size() - begin);
    }
  }
}

template <typename T>
void QuickSortNode<T>::move_pivot_first(ArrayRef<Record> records,
                                        Value *values) {
  // Choose the median from values[1], values[1 / size], and values[size - 2].
  // The reason why not using values[0] and values[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  size_t first = 1;
  size_t middle = records.size() / 2;
  size_t last = records.size() - 2;
  if (comparer_(values[first], values[middle])) {
    // first < middle.
    if (comparer_(values[middle], values[last])) {
      // first < middle < last.
      std::swap(values[0], values[middle]);
      std::swap(records[0], records[middle]);
    } else if (comparer_(values[first], values[last])) {
      // first < last < middle.
      std::swap(values[0], values[last]);
      std::swap(records[0], records[last]);
    } else {
      // last < first < middle.
      std::swap(values[0], values[first]);
      std::swap(records[0], records[first]);
    }
  } else if (comparer_(values[last], values[middle])) {
    // last < middle < first.
    std::swap(values[0], values[middle]);
    std::swap(records[0], records[middle]);
  } else if (comparer_(values[last], values[first])) {
    // middle < last < first.
    std::swap(values[0], values[last]);
    std::swap(records[0], records[last]);
  } else {
    // middle < first < last.
    std::swap(values[0], values[first]);
    std::swap(records[0], records[first]);
  }
}

// ----- RegularComparer -----

template <typename T>
struct RegularComparer {
  using Value = T;

  // Return whether "lhs" is prior to "rhs" or not.
  bool operator()(const Value &lhs, const Value &rhs) const {
    if (lhs.is_na()) {
      return false;
    } else if (rhs.is_na()) {
      return true;
    }
    return (lhs < rhs).is_true();
  }
};

// ----- ReverseComparer -----

template <typename T>
struct ReverseComparer {
  using Value = T;

  // Return whether "lhs" is prior to "rhs" or not.
  bool operator()(const Value &lhs, const Value &rhs) const {
    if (lhs.is_na()) {
      return false;
    } else if (rhs.is_na()) {
      return true;
    }
    return (lhs > rhs).is_true();
  }
};

// -- Node --

Node *Node::create(SorterOrder &&order) try {
  switch (order.expression->data_type()) {
    case BOOL_DATA: {
      return new SeparatorNode(std::move(order));
    }
    case INT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new QuickSortNode<RegularComparer<Int>>(std::move(order));
      } else {
        return new QuickSortNode<ReverseComparer<Int>>(std::move(order));
      }
    }
    case FLOAT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new QuickSortNode<RegularComparer<Float>>(std::move(order));
      } else {
        return new QuickSortNode<ReverseComparer<Float>>(std::move(order));
      }
    }
    case TEXT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new QuickSortNode<RegularComparer<Text>>(std::move(order));
      } else {
        return new QuickSortNode<ReverseComparer<Text>>(std::move(order));
      }
    }
    default: {
      throw "Invalid data type";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

// TODO: Sorter for RowID and Score should be specialized.

// TODO: Sorter for Text should be specialized.

}  // namespace sorter

using namespace sorter;

Sorter::Sorter(Array<SorterOrder> &&orders, const SorterOptions &options)
    : SorterInterface(),
      table_(nullptr),
      nodes_(),
      records_(nullptr),
      offset_(options.offset),
      limit_(options.limit) {
  // A sorter requires one or more orders.
  // Also, expressions must be valid and associated tables must be the same.
  if (orders.size() == 0) {
    throw "No order";  // TODO
  }
  for (size_t i = 0; i < orders.size(); ++i) {
    if (!orders[i].expression) {
      throw "Missing expression";  // TODO
    }
  }
  table_ = static_cast<const Table *>(orders[0].expression->table());
  for (size_t i = 1; i < orders.size(); ++i) {
    if (orders[i].expression->table() != table_) {
      throw "Table conflict";  // TODO
    }
  }

  nodes_.resize(orders.size());
  for (size_t i = 0; i < orders.size(); ++i) {
    nodes_[i].reset(Node::create(std::move(orders[i])));
  }
  for (size_t i = 1; i < orders.size(); ++i) {
    nodes_[i - 1]->set_next(nodes_[i].get());
  }
  orders.clear();
}

Sorter::~Sorter() {}

void Sorter::reset(Array<Record> *records) {
  records_ = records;
}

void Sorter::progress() {
  // TODO: Incremental sorting is not supported yet.
}

void Sorter::finish() {
  if (!records_) {
    throw "No target";  // TODO
  }
  if ((offset_ >= records_->size()) || (limit_ <= 0)) {
    records_->clear();
    return;
  }
  size_t begin = offset_;
  size_t end;
  if (limit_ <= (records_->size() - offset_)) {
    end = offset_ + limit_;
  } else {
    end = records_->size();
  }
  if (records_->size() <= 1) {
    return;
  }
  nodes_[0]->sort(records_->ref(), begin, end);
  for (size_t i = begin, j = 0; i < end; ++i, ++j) {
    (*records_)[j] = (*records_)[i];
  }
  records_->resize(end - begin);
}

void Sorter::sort(Array<Record> *records) {
  reset(records);
  finish();
}

}  // namespace impl
}  // namespace grnxx
