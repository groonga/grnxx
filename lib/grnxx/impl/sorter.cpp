#include "grnxx/impl/sorter.hpp"

namespace grnxx {
namespace impl {
namespace sorter {

// -- Node --

class Node {
 public:
  explicit Node(SorterOrder &&order)
      : order_(std::move(order)),
        next_(nullptr) {}
  virtual ~Node() = default;

  // Set the next node.
  void set_next(Node *next) {
    next_ = next;
  }

  // Sort new records.
  virtual void progress(Array<Record> *records,
                        size_t offset,
                        size_t limit,
                        size_t progress);

  // Sort records in [begin, end).
  //
  // On failure, throws an exception.
  virtual void sort(ArrayRef<Record> ref, size_t begin, size_t end) = 0;

 protected:
  SorterOrder order_;
  Node *next_;
};

void Node::progress(Array<Record> *, size_t, size_t, size_t) {
  // Not supported.
}

// --- RowIDNode ---

// NOTE: The following implementation assumes that there are no duplicates.

struct RegularRowIDComparer {
  bool operator()(const Record &lhs, const Record &rhs) const {
    return lhs.row_id.raw() < rhs.row_id.raw();
  }
};

struct ReverseRowIDComparer {
  bool operator()(const Record &lhs, const Record &rhs) const {
    return lhs.row_id.raw() > rhs.row_id.raw();
  }
};

template <typename T>
class RowIDNode : public Node {
 public:
  using Comparer = T;

  explicit RowIDNode(SorterOrder &&order)
      : Node(std::move(order)),
        comparer_() {}
  ~RowIDNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end) {
    return quick_sort(records, begin, end);
  }

 private:
  Comparer comparer_;

  // Sort records with quick sort.
  //
  // Switches to insertion sort when there are few records.
  //
  // On failure, throws an exception.
  void quick_sort(ArrayRef<Record> records, size_t begin, size_t end);

  // Sort records with insertion sort.
  //
  // On failure, throws an exception.
  void insertion_sort(ArrayRef<Record> records);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records);
};

template <typename T>
void RowIDNode<T>::quick_sort(ArrayRef<Record> records,
                              size_t begin,
                              size_t end) {
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records);
    const Record pivot = records[0];
    size_t left = 1;
    size_t right = records.size();
    for ( ; ; ) {
      // Move prior records to left.
      while (left < right) {
        if (comparer_(pivot, records[left])) {
          break;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (comparer_(records[right], pivot)) {
          break;
        }
      }
      if (left >= right) {
        break;
      }
      std::swap(records[left], records[right]);
      ++left;
    }

    // Move the pivot to the boundary.
    --left;
    std::swap(records[0], records[left]);

    // There are the left group and the right group.
    // A recursive call is used for sorting the smaller group.
    // The recursion depth is less than log_2(n) where n is the number of
    // records.
    // The next loop of the current call is used for sorting the larger group.
    if (left < (records.size() - right)) {
      if ((begin < left) && (left >= 2)) {
        size_t next_end = (end < left) ? end : left;
        quick_sort(records.ref(0, left), begin, next_end);
      }
      if (end <= right) {
        return;
      }
      records = records.ref(right);
      begin = (begin < right) ? 0 : (begin - right);
      end -= right;
    } else {
      if ((end > right) && ((records.size() - right) >= 2)) {
        size_t next_begin = (begin < right) ? 0 : (begin - right);
        size_t next_end = end - right;
        quick_sort(records.ref(right), next_begin, next_end);
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
    insertion_sort(records);
  }
}

template <typename T>
void RowIDNode<T>::insertion_sort(ArrayRef<Record> records) {
  for (size_t i = 1; i < records.size(); ++i) {
    for (size_t j = i; j > 0; --j) {
      if (comparer_(records[j], records[j - 1])) {
        std::swap(records[j], records[j - 1]);
      } else {
        break;
      }
    }
  }
}

template <typename T>
void RowIDNode<T>::move_pivot_first(ArrayRef<Record> records) {
  // Choose the median from records[1], records[1 / size], and
  // records[size - 2].
  // The reason why not using records[0] and records[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  size_t first = 1;
  size_t middle = records.size() / 2;
  size_t last = records.size() - 2;
  if (comparer_(records[first], records[middle])) {
    // first < middle.
    if (comparer_(records[middle], records[last])) {
      // first < middle < last.
      std::swap(records[0], records[middle]);
    } else if (comparer_(records[first], records[last])) {
      // first < last < middle.
      std::swap(records[0], records[last]);
    } else {
      // last < first < middle.
      std::swap(records[0], records[first]);
    }
  } else if (comparer_(records[last], records[middle])) {
    // last < middle < first.
    std::swap(records[0], records[middle]);
  } else if (comparer_(records[last], records[first])) {
    // middle < last < first.
    std::swap(records[0], records[last]);
  } else {
    // middle < first < last.
    std::swap(records[0], records[first]);
  }
}

// --- ScoreNode ---

struct RegularScoreComparer {
  bool operator()(const Record &lhs, const Record &rhs) const {
    if (lhs.score.is_na()) {
      return false;
    } else if (rhs.score.is_na()) {
      return true;
    }
    return lhs.score.raw() < rhs.score.raw();
  }
};

struct ReverseScoreComparer {
  bool operator()(const Record &lhs, const Record &rhs) const {
    if (lhs.score.is_na()) {
      return false;
    } else if (rhs.score.is_na()) {
      return true;
    }
    return lhs.score.raw() > rhs.score.raw();
  }
};

template <typename T>
class ScoreNode : public Node {
 public:
  using Comparer = T;

  explicit ScoreNode(SorterOrder &&order)
      : Node(std::move(order)),
        comparer_() {}
  ~ScoreNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end) {
    return quick_sort(records, begin, end);
  }

 private:
  Comparer comparer_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the sorting range becomes small enough.
  //
  // On failure, throws an exception.
  void quick_sort(ArrayRef<Record> records, size_t begin, size_t end);

  // Sort records with insertion sort.
  //
  // Insertion sort should be used when there few records.
  //
  // On failure, throws an exception.
  void insertion_sort(ArrayRef<Record> records);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records);
};

template <typename T>
void ScoreNode<T>::quick_sort(ArrayRef<Record> records,
                              size_t begin,
                              size_t end) {
  // Use ternary quick sort if there are enough records.
  //
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records);
    const Record pivot = records[0];
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
        if (comparer_(pivot, records[left])) {
          break;
        } else if (pivot.score.match(records[left].score)) {
          std::swap(records[left], records[pivot_left]);
          ++pivot_left;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (comparer_(records[right], pivot)) {
          break;
        } else if (records[right].score.match(pivot.score)) {
          --pivot_right;
          std::swap(records[right], records[pivot_right]);
        }
      }
      if (left >= right) {
        break;
      }
      std::swap(records[left], records[right]);
      ++left;
    }

    // Move left pivot-equivalent entries to the left side of the boundary.
    while (pivot_left > 0) {
      --pivot_left;
      --left;
      std::swap(records[pivot_left], records[left]);
    }
    // Move right pivot-equivalent entries to the right side of the boundary.
    while (pivot_right < records.size()) {
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
        quick_sort(records.ref(0, left), begin, next_end);
      }
      if (end <= right) {
        return;
      }
      records = records.ref(right);
      begin = (begin < right) ? 0 : (begin - right);
      end -= right;
    } else {
      if ((end > right) && ((records.size() - right) >= 2)) {
        size_t next_begin = (begin < right) ? 0 : (begin - right);
        size_t next_end = end - right;
        quick_sort(records.ref(right), next_begin, next_end);
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
    insertion_sort(records);
  }
}

template <typename T>
void ScoreNode<T>::insertion_sort(ArrayRef<Record> records) {
  for (size_t i = 1; i < records.size(); ++i) {
    for (size_t j = i; j > 0; --j) {
      if (comparer_(records[j], records[j - 1])) {
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
      if (records[i].score.unmatch(records[begin].score)) {
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
void ScoreNode<T>::move_pivot_first(ArrayRef<Record> records) {
  // Choose the median from records[1], records[1 / size], and
  // records[size - 2].
  // The reason why not using records[0] and records[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  size_t first = 1;
  size_t middle = records.size() / 2;
  size_t last = records.size() - 2;
  if (comparer_(records[first], records[middle])) {
    // first < middle.
    if (comparer_(records[middle], records[last])) {
      // first < middle < last.
      std::swap(records[0], records[middle]);
    } else if (comparer_(records[first], records[last])) {
      // first < last < middle.
      std::swap(records[0], records[last]);
    } else {
      // last < first < middle.
      std::swap(records[0], records[first]);
    }
  } else if (comparer_(records[last], records[middle])) {
    // last < middle < first.
    std::swap(records[0], records[middle]);
  } else if (comparer_(records[last], records[first])) {
    // middle < last < first.
    std::swap(records[0], records[last]);
  } else {
    // middle < first < last.
    std::swap(records[0], records[first]);
  }
}

// --- BoolNode ---

class BoolNode : public Node {
 public:
  explicit BoolNode(SorterOrder &&order)
      : Node(std::move(order)),
        prior_raw_((order.type == SORTER_REGULAR_ORDER) ?
                   Bool::raw_false() : Bool::raw_true()),
        values_() {}
  virtual ~BoolNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end);

 private:
  uint8_t prior_raw_;
  Array<Bool> values_;
};

void BoolNode::sort(ArrayRef<Record> records, size_t begin, size_t end) {
  // Sort "records" as follows:
  // - Prior values: [0, posterior_offset)
  // - Posterior values: [posterior_offset, na_offset)
  // - N/A: [na_offset, records.size())
  order_.expression->evaluate(records, &values_);
  size_t posterior_offset = records.size();
  size_t na_offset = records.size();
  for (size_t i = 0; i < posterior_offset; ) {
    while (i < posterior_offset) {
      if (values_[i].is_na()) {
        Record temp = records[i];
        records[i] = records[posterior_offset - 1];
        values_[i] = values_[posterior_offset - 1];
        records[posterior_offset - 1] = records[na_offset - 1];
        records[na_offset - 1] = temp;
        --posterior_offset;
        --na_offset;
      } else if (values_[i].raw() == prior_raw_) {
        ++i;
      } else {
        break;
      }
    }
    while (i < posterior_offset) {
      if (values_[posterior_offset - 1].is_na()) {
        --posterior_offset;
        --na_offset;
        std::swap(records[posterior_offset], records[na_offset]);
      } else if (values_[posterior_offset - 1].raw() == prior_raw_) {
        break;
      } else {
        --posterior_offset;
      }
    }
    if (i < posterior_offset) {
      --posterior_offset;
      std::swap(records[i], records[posterior_offset]);
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

// --- ConvertNode ---

template <typename T, typename U>
class ConvertNode : public Node {
 public:
  using Value     = T;
  using Converter = U;

  explicit ConvertNode(SorterOrder &&order)
      : Node(std::move(order)),
        converter_(),
        values_(),
        internal_values_() {}
  ~ConvertNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end);

 private:
  Converter converter_;
  Array<Value> values_;
  Array<uint64_t> internal_values_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the sorting range becomes small enough.
  //
  // On failure, throws an exception.
  void quick_sort(ArrayRef<Record> records,
                  uint64_t *values,
                  size_t begin,
                  size_t end);

  // Sort records with insertion sort.
  //
  // Insertion sort should be used when there few records.
  //
  // On failure, throws an exception.
  void insertion_sort(ArrayRef<Record> records, uint64_t *values);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records, uint64_t *values);
};

template <typename T, typename U>
void ConvertNode<T, U>::sort(ArrayRef<Record> records,
                             size_t begin,
                             size_t end) {
  // TODO: A magic number (1024) should not be used.
  if (internal_values_.size() < records.size()) {
    internal_values_.resize(records.size());
  }
  values_.resize(1024);
  size_t offset = 0;
  while (offset < records.size()) {
    size_t block_size = records.size() - offset;
    if (block_size > values_.size()) {
      block_size = values_.size();
    }
    this->order_.expression->evaluate(
        records.cref(offset, block_size), &values_);
    for (size_t i = 0; i < block_size; ++i) {
      internal_values_[offset + i] = converter_(values_[i]);
    }
    offset += block_size;
  }
  quick_sort(records, internal_values_.buffer(), begin, end);
}

template <typename T, typename U>
void ConvertNode<T, U>::quick_sort(ArrayRef<Record> records,
                                   uint64_t *values,
                                   size_t begin,
                                   size_t end) {
  // Use ternary quick sort if there are enough records.
  //
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records, values);
    const uint64_t pivot = values[0];
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
        if (pivot < values[left]) {
          break;
        } else if (pivot == values[left]) {
          std::swap(values[left], values[pivot_left]);
          std::swap(records[left], records[pivot_left]);
          ++pivot_left;
        }
        ++left;
      }
      while (left < right) {
        --right;
        if (values[right] < pivot) {
          break;
        } else if (values[right] == pivot) {
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
        quick_sort(records.ref(right), values + right, next_begin, next_end);
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

template <typename T, typename U>
void ConvertNode<T, U>::insertion_sort(ArrayRef<Record> records,
                                       uint64_t *values) {
  for (size_t i = 1; i < records.size(); ++i) {
    for (size_t j = i; j > 0; --j) {
      if (values[j] < values[j - 1]) {
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
      if (values[i] != values[begin]) {
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

template <typename T, typename U>
void ConvertNode<T, U>::move_pivot_first(ArrayRef<Record> records,
                                         uint64_t *values) {
  // Choose the median from values[1], values[1 / size], and values[size - 2].
  // The reason why not using values[0] and values[size - 1] is to avoid the
  // worst case which occurs when the records are sorted in reverse order.
  size_t first = 1;
  size_t middle = records.size() / 2;
  size_t last = records.size() - 2;
  if (values[first] < values[middle]) {
    // first < middle.
    if (values[middle] < values[last]) {
      // first < middle < last.
      std::swap(values[0], values[middle]);
      std::swap(records[0], records[middle]);
    } else if (values[first] < values[last]) {
      // first < last < middle.
      std::swap(values[0], values[last]);
      std::swap(records[0], records[last]);
    } else {
      // last < first < middle.
      std::swap(values[0], values[first]);
      std::swap(records[0], records[first]);
    }
  } else if (values[last] < values[middle]) {
    // last < middle < first.
    std::swap(values[0], values[middle]);
    std::swap(records[0], records[middle]);
  } else if (values[last] < values[first]) {
    // middle < last < first.
    std::swap(values[0], values[last]);
    std::swap(records[0], records[last]);
  } else {
    // middle < first < last.
    std::swap(values[0], values[first]);
    std::swap(records[0], records[first]);
  }
}

// --- IntNode ---

// TODO: Sorter for RowID should be specialized.

struct RegularIntConverter {
  uint64_t operator()(Int value) const {
    return static_cast<uint64_t>(value.raw()) + (~uint64_t(0) >> 1);
  }
};

struct ReverseIntConverter {
  uint64_t operator()(Int value) const {
    return (~uint64_t(0) >> 1) - static_cast<uint64_t>(value.raw());
  }
};

template <typename T>
using IntNode = ConvertNode<Int, T>;

// --- FloatNode ---

// NOTE: This implementation assumes IEEE754.
struct RegularFloatConverter {
  uint64_t operator()(Float value) const {
    if (value.is_na()) {
      return ~uint64_t(0);
    } else {
      double raw = value.raw();
      if (raw == 0.0) {
        raw = 0.0;
      }
      int64_t temp;
      std::memcpy(&temp, &raw, sizeof(int64_t));
      return temp ^ ((temp >> 63) | (uint64_t(1) << 63));
    }
  }
};

// NOTE: This implementation assumes IEEE754.
struct ReverseFloatConverter {
  uint64_t operator()(Float value) const {
    if (value.is_na()) {
      return ~uint64_t(0);
    } else {
      double raw = value.raw();
      if (raw == 0.0) {
        raw = 0.0;
      }
      int64_t temp;
      std::memcpy(&temp, &raw, sizeof(int64_t));
      return temp ^ (((temp ^ (int64_t(1) << 63)) >> 63) &
                     (~uint64_t(0) >> 1));
    }
  }
};

template <typename T>
using FloatNode = ConvertNode<Float, T>;

// --- TextNode ---

// TODO: Sorter for Text should be specialized.

struct RegularTextComparer {
  // Return whether "lhs" is prior to "rhs" or not.
  bool operator()(const Text &lhs, const Text &rhs) const {
    if (lhs.is_na()) {
      return false;
    } else if (rhs.is_na()) {
      return true;
    }
    return (lhs < rhs).is_true();
  }
};

struct ReverseTextComparer {
  // Return whether "lhs" is prior to "rhs" or not.
  bool operator()(const Text &lhs, const Text &rhs) const {
    if (lhs.is_na()) {
      return false;
    } else if (rhs.is_na()) {
      return true;
    }
    return (lhs > rhs).is_true();
  }
};

template <typename T>
class TextNode : public Node {
 public:
  explicit TextNode(SorterOrder &&order)
      : Node(std::move(order)),
        comparer_(),
        values_() {}
  ~TextNode() = default;

  void sort(ArrayRef<Record> records, size_t begin, size_t end);

 private:
  T comparer_;
  Array<Text> values_;

  // Sort records with ternary quick sort.
  //
  // Switches to insertion sort when the there are few records.
  //
  // On failure, throws an exception.
  void quick_sort(ArrayRef<Record> records,
                  Text *values,
                  size_t begin,
                  size_t end);

  // Sort records with insertion sort.
  //
  // On failure, throws an exception.
  void insertion_sort(ArrayRef<Record> records, Text *values);

  // Choose the pivot and move it to the front.
  void move_pivot_first(ArrayRef<Record> records, Text *values);
};

template <typename T>
void TextNode<T>::sort(ArrayRef<Record> records, size_t begin, size_t end) {
  this->order_.expression->evaluate(records, &this->values_);
  quick_sort(records, this->values_.buffer(), begin, end);
}

template <typename T>
void TextNode<T>::quick_sort(ArrayRef<Record> records,
                             Text *values,
                             size_t begin,
                             size_t end) {
  // Use ternary quick sort if there are enough records.
  //
  // TODO: Currently, the threshold is 16.
  //       This value should be optimized and replaced with a named constant.
  while (records.size() >= 16) {
    move_pivot_first(records, values);
    const Text pivot = values[0];
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
        } else if (pivot.match(values[left])) {
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
        } else if (values[right].match(pivot)) {
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
        quick_sort(records.ref(right), values + right, next_begin, next_end);
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
void TextNode<T>::insertion_sort(ArrayRef<Record> records, Text *values) {
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
      if (values[i].unmatch(values[begin])) {
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
void TextNode<T>::move_pivot_first(ArrayRef<Record> records, Text *values) {
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

// --- RowIDNodeS ---

template <typename T>
class RowIDNodeS : public Node {
 public:
  using Comparer = T;

  explicit RowIDNodeS(SorterOrder &&order)
      : Node(std::move(order)),
        comparer_() {}
  ~RowIDNodeS() = default;

  void progress(Array<Record> *records,
                size_t offset,
                size_t limit,
                size_t progress);

  void sort(ArrayRef<Record> ref, size_t begin, size_t end);

 private:
  Comparer comparer_;
};

template <typename T>
void RowIDNodeS<T>::progress(Array<Record> *records,
                             size_t offset,
                             size_t limit,
                             size_t progress) {
  ArrayRef<Record> ref = records->ref();
  size_t boundary = offset + limit;
  if (progress < boundary) {
    size_t end = (boundary < ref.size()) ? boundary : ref.size();
    for (size_t i = progress; i < end; ++i) {
      for (size_t j = i; j != 0; ) {
        size_t parent = (j - 1) / 2;
        if (comparer_(ref[j], ref[parent])) {
          break;
        }
        std::swap(ref[parent], ref[j]);
        j = parent;
      }
    }
    progress = end;
  }
  for (size_t i = boundary; i < ref.size(); ++i) {
    if (comparer_(ref[i], ref[0])) {
      std::swap(ref[0], ref[i]);
      size_t parent = 0;
      size_t inprior = 0;
      for ( ; ; ) {
        size_t left = (parent * 2) + 1;
        size_t right = left + 1;
        if (left >= boundary) {
          break;
        }
        if (comparer_(ref[parent], ref[left])) {
          inprior = left;
        }
        if ((right < boundary) && comparer_(ref[inprior], ref[right])) {
          inprior = right;
        }
        if (inprior == parent) {
          break;
        }
        std::swap(ref[inprior], ref[parent]);
        parent = inprior;
      }
    }
  }
  if (records->size() > boundary) {
    records->resize(boundary);
  }
}

template <typename T>
void RowIDNodeS<T>::sort(ArrayRef<Record> ref, size_t begin, size_t end) {
  for (size_t i = end; i > begin; ) {
    --i;
    std::swap(ref[0], ref[i]);
    size_t parent = 0;
    size_t inprior = 0;
    for ( ; ; ) {
      size_t left = (parent * 2) + 1;
      size_t right = left + 1;
      if (left >= i) {
        break;
      }
      if (comparer_(ref[parent], ref[left])) {
        inprior = left;
      }
      if ((right < i) && comparer_(ref[inprior], ref[right])) {
        inprior = right;
      }
      if (inprior == parent) {
        break;
      }
      std::swap(ref[inprior], ref[parent]);
      parent = inprior;
    }
  }
}

}  // namespace sorter

using namespace sorter;

Sorter::Sorter(Array<SorterOrder> &&orders, const SorterOptions &options)
    : SorterInterface(),
      table_(nullptr),
      nodes_(),
      records_(nullptr),
      offset_(options.offset),
      limit_(options.limit),
      progress_(0) {
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

  if (limit_ > (std::numeric_limits<size_t>::max() - offset_)) {
    limit_ = std::numeric_limits<size_t>::max() - offset_;
  }

  for (size_t i = 0; i < orders.size(); ++i) {
    nodes_.push_back(std::unique_ptr<Node>(create_node(std::move(orders[i]))));
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
  if (!records_) {
    throw "No target";  // TODO
  }
  nodes_[0]->progress(records_, offset_, limit_, progress_);
  progress_ = records_->size();
}

void Sorter::finish() {
  if (!records_) {
    throw "No target";  // TODO
  }
  nodes_[0]->progress(records_, offset_, limit_, progress_);
  progress_ = records_->size();
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

Node *Sorter::create_node(SorterOrder &&order) try {
  if (order.expression->is_row_id()) {
    if (nodes_.is_empty() && ((offset_ + limit_) < 1000)) {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new RowIDNodeS<RegularRowIDComparer>(std::move(order));
      } else {
        return new RowIDNodeS<ReverseRowIDComparer>(std::move(order));
      }
    } else {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new RowIDNode<RegularRowIDComparer>(std::move(order));
      } else {
        return new RowIDNode<ReverseRowIDComparer>(std::move(order));
      }
    }
  } else if (order.expression->is_score()) {
    // NOTE: Specialization for Score is disabled because the implementation
    //       showed poor performance.
//    if (order.type == SORTER_REGULAR_ORDER) {
//      return new ScoreNode<RegularScoreComparer>(std::move(order));
//    } else {
//      return new ScoreNode<ReverseScoreComparer>(std::move(order));
//    }
  }

  switch (order.expression->data_type()) {
    case BOOL_DATA: {
      return new BoolNode(std::move(order));
    }
    case INT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new IntNode<RegularIntConverter>(std::move(order));
      } else {
        return new IntNode<ReverseIntConverter>(std::move(order));
      }
    }
    case FLOAT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new FloatNode<RegularFloatConverter>(std::move(order));
      } else {
        return new FloatNode<ReverseFloatConverter>(std::move(order));
      }
    }
    case TEXT_DATA: {
      if (order.type == SORTER_REGULAR_ORDER) {
        return new TextNode<RegularTextComparer>(std::move(order));
      } else {
        return new TextNode<ReverseTextComparer>(std::move(order));
      }
    }
    default: {
      throw "Invalid data type";  // TODO
    }
  }
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace impl
}  // namespace grnxx
