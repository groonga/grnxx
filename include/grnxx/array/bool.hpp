#ifndef GRNXX_ARRAY_BOOL_HPP
#define GRNXX_ARRAY_BOOL_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// ArrayCRef<Bool> is specialized because each Bool value does not have its own
// unique address and thus a pointer type for Bool is not available.
template <>
class ArrayCRef<Bool> {
 public:
  using Value = Bool;
  using Block = uint64_t;

  ArrayCRef() = default;
  ArrayCRef(const ArrayCRef &) = default;

  ArrayCRef &operator=(const ArrayCRef &) = default;

  bool operator==(ArrayCRef<Value> rhs) const {
    return (blocks_ == rhs.blocks_) &&
           (offset_ == rhs.offset_) &&
           (size_ == rhs.size_);
  }
  bool operator!=(ArrayCRef<Value> rhs) const {
    return (blocks_ != rhs.blocks_) ||
           (offset_ != rhs.offset_) ||
           (size_ != rhs.size_);
  }

  ArrayCRef ref(Int offset = 0) const {
    return ArrayCRef(blocks_, offset + offset_, size_ - offset);
  }
  ArrayCRef ref(Int offset, Int size) const {
    return ArrayCRef(blocks_, offset + offset_, size);
  }

  Value get(Int i) const {
    i += offset_;
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }

  Value operator[](Int i) const {
    i += offset_;
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }

  Block get_block(Int i) const {
    return blocks_[i];
  }

  Int offset() const {
    return offset_;
  }
  Int size() const {
    return size_;
  }

 private:
  const Block *blocks_;
  Int offset_;
  Int size_;

  ArrayCRef(const Block *blocks, Int offset, Int size)
      : blocks_(blocks + (offset / 64)),
        offset_(offset % 64),
        size_(size) {}

  friend class ArrayRef<Value>;
  friend class Array<Value>;
};

// ArrayRef<Bool> is specialized because each Bool value does not have its own
// unique address and thus a pointer type for Bool is not available.
template <>
class ArrayRef<Bool> {
 public:
  using Value = Bool;
  using Block = uint64_t;

  ArrayRef() = default;
  ArrayRef(const ArrayRef &) = default;

  ArrayRef &operator=(const ArrayRef &) = default;

  bool operator==(ArrayCRef<Value> rhs) const {
    return (blocks_ == rhs.blocks_) &&
           (offset_ == rhs.offset_) &&
           (size_ == rhs.size_);
  }
  bool operator==(ArrayRef<Value> rhs) const {
    return (blocks_ == rhs.blocks_) &&
           (offset_ == rhs.offset_) &&
           (size_ == rhs.size_);
  }

  bool operator!=(ArrayCRef<Value> rhs) const {
    return (blocks_ != rhs.blocks_) ||
           (offset_ != rhs.offset_) ||
           (size_ != rhs.size_);
  }
  bool operator!=(ArrayRef<Value> rhs) const {
    return (blocks_ == rhs.blocks_) ||
           (offset_ == rhs.offset_) ||
           (size_ == rhs.size_);
  }

  operator ArrayCRef<Value>() const {
    return ref();
  }

  ArrayCRef<Value> ref(Int offset = 0) const {
    return ArrayCRef<Value>(blocks_, offset + offset_, size_ - offset);
  }
  ArrayCRef<Value> ref(Int offset, Int size) const {
    return ArrayCRef<Value>(blocks_, offset + offset_, size);
  }

  ArrayRef ref(Int offset = 0) {
    return ArrayRef(blocks_, offset + offset_, size_ - offset);
  }
  ArrayRef ref(Int offset, Int size) {
    return ArrayRef(blocks_, offset + offset_, size);
  }

  Value get(Int i) const {
    i += offset_;
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }
  void set(Int i, Value value) {
    i += offset_;
    if (value) {
      blocks_[i / 64] |= Block(1) << (i % 64);
    } else {
      blocks_[i / 64] &= ~(Block(1) << (i % 64));
    }
  }

  Value operator[](Int i) const {
    i += offset_;
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }

  Block get_block(Int i) const {
    return blocks_[i];
  }
  void set_block(Int i, Block block) {
    blocks_[i] = block;
  }

  Int offset() const {
    return offset_;
  }
  Int size() const {
    return size_;
  }

  void swap(Int i, Int j) {
    Value temp = get(i);
    set(i, get(j));
    set(j, temp);
  }

 private:
  Block *blocks_;
  Int offset_;
  Int size_;

  ArrayRef(Block *blocks, Int offset, Int size)
      : blocks_(blocks + (offset / 64)),
        offset_(offset % 64),
        size_(size) {}

  friend class Array<Value>;
};

// Array<Bool> is specialized because each Bool value does not have its own
// unique address and thus a pointer type for Bool is not available.
template <>
class Array<Bool> {
 public:
  using Value = Bool;
  using Block = uint64_t;

  Array() : blocks_(), size_(0), capacity_(0) {}
  ~Array() {}

  Array(Array &&array)
      : blocks_(std::move(array.blocks_)),
        size_(array.size_),
        capacity_(array.capacity_) {
    array.size_ = 0;
    array.capacity_ = 0;
  }

  Array &operator=(Array &&array) {
    blocks_ = std::move(array.blocks_);
    size_ = array.size_;
    capacity_ = array.capacity_;
    array.size_ = 0;
    array.capacity_ = 0;
    return *this;
  }

  operator ArrayCRef<Value>() const {
    return ref();
  }

  ArrayCRef<Value> ref(Int offset = 0) const {
    return ArrayCRef<Value>(blocks_.get(), offset, size_ - offset);
  }
  ArrayCRef<Value> ref(Int offset, Int size) const {
    return ArrayCRef<Value>(blocks_.get(), offset, size);
  }

  ArrayRef<Value> ref(Int offset = 0) {
    return ArrayRef<Value>(blocks_.get(), offset, size_ - offset);
  }
  ArrayRef<Value> ref(Int offset, Int size) {
    return ArrayRef<Value>(blocks_.get(), offset, size);
  }

  Value get(Int i) const {
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }
  void set(Int i, Value value) {
    if (value) {
      blocks_[i / 64] |= Block(1) << (i % 64);
    } else {
      blocks_[i / 64] &= ~(Block(1) << (i % 64));
    }
  }

  Value operator[](Int i) const {
    return (blocks_[i / 64] & (Block(1) << (i % 64))) != 0;
  }

  Block get_block(Int i) const {
    return blocks_[i];
  }
  void set_block(Int i, Block block) {
    blocks_[i] = block;
  }

  Value front() const {
    return (blocks_[0] & 1) != 0;
  }
  Value back() const {
    return get(size_ - 1);
  }

  Int size() const {
    return size_;
  }
  Int capacity() const {
    return capacity_;
  }

  bool reserve(Error *error, Int new_size) {
    if (new_size <= capacity_) {
      return true;
    }
    return resize_blocks(error, new_size);
  }

  bool resize(Error *error, Int new_size) {
    if (new_size <= capacity_) {
      size_ = new_size;
      return true;
    }
    if (!resize_blocks(error, new_size)) {
      return false;
    }
    size_ = new_size;
    return true;
  }
  bool resize(Error *error, Int new_size, Value value) {
    if (new_size <= capacity_) {
      size_ = new_size;
      return true;
    }
    if (!resize_blocks(error, new_size)) {
      return false;
    }
    if ((size_ % 64) != 0) {
      if (value) {
        blocks_[size_ / 64] |= ~Block(0) << (size_ % 64);
      } else {
        blocks_[size_ / 64] &= ~(~Block(0) << (size_ % 64));
      }
      size_ = (size_ + 63) & ~Int(63);
    }
    Block block = value ? ~Block(0) : Block(0);
    for (Int i = size_; i < new_size; i += 64) {
      blocks_[i / 64] = block;
    }
    size_ = new_size;
    return true;
  }

  void clear() {
    size_ = 0;
  }

  bool push_back(Error *error, Value value) {
    if (size_ == capacity_) {
      if (!resize_blocks(error, size_ + 1)) {
        return false;
      }
    }
    if (value) {
      blocks_[size_ / 64] |= Block(1) << (size_ % 64);
    } else {
      blocks_[size_ / 64] &= ~(Block(1) << (size_ % 64));
    }
    ++size_;
    return true;
  }
  void pop_back() {
    --size_;
  }

  void swap(Int i, Int j) {
    Value temp = get(i);
    set(i, get(j));
    set(j, temp);
  }

 private:
  unique_ptr<Block[]> blocks_;
  Int size_;
  Int capacity_;

  // Assume new_size > capacity_.
  bool resize_blocks(Error *error, Int new_size);
};

}  // namespace grnxx

#endif  // GRNXX_ARRAY_BOOL_HPP
