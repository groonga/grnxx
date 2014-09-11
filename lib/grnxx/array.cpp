#include "grnxx/types.hpp"

namespace grnxx {

void ArrayErrorReporter::report_memory_error(Error *error) {
  GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
}

bool Array<Bool>::resize_blocks(Error *error, Int new_size) {
  Int new_capacity = capacity_ * 2;
  if (new_size > new_capacity) {
    new_capacity = (new_size + 63) & ~Int(63);
  }
  unique_ptr<Block[]> new_blocks(new (nothrow) Block[new_capacity / 64]);
  if (!new_blocks) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  Int num_valid_blocks = (size_ + 63) / 64;
  for (Int i = 0; i < num_valid_blocks; ++i) {
    new_blocks[i] = blocks_[i];
  }
  blocks_ = std::move(new_blocks);
  capacity_ = new_capacity;
  return true;
}

bool Array<Record>::resize_buf(Error *error, Int new_size) {
  Int new_capacity = capacity_ * 2;
  if (new_size > new_capacity) {
    new_capacity = new_size;
  }
  Int new_buf_size = sizeof(Value) * new_capacity;
  unique_ptr<char[]> new_buf(new (nothrow) char[new_buf_size]);
  if (!new_buf) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  Value *new_values = reinterpret_cast<Value *>(new_buf.get());
  for (Int i = 0; i < size_; ++i) {
    new (&new_values[i]) Value(std::move(data()[i]));
  }
  buf_ = std::move(new_buf);
  capacity_ = new_capacity;
  return true;
}

}  // namespace grnxx
