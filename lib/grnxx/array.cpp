#include "grnxx/array.hpp"

#include "grnxx/error.hpp"

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

}  // namespace grnxx
