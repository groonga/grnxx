#include "grnxx/cursor.hpp"

#include <limits>

namespace grnxx {
namespace {

constexpr size_t CURSOR_BLOCK_SIZE = 1024;

}  // namespace

size_t Cursor::read(size_t max_count, Array<Record> *records) {
  if (max_count == 0) {
    return 0;
  }

  size_t offset = records->size();
  if (offset > (std::numeric_limits<size_t>::max() - max_count)) {
    // Reduce "max_count" to avoid overflow.
    max_count = std::numeric_limits<size_t>::max() - offset;
  }

  size_t next_size = offset + max_count;
  if (next_size <= records->capacity()) {
    // There is enough space to store requested records.
    records->resize(next_size);
    size_t count = read(records->ref(offset, max_count));
    records->resize(offset + count);
    return count;
  }

  // Read the first block.
  size_t block_size = max_count;
  if (block_size > CURSOR_BLOCK_SIZE) {
    block_size = CURSOR_BLOCK_SIZE;
  }
  records->resize(offset + block_size);
  size_t count = read(records->ref(offset, block_size));
  if (count != block_size) {
    records->resize(offset + count);
    if (count == 0) {
      return 0;
    }
  }

  // Read the remaining blocks.
  while (count < max_count) {
    block_size = max_count - count;
    if (block_size > CURSOR_BLOCK_SIZE) {
      block_size = CURSOR_BLOCK_SIZE;
    }
    size_t this_offset = offset + count;
    records->resize(this_offset + block_size);
    size_t this_count = read(records->ref(this_offset, block_size));
    if (this_count != block_size) {
      records->resize(this_offset + this_count);
    }
    count += this_count;
    if (this_count == 0) {
      return count;
    }
  }
  return count;
}

size_t Cursor::read_all(Array<Record> *records) {
  return read(std::numeric_limits<size_t>::max(), records);
}

}  // namespace grnxx
