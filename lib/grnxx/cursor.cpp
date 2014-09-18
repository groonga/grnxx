#include "grnxx/cursor.hpp"

namespace grnxx {
namespace {

constexpr Int CURSOR_BLOCK_SIZE = 1024;

}  // namespace

Int Cursor::read(Error *error, Int max_count, Array<Record> *records) {
  if (max_count < 0) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid argument");
    return -1;
  } else if (max_count == 0) {
    return read(error, records->ref(0, 0));
  }

  Int offset = records->size();
  if (offset > (numeric_limits<Int>::max() - max_count)) {
    // Reduce "max_count" to avoid overflow.
    max_count = numeric_limits<Int>::max() - offset;
  }

  Int next_size = offset + max_count;
  if (next_size <= records->capacity()) {
    // There are enough space for requested records.
    records->resize(nullptr, next_size);
    Int count = read(error, records->ref(offset, max_count));
    records->resize(nullptr, offset + ((count != -1) ? count : 0));
    return count;
  }

  // Read the first block.
  Int block_size = max_count;
  if (block_size > CURSOR_BLOCK_SIZE) {
    block_size = CURSOR_BLOCK_SIZE;
  }
  if (!records->resize(error, offset + block_size)) {
    return -1;
  }
  Int count = read(error, records->ref(offset, block_size));
  if (count != block_size) {
    records->resize(nullptr, offset + ((count != -1) ? count : 0));
    if (count <= 0) {
      return count;
    }
  }

  // Read the remaining blocks.
  while (count < max_count) {
    block_size = max_count - count;
    if (block_size > CURSOR_BLOCK_SIZE) {
      block_size = CURSOR_BLOCK_SIZE;
    }
    Int this_offset = offset + count;
    if (!records->resize(error, this_offset + block_size)) {
      return count;
    }
    Int this_count = read(error, records->ref(this_offset, block_size));
    if (this_count != block_size) {
      records->resize(nullptr,
                      this_offset + ((this_count != -1) ? this_count : 0));
      if (this_count <= 0) {
        return count;
      }
    }
    count += this_count;
  }
  return count;
}

// TODO: To be removed!
Int Cursor::read(Error *, ArrayRef<Record>) {
  return -1;
}

Int Cursor::read_all(Error *error, Array<Record> *records) {
  Int total_count = 0;
  Int count;
  while ((count = read(error, numeric_limits<Int>::max(), records)) > 0) {
    total_count += count;
  }
  return (count == 0) ? total_count : count;
}

}  // namespace grnxx
