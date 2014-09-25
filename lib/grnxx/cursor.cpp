#include "grnxx/cursor.hpp"

namespace grnxx {
namespace {

constexpr Int CURSOR_BLOCK_SIZE = 1024;

}  // namespace

CursorResult Cursor::read(Error *error,
                          Int max_count,
                          Array<Record> *records) {
  if (max_count < 0) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Invalid argument");
    return { false, 0 };
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
    auto result = read(error, records->ref(offset, max_count));
    records->resize(nullptr, offset + result.count);
    return result;
  }

  // Read the first block.
  Int block_size = max_count;
  if (block_size > CURSOR_BLOCK_SIZE) {
    block_size = CURSOR_BLOCK_SIZE;
  }
  if (!records->resize(error, offset + block_size)) {
    return { false, 0 };
  }
  auto result = read(error, records->ref(offset, block_size));
  if (result.count != block_size) {
    records->resize(nullptr, offset + result.count);
  }
  if (!result.is_ok || (result.count == 0)) {
    return result;
  }

  // Read the remaining blocks.
  while (result.count < max_count) {
    block_size = max_count - result.count;
    if (block_size > CURSOR_BLOCK_SIZE) {
      block_size = CURSOR_BLOCK_SIZE;
    }
    Int this_offset = offset + result.count;
    if (!records->resize(error, this_offset + block_size)) {
      result.is_ok = false;
      return result;
    }
    auto this_result = read(error, records->ref(this_offset, block_size));
    if (this_result.count != block_size) {
      records->resize(nullptr, this_offset + this_result.count);
    }
    result.is_ok = this_result.is_ok;
    result.count += this_result.count;
    if (!this_result.is_ok || (this_result.count == 0)) {
      return result;
    }
  }
  return result;
}

CursorResult Cursor::read_all(Error *error, Array<Record> *records) {
  return read(error, numeric_limits<Int>::max(), records);
}

}  // namespace grnxx
