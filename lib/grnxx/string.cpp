#include "grnxx/string.hpp"

#include <cstdlib>

namespace grnxx {

String::~String() {
  std::free(buf_);
}

bool String::resize_buf(Error *error, size_t new_size) {
  size_t new_capacity = capacity_ * 2;
  if (new_size > new_capacity) {
    new_capacity = new_size;
  }
  char *new_buf = static_cast<char *>(std::malloc(new_capacity));
  if (!new_buf) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  std::memcpy(new_buf, buf_, size_);
  std::free(buf_);
  buf_ = new_buf;
  capacity_ = new_capacity;
  return true;
}

bool String::append_overlap(Error *error, const StringCRef &rhs) {
  size_t new_capacity = capacity_ * 2;
  size_t new_size = size_ + rhs.size();
  if (new_size > new_capacity) {
    new_capacity = new_size;
  }
  char *new_buf = static_cast<char *>(std::malloc(new_capacity));
  if (!new_buf) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
  std::memcpy(new_buf, buf_, size_);
  std::memcpy(new_buf + size_, rhs.data(), rhs.size());
  std::free(buf_);
  buf_ = new_buf;
  size_ = new_size;
  capacity_ = new_capacity;
  return true;
}

}  // namespace grnxx
