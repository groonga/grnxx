#include "grnxx/string.hpp"

#include <cstdlib>

namespace grnxx {

String::~String() {
  if (capacity_ != 0) {
    std::free(buffer_);
  }
}

String &String::operator=(const String &rhs) {
  if (capacity_ != 0) {
    std::free(buffer_);
  }
  data_ = rhs.data_;
  size_ = rhs.size_;
  capacity_ = 0;
  return *this;
}

String::String(size_t size)
    : buffer_(),
      size_(),
      capacity_() {
  char *new_buffer = static_cast<char *>(std::malloc(size));
  if (!new_buffer) {
    throw "Failed";  // TODO
  }
  buffer_ = new_buffer;
  size_ = size;
  capacity_ = size;
}

String::String(size_t size, char byte)
    : buffer_(),
      size_(),
      capacity_() {
  char *new_buffer = static_cast<char *>(std::malloc(size));
  if (!new_buffer) {
    throw "Failed";  // TODO
  }
  std::memset(new_buffer, byte, size);
  buffer_ = new_buffer;
  size_ = size;
  capacity_ = size;
}

String String::clone() const {
  String clone(size_);
  std::memcpy(clone.buffer_, data_, size_);
  return clone;
}

String &String::instantiate() {
  if (is_empty() || is_instance()) {
    // Nothing to do.
    return *this;
  }
  char *new_buffer = static_cast<char *>(std::malloc(size_));
  if (!new_buffer) {
    throw "Failed";  // TODO
  }
  std::memcpy(new_buffer, data_, size_);
  buffer_ = new_buffer;
  capacity_ = size_;
  return *this;
}

void String::resize_buffer(size_t new_size) {
  size_t new_capacity = capacity_ * 2;
  if (new_size > new_capacity) {
    new_capacity = new_size;
  }
  char *new_buffer = static_cast<char *>(std::malloc(new_capacity));
  if (!new_buffer) {
    throw "Failed";  // TODO
  }
  std::memcpy(new_buffer, data_, size_);
  if (capacity_ != 0) {
    std::free(buffer_);
  }
  buffer_ = new_buffer;
  capacity_ = new_capacity;
}

void String::append_overlap(const char *data, size_t size) {
  size_t new_capacity = capacity_ * 2;
  size_t new_size = size_ + size;
  if (new_size > new_capacity) {
    new_capacity = new_size;
  }
  char *new_buffer = static_cast<char *>(std::malloc(new_capacity));
  if (!new_buffer) {
    throw "Failed";  // TODO
  }
  std::memcpy(new_buffer, buffer_, size_);
  std::memcpy(new_buffer + size_, data, size);
  if (capacity_ != 0) {
    std::free(buffer_);
  }
  buffer_ = new_buffer;
  size_ = new_size;
  capacity_ = new_capacity;
}

}  // namespace grnxx
