#ifndef GRNXX_NEW_TYPES_TEXT_HPP
#define GRNXX_NEW_TYPES_TEXT_HPP

#include <cstdint>
#include <cstring>

#include "grnxx/new_types/na.hpp"

namespace grnxx {

// Reference to a byte string.
class Text {
 public:
  Text() = default;
  ~Text() = default;

  constexpr Text(const Text &) = default;
  Text &operator=(const Text &) = default;

  explicit Text(const char *str)
      : data_(str), size_(str ? std::strlen(str) : 0) {}
  constexpr Text(const char *data, size_t size) : data_(data), size_(size) {}
  explicit constexpr Text(NA) : data_(nullptr), size_(0) {}

  const char &operator[](size_t i) const {
    return data_[i];
  }
  constexpr const char *data() const {
    return data_;
  }
  constexpr size_t size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return !is_na() && (size_ == 0);
  }
  constexpr bool is_na() const {
    return data_ == na_data();
  }

  Bool operator==(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    if (size_ != rhs.size_) {
      return Bool(false);
    }
    return Bool(std::memcmp(data_, rhs.data_, size_) == 0);
  }
  Bool operator!=(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    if (size_ != rhs.size_) {
      return Bool(true);
    }
    return Bool(std::memcmp(data_, rhs.data_, size_) != 0);
  }
  Bool operator<(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return Bool((result < 0) || ((result == 0) && (size_ < rhs.size_)));
  }
  Bool operator>(const Text &rhs) const {
    return rhs < *this;
  }
  Bool operator<=(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return Bool((result < 0) || ((result == 0) && (size_ <= rhs.size_)));
  }
  Bool operator>=(const Text &rhs) const {
    return rhs <= *this;
  }

  Bool starts_with(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    if (size_ < rhs.size_) {
      return Bool(false);
    }
    return Bool(std::memcmp(data_, rhs.data_, rhs.size_) == 0);
  }
  Bool ends_with(const Text &rhs) const {
    if (is_na() || rhs.is_na()) {
      return Bool::na();
    }
    if (size_ < rhs.size_) {
      return Bool(false);
    }
    return Bool(std::memcmp(data_ + size_ - rhs.size_,
                            rhs.data_, rhs.size_) == 0);
  }

  static constexpr Text empty() {
    return Text("", 0);
  }
  static constexpr Text na() {
    return Text(NA());
  }

  static constexpr const char *na_data() {
    return nullptr;
  }

 private:
  const char *data_;
  size_t size_;
};

}  // namespace grnxx

#endif   // GRNXX_NEW_TYPES_TEXT_HPP
