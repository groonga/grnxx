#ifndef GRNXX_DATA_TYPES_SCALAR_TEXT_HPP
#define GRNXX_DATA_TYPES_SCALAR_TEXT_HPP

#include <cstdint>
#include <cstring>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/int.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

// Reference to a byte string.
class Text {
 public:
  Text() = default;
  ~Text() = default;

  constexpr Text(const Text &) = default;
  Text &operator=(const Text &) = default;

  explicit Text(const char *string)
      : data_(string),
        size_(std::strlen(string)) {}
  constexpr Text(const char *data, size_t size) : data_(data), size_(size) {}
  explicit constexpr Text(const String &string)
      : data_(string.data()),
        size_(string.size()) {}
  explicit constexpr Text(NA) : data_(nullptr), size_(NA()) {}

  const char &operator[](size_t i) const {
    return data_[i];
  }
  constexpr const char *data() const {
    return data_;
  }
  constexpr Int size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return size_.value() == 0;
  }
  constexpr bool is_na() const {
    return size_.is_na();
  }

  Bool operator==(const Text &rhs) const {
    Bool has_equal_size = (size_ == rhs.size_);
    if (has_equal_size) {
      return Bool(std::memcmp(data_, rhs.data_, size_.value()) == 0);
    }
    return has_equal_size;
  }
  Bool operator!=(const Text &rhs) const {
    Bool has_not_equal_size = (size_ != rhs.size_);
    if (has_not_equal_size.is_false()) {
      return Bool(std::memcmp(data_, rhs.data_, size_.value()) != 0);
    }
    return has_not_equal_size;
  }
  Bool operator<(const Text &rhs) const {
    Bool has_less_size = (size_ < rhs.size_);
    if (has_less_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_size ? size_.value() : rhs.size_.value();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_size : Bool(false));

//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
//    int result = std::memcmp(data_, rhs.data_, min_size);
//    return Bool((result < 0) || ((result == 0) && (size_ < rhs.size_)));
  }
  Bool operator>(const Text &rhs) const {
    return rhs < *this;
  }
  Bool operator<=(const Text &rhs) const {
    Bool has_less_or_equal_size = (size_ <= rhs.size_);
    if (has_less_or_equal_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_or_equal_size ?
                      size_.value() : rhs.size_.value();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_or_equal_size : Bool(false));

//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
//    int result = std::memcmp(data_, rhs.data_, min_size);
//    return Bool((result < 0) || ((result == 0) && (size_ <= rhs.size_)));
  }
  Bool operator>=(const Text &rhs) const {
    return rhs <= *this;
  }

  Bool starts_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size) {
      return Bool(std::memcmp(data_, rhs.data_, rhs.size_.value()) == 0);
    }
    return has_greater_or_equal_size;

//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ < rhs.size_) {
//      return Bool(false);
//    }
//    return Bool(std::memcmp(data_, rhs.data_, rhs.size_) == 0);
  }
  Bool ends_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size) {
      return Bool(std::memcmp(data_ + size_.value() - rhs.size_.value(),
                              rhs.data_, rhs.size_.value()) == 0);
    }
    return has_greater_or_equal_size;

//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ < rhs.size_) {
//      return Bool(false);
//    }
//    return Bool(std::memcmp(data_ + size_ - rhs.size_,
//                            rhs.data_, rhs.size_) == 0);
  }

  static constexpr DataType type() {
    return TEXT_DATA;
  }

  static constexpr Text empty() {
    return Text(nullptr, 0);
  }
  static constexpr Text na() {
    return Text(NA());
  }

 private:
  const char *data_;
  Int size_;
};

//// Reference to a byte string.
//class Text {
// public:
//  Text() = default;
//  ~Text() = default;

//  constexpr Text(const Text &) = default;
//  Text &operator=(const Text &) = default;

//  explicit Text(const char *str)
//      : data_(str),
//        size_(str ? std::strlen(str) : na_size()) {}
//  constexpr Text(const char *data, size_t size) : data_(data), size_(size) {}
//  explicit constexpr Text(NA) : data_(na_data()), size_(na_size()) {}

//  const char &operator[](size_t i) const {
//    return data_[i];
//  }
//  constexpr const char *data() const {
//    return data_;
//  }
//  constexpr size_t size() const {
//    return size_;
//  }

//  constexpr bool is_empty() const {
//    return !is_na() && (size_ == 0);
//  }
//  constexpr bool is_na() const {
//    return data_ == na_data();
//  }

//  Bool operator==(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ != rhs.size_) {
//      return Bool(false);
//    }
//    return Bool(std::memcmp(data_, rhs.data_, size_) == 0);
//  }
//  Bool operator!=(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ != rhs.size_) {
//      return Bool(true);
//    }
//    return Bool(std::memcmp(data_, rhs.data_, size_) != 0);
//  }
//  Bool operator<(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
//    int result = std::memcmp(data_, rhs.data_, min_size);
//    return Bool((result < 0) || ((result == 0) && (size_ < rhs.size_)));
//  }
//  Bool operator>(const Text &rhs) const {
//    return rhs < *this;
//  }
//  Bool operator<=(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    size_t min_size = size_ < rhs.size_ ? size_ : rhs.size_;
//    int result = std::memcmp(data_, rhs.data_, min_size);
//    return Bool((result < 0) || ((result == 0) && (size_ <= rhs.size_)));
//  }
//  Bool operator>=(const Text &rhs) const {
//    return rhs <= *this;
//  }

//  Bool starts_with(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ < rhs.size_) {
//      return Bool(false);
//    }
//    return Bool(std::memcmp(data_, rhs.data_, rhs.size_) == 0);
//  }
//  Bool ends_with(const Text &rhs) const {
//    if (is_na() || rhs.is_na()) {
//      return Bool::na();
//    }
//    if (size_ < rhs.size_) {
//      return Bool(false);
//    }
//    return Bool(std::memcmp(data_ + size_ - rhs.size_,
//                            rhs.data_, rhs.size_) == 0);
//  }

//  static constexpr DataType type() {
//    return TEXT_DATA;
//  }

//  static constexpr Text empty() {
//    return Text("", 0);
//  }
//  static constexpr Text na() {
//    return Text(NA());
//  }

//  static constexpr const char *na_data() {
//    return nullptr;
//  }
//  static constexpr size_t na_size() {
//    return 0;
//  }

// private:
//  const char *data_;
//  size_t size_;
//};

}  // namespace grnxx

#endif   // GRNXX_DATA_TYPES_SCALAR_TEXT_HPP
