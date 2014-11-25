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
  explicit Text(const String &string)
      : data_(string.data()),
        size_(string.size()) {}
  explicit constexpr Text(NA) : data_(nullptr), size_(NA()) {}

  constexpr const char *raw_data() const {
    return data_;
  }
  constexpr size_t raw_size() const {
    return size_.raw();
  }
  constexpr Int size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return raw_size() == 0;
  }
  constexpr bool is_na() const {
    return size_.is_na();
  }

  Bool operator==(const Text &rhs) const {
    Bool has_equal_size = (size_ == rhs.size_);
    if (has_equal_size.is_true()) {
      return Bool(std::memcmp(data_, rhs.data_, raw_size()) == 0);
    }
    return has_equal_size;
  }
  Bool operator!=(const Text &rhs) const {
    Bool has_not_equal_size = (size_ != rhs.size_);
    if (has_not_equal_size.is_false()) {
      return Bool(std::memcmp(data_, rhs.data_, raw_size()) != 0);
    }
    return has_not_equal_size;
  }
  Bool operator<(const Text &rhs) const {
    Bool has_less_size = (size_ < rhs.size_);
    if (has_less_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_size.is_true() ?
                      raw_size() : rhs.raw_size();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_size : Bool(false));
  }
  Bool operator>(const Text &rhs) const {
    return rhs < *this;
  }
  Bool operator<=(const Text &rhs) const {
    Bool has_less_or_equal_size = (size_ <= rhs.size_);
    if (has_less_or_equal_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_or_equal_size.is_true() ?
                      raw_size() : rhs.raw_size();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_or_equal_size : Bool(false));
  }
  Bool operator>=(const Text &rhs) const {
    return rhs <= *this;
  }

  Bool starts_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size.is_true()) {
      return Bool(std::memcmp(data_, rhs.data_, rhs.raw_size()) == 0);
    }
    return has_greater_or_equal_size;
  }
  Bool ends_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size.is_true()) {
      return Bool(std::memcmp(data_ + raw_size() - rhs.raw_size(),
                              rhs.data_, rhs.raw_size()) == 0);
    }
    return has_greater_or_equal_size;
  }

  bool match(const Text &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return false;
    }
    if (is_na()) {
      return true;
    }
    return std::memcmp(data_, rhs.data_, raw_size()) == 0;
  }
  bool unmatch(const Text &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return true;
    }
    if (is_na()) {
      return false;
    }
    return std::memcmp(data_, rhs.data_, raw_size()) != 0;
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

  static constexpr size_t raw_na_size() {
    return Int::na().raw();
  }

 private:
  const char *data_;
  Int size_;
};

}  // namespace grnxx

#endif   // GRNXX_DATA_TYPES_SCALAR_TEXT_HPP
