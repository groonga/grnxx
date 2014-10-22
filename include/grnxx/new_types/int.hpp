#ifndef GRNXX_NEW_TYPES_INT_HPP
#define GRNXX_NEW_TYPES_INT_HPP

#include <cstdint>
#include <limits>

#include "grnxx/new_types/na.hpp"

namespace grnxx {

class Int {
 public:
  Int() = default;
  ~Int() = default;

  constexpr Int(const Int &) = default;
  Int &operator=(const Int &) = default;

  explicit constexpr Int(int64_t value) : value_(value) {}
  explicit constexpr Int(NA) : value_(na_value()) {}

  constexpr int64_t value() const {
    return value_;
  }

  constexpr bool is_min() const {
    return value_ == min_value();
  }
  constexpr bool is_max() const {
    return value_ == max_value();
  }
  constexpr bool is_na() const {
    return value_ == na_value();
  }

  // -- Unary operators --

  constexpr Int operator+() const {
    return *this;
  }
  constexpr Int operator-() const {
    return is_na() ? na() : Int(-value_);
  }
  constexpr Int operator~() const {
    return is_na() ? na() : Int(~value_);
  }

  Int &operator++() & {
    if (!is_na()) {
      ++value_;
    }
    return *this;
  }
  Int operator++(int) & {
    if (is_na()) {
      return na();
    }
    return Int(value_++);
  }
  Int &operator--() & {
    if (!is_na()) {
      --value_;
    }
    return *this;
  }
  Int operator--(int) & {
    if (is_na()) {
      return na();
    }
    return Int(value_--);
  }

  // -- Binary operators --

  constexpr Int operator&(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(value_ & rhs.value_);
  }
  constexpr Int operator|(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(value_ | rhs.value_);
  }
  constexpr Int operator^(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(value_ ^ rhs.value_);
  }

  Int &operator&=(Int rhs) & {
    if (!is_na()) {
      value_ = rhs.is_na() ? na_value() : (value_ & rhs.value_);
    }
    return *this;
  }
  Int &operator|=(Int rhs) & {
    if (!is_na()) {
      value_ = rhs.is_na() ? na_value() : (value_ | rhs.value_);
    }
    return *this;
  }
  Int &operator^=(Int rhs) & {
    if (!is_na()) {
      value_ = rhs.is_na() ? na_value() : (value_ ^ rhs.value_);
    }
    return *this;
  }

  // TODO: Bitwise shift operators.

  // TODO: Arithmetic operators.

  // -- Comparison operators --

  constexpr Bool operator==(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ == rhs.value_);
  }
  constexpr Bool operator!=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ != rhs.value_);
  }
  constexpr Bool operator<(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ < rhs.value_);
  }
  constexpr Bool operator>(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ > rhs.value_);
  }
  constexpr Bool operator<=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ <= rhs.value_);
  }
  constexpr Bool operator>=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ >= rhs.value_);
  }

  static constexpr Int na() {
    return Int(NA());
  }

  static constexpr int64_t min_value() {
    return std::numeric_limits<int64_t>::min() + 1;
  }
  static constexpr int64_t max_value() {
    return std::numeric_limits<int64_t>::max();
  }
  static constexpr int64_t na_value() {
    return std::numeric_limits<int64_t>::min();
  }

 private:
  int64_t value_;
};

}  // namespace grnxx

#endif  // GRNXX_NEW_TYPES_INT_HPP
