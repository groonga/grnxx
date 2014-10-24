#ifndef GRNXX_NEW_TYPES_INT_HPP
#define GRNXX_NEW_TYPES_INT_HPP

#include <cstdint>
#include <limits>

#include "grnxx/new_types/na.hpp"

namespace grnxx {

// NOTE: This implementation assumes two's complement.
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
  // NOTE: This implementation assumes that -na_value() returns na_value(),
  //       although -na_value() in two's complement causes an overflow and
  //       the behavior is undefined in C/C++.
  //       If this assumption is wrong, N/A must be excluded.
  constexpr Int operator-() const {
    return Int(-value_);
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

  // -- Bitwise shift operators --

  constexpr Int operator<<(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.value_) >= 64)) ?
           na() : Int(value_ << rhs.value_);
  }
  // NOTE: This is an arithmetic shift.
  constexpr Int operator>>(Int rhs) const {
    return arithmetic_right_shift(rhs);
  }

  Int &operator<<=(Int rhs) & {
    if (!is_na()) {
      if (rhs.is_na() || (static_cast<uint64_t>(rhs.value_) >= 64)) {
        value_ = na_value();
      } else {
        value_ <<= rhs.value_;
      }
    }
    return *this;
  }
  // NOTE: This is an arithmetic shift.
  Int &operator>>=(Int rhs) & {
    if (!is_na()) {
      if (rhs.is_na() || (static_cast<uint64_t>(rhs.value_) >= 64)) {
        value_ = na_value();
      } else {
        value_ >>= rhs.value_;
      }
    }
    return *this;
  }

  constexpr Int arithmetic_right_shift(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.value_) >= 64)) ?
           na() : Int(value_ >> rhs.value_);
  }
  constexpr Int logical_right_shift(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.value_) >= 64)) ?
           na() : Int(static_cast<uint64_t>(value_) >> rhs.value_);
  }

  // -- Arithmetic operators --

  // NOTE: C++11 does not allow `if` in constexpr function, but C++14 does.
  Int operator+(Int rhs) const {
    return add(*this, rhs);
  }
  Int operator-(Int rhs) const {
    return subtract(*this, rhs);
  }
  Int operator*(Int rhs) const {
    return multiply(*this, rhs);
  };
  Int operator/(Int rhs) const {
    return (is_na() || rhs.is_na() || (rhs.value_ == 0)) ?
           na() : Int(value_ / rhs.value_);
  }
  Int operator%(Int rhs) const {
    return (is_na() || rhs.is_na() || (rhs.value_ == 0)) ?
           na() : Int(value_ % rhs.value_);
  }

  Int &operator+=(Int rhs) & {
    return *this = operator+(rhs);
  }
  Int &operator-=(Int rhs) & {
    return *this = operator-(rhs);
  }
  Int &operator*=(Int rhs) & {
    return *this = operator*(rhs);
  }
  Int &operator/=(Int rhs) &{
    if (!is_na()) {
      value_ = (rhs.is_na() || (rhs.value_ == 0)) ?
               na_value() : (value_ / rhs.value_);
    }
    return *this;
  }
  Int &operator%=(Int rhs) &{
    if (!is_na()) {
      value_ = (rhs.is_na() || (rhs.value_ == 0)) ?
               na_value() : (value_ % rhs.value_);
    }
    return *this;
  }

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

  static constexpr Int min() {
    return Int(min_value());
  }
  static constexpr Int max() {
    return Int(max_value());
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

#if defined(GRNXX_HAVE_X86_64)
 #if defined(GRNXX_HAVE_GNUC)
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    __asm__ ("ADD %1, %0;"
             "JNO GRNXX_INT_ADD_OVERFLOW%=;"
             "MOV %2, %0;"
             "GRNXX_INT_ADD_OVERFLOW%=:"
             : "+r" (lhs.value_)
             : "r" (rhs.value_), "r" (na_value())
             : "cc");
    return lhs;
  }
  static Int subtract(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    __asm__ ("SUB %1, %0;"
             "JNO GRNXX_INT_SUBTRACT_OVERFLOW%=;"
             "MOV %2, %0;"
             "GRNXX_INT_SUBTRACT_OVERFLOW%=:"
             : "+r" (lhs.value_)
             : "r" (rhs.value_), "r" (na_value())
             : "cc");
    return lhs;
  }
  static Int multiply(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    __asm__ ("IMUL %1, %0;"
             "JNO GRNXX_INT_MULTIPLY_OVERFLOW%=;"
             "MOV %2, %0;"
             "GRNXX_INT_MULTIPLY_OVERFLOW%=:"
             : "+r" (lhs.value_)
             : "r" (rhs.value_), "r" (na_value())
             : "cc");
    return lhs;
  }
 #else  // !defined(GRNXX_HAVE_GNUC)
  // TODO: Use assmbly for VC++.
 #endif  // defined(GRNXX_HAVE_GNUC), etc.
#elif defined(GRNXX_HAVE_WRAP_AROUND)  // !defined(GRNXX_HAVE_X86_64)
  // NOTE: These implementations assume silent two's complement wrap-around,
  //       although a signed integer overflow causes undefined behavior in
  //       C/C++.
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    int64_t result_value = lhs.value_ + rhs.value_;
    lhs.value_ ^= result_value;
    rhs.value_ ^= result_value;
    if (static_cast<uint64_t>(lhs.value_ & rhs.value_) >> 63) {
      return na();
    }
    return Int(result_value);
  }
  static Int subtract(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    int64_t result_value = lhs.value_ - rhs.value_;
    lhs.value_ ^= result_value;
    rhs.value_ ^= result_value;
    if (static_cast<uint64_t>(lhs.value_ & rhs.value_) >> 63) {
      return na();
    }
    return Int(result_value);
  }
  static Int multiply(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.value_ == 0) {
      return Int(0);
    }
    int64_t result = lhs.value_ * rhs.value_;
    if ((result / rhs.value_) != lhs.value_) {
      return na();
    }
    return Int(result);
  }
#else  // !defined(GRNXX_HAVE_X86_64) && !defined(GRNXX_HAVE_WRAP_AROUND)
  // NOTE: These implementations are portable but slow.
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (lhs.value_ >= 0) {
      if (rhs.value_ > (max_value() - lhs.value_)) {
        return na();
      }
    } else {
      if (rhs.value_ < (min_value() - lhs.value_)) {
        return na();
      }
    }
    return Int(lhs.value_ + rhs.value_);
  }
  static Int subtract(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.value_ >= 0) {
      if (lhs.value_ < (min_value() + rhs.value_)) {
        return na();
      }
    } else {
      if (lhs.value_ > (max_value() + rhs.value_)) {
        return na();
      }
    }
    return Int(lhs.value_ - rhs.value_);
  }
  static Int multiply(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.value_ == 0) {
      return Int(0);
    }
    if (lhs.value_ >= 0) {
      if (rhs.value_ > 0) {
        if (lhs.value_ > (max_value() / rhs.value_)) {
          return na();
        }
      } else {
        if (lhs.value_ > (min_value() / rhs.value_)) {
          return na();
        }
      }
    } else if (rhs.value_ > 0) {
      if (lhs.value_ < (min_value() / rhs.value_)) {
        return na();
      }
    } else {
      if (lhs.value_ < (max_value() / rhs.value_)) {
        return na();
      }
    }
    return Int(lhs.value_ * rhs.value_);
  }
#endif  // defined(GRNXX_HAVE_X86_64), etc.
};

}  // namespace grnxx

#endif  // GRNXX_NEW_TYPES_INT_HPP
