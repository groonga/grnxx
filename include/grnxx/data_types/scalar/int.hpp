#ifndef GRNXX_DATA_TYPES_SCALAR_INT_HPP
#define GRNXX_DATA_TYPES_SCALAR_INT_HPP

#include <cstdint>
#include <limits>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/features.hpp"

namespace grnxx {

class Float;

// NOTE: This implementation assumes two's complement.
class Int {
 public:
  Int() = default;
  ~Int() = default;

  constexpr Int(const Int &) = default;
  Int &operator=(const Int &) = default;

  explicit constexpr Int(int64_t raw) : raw_(raw) {}
  explicit constexpr Int(NA) : raw_(raw_na()) {}

  constexpr int64_t raw() const {
    return raw_;
  }

  constexpr bool is_min() const {
    return raw_ == raw_min();
  }
  constexpr bool is_max() const {
    return raw_ == raw_max();
  }
  constexpr bool is_na() const {
    return raw_ == raw_na();
  }

  // -- Unary operators --

  constexpr Int operator+() const {
    return *this;
  }
  // NOTE: This implementation assumes that -raw_na() returns raw_na(),
  //       although -raw_na() in two's complement causes an overflow and
  //       the behavior is undefined in C/C++.
  //       If this assumption is wrong, N/A must be excluded.
  constexpr Int operator-() const {
    return Int(-raw_);
  }
  constexpr Int operator~() const {
    return is_na() ? na() : Int(~raw_);
  }

  Int &operator++() & {
    if (!is_na()) {
      ++raw_;
    }
    return *this;
  }
  Int operator++(int) & {
    if (is_na()) {
      return na();
    }
    return Int(raw_++);
  }
  Int &operator--() & {
    if (!is_na()) {
      --raw_;
    }
    return *this;
  }
  Int operator--(int) & {
    if (is_na()) {
      return na();
    }
    return Int(raw_--);
  }

  // -- Binary operators --

  constexpr Int operator&(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(raw_ & rhs.raw_);
  }
  constexpr Int operator|(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(raw_ | rhs.raw_);
  }
  constexpr Int operator^(Int rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Int(raw_ ^ rhs.raw_);
  }

  Int &operator&=(Int rhs) & {
    if (!is_na()) {
      raw_ = rhs.is_na() ? raw_na() : (raw_ & rhs.raw_);
    }
    return *this;
  }
  Int &operator|=(Int rhs) & {
    if (!is_na()) {
      raw_ = rhs.is_na() ? raw_na() : (raw_ | rhs.raw_);
    }
    return *this;
  }
  Int &operator^=(Int rhs) & {
    if (!is_na()) {
      raw_ = rhs.is_na() ? raw_na() : (raw_ ^ rhs.raw_);
    }
    return *this;
  }

  // -- Bitwise shift operators --

  constexpr Int operator<<(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.raw_) >= 64)) ?
             na() : Int(raw_ << rhs.raw_);
  }
  // NOTE: This is an arithmetic shift.
  constexpr Int operator>>(Int rhs) const {
    return arithmetic_right_shift(rhs);
  }

  Int &operator<<=(Int rhs) & {
    if (!is_na()) {
      if (rhs.is_na() || (static_cast<uint64_t>(rhs.raw_) >= 64)) {
        raw_ = raw_na();
      } else {
        raw_ <<= rhs.raw_;
      }
    }
    return *this;
  }
  // NOTE: This is an arithmetic shift.
  Int &operator>>=(Int rhs) & {
    if (!is_na()) {
      if (rhs.is_na() || (static_cast<uint64_t>(rhs.raw_) >= 64)) {
        raw_ = raw_na();
      } else {
        raw_ >>= rhs.raw_;
      }
    }
    return *this;
  }

  constexpr Int arithmetic_right_shift(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.raw_) >= 64)) ?
           na() : Int(raw_ >> rhs.raw_);
  }
  constexpr Int logical_right_shift(Int rhs) const {
    return (is_na() || rhs.is_na() ||
            (static_cast<uint64_t>(rhs.raw_) >= 64)) ?
           na() : Int(static_cast<uint64_t>(raw_) >> rhs.raw_);
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
    return (is_na() || rhs.is_na() || (rhs.raw_ == 0)) ?
           na() : Int(raw_ / rhs.raw_);
  }
  Int operator%(Int rhs) const {
    return (is_na() || rhs.is_na() || (rhs.raw_ == 0)) ?
           na() : Int(raw_ % rhs.raw_);
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
      raw_ = (rhs.is_na() || (rhs.raw_ == 0)) ? raw_na() : (raw_ / rhs.raw_);
    }
    return *this;
  }
  Int &operator%=(Int rhs) &{
    if (!is_na()) {
      raw_ = (rhs.is_na() || (rhs.raw_ == 0)) ? raw_na() : (raw_ % rhs.raw_);
    }
    return *this;
  }

  // -- Comparison operators --

  constexpr Bool operator==(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ == rhs.raw_);
  }
  constexpr Bool operator!=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ != rhs.raw_);
  }
  constexpr Bool operator<(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ < rhs.raw_);
  }
  constexpr Bool operator>(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ > rhs.raw_);
  }
  constexpr Bool operator<=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ <= rhs.raw_);
  }
  constexpr Bool operator>=(Int rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ >= rhs.raw_);
  }

  constexpr bool match(Int rhs) const {
    return raw_ == rhs.raw_;
  }
  constexpr bool unmatch(Int rhs) const {
    return raw_ != rhs.raw_;
  }

  // -- Typecast (grnxx/data_types/typecast.hpp) --

  constexpr Float to_float() const;

  static constexpr DataType type() {
    return INT_DATA;
  }

  static constexpr Int min() {
    return Int(raw_min());
  }
  static constexpr Int max() {
    return Int(raw_max());
  }
  static constexpr Int na() {
    return Int(NA());
  }

  static constexpr int64_t raw_min() {
    return std::numeric_limits<int64_t>::min() + 1;
  }
  static constexpr int64_t raw_max() {
    return std::numeric_limits<int64_t>::max();
  }
  static constexpr int64_t raw_na() {
    return std::numeric_limits<int64_t>::min();
  }

 private:
  int64_t raw_;

#if defined(GRNXX_GNUC) && defined(GRNXX_X86_64)
  // TODO: Implementations for MSC should be written.
  // NOTE: These implementations use x86_64 instructions for speed.
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    __asm__ ("ADD %1, %0;"
             "JNO GRNXX_INT_ADD_OVERFLOW%=;"
             "MOV %2, %0;"
             "GRNXX_INT_ADD_OVERFLOW%=:"
             : "+r" (lhs.raw_)
             : "r" (rhs.raw_), "r" (raw_na())
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
             : "+r" (lhs.raw_)
             : "r" (rhs.raw_), "r" (raw_na())
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
             : "+r" (lhs.raw_)
             : "r" (rhs.raw_), "r" (raw_na())
             : "cc");
    return lhs;
  }
#elif defined(GRNXX_WRAP_AROUND)
  // TODO: The following implementations should be used if the above
  //       implementations are not available.
  // NOTE: These implementations assume silent two's complement wrap-around.
  //       The C/C++ standards say that a signed integer overflow causes
  //       undefined behavior.
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    int64_t result_raw = lhs.raw_ + rhs.raw_;
    lhs.raw_ ^= result_raw;
    rhs.raw_ ^= result_raw;
    if (static_cast<uint64_t>(lhs.raw_ & rhs.raw_) >> 63) {
      return na();
    }
    return Int(result_raw);
  }
  static Int subtract(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    int64_t result_raw = lhs.raw_ - rhs.raw_;
    lhs.raw_ ^= result_raw;
    rhs.raw_ ^= result_raw;
    if (static_cast<uint64_t>(lhs.raw_ & rhs.raw_) >> 63) {
      return na();
    }
    return Int(result_raw);
  }
  static Int multiply(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.raw_ == 0) {
      return Int(0);
    }
    int64_t result = lhs.raw_ * rhs.raw_;
    if ((result / rhs.raw_) != lhs.raw_) {
      return na();
    }
    return Int(result);
  }
# else  // defined(GRNXX_WRAP_AROUND)
  // NOTE: These implementations are portable but slow.
  static Int add(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (lhs.raw_ >= 0) {
      if (rhs.raw_ > (raw_max() - lhs.raw_)) {
        return na();
      }
    } else {
      if (rhs.raw_ < (raw_min() - lhs.raw_)) {
        return na();
      }
    }
    return Int(lhs.raw_ + rhs.raw_);
  }
  static Int subtract(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.raw_ >= 0) {
      if (lhs.raw_ < (raw_min() + rhs.raw_)) {
        return na();
      }
    } else {
      if (lhs.raw_ > (raw_max() + rhs.raw_)) {
        return na();
      }
    }
    return Int(lhs.raw_ - rhs.raw_);
  }
  static Int multiply(Int lhs, Int rhs) {
    if (lhs.is_na() || rhs.is_na()) {
      return na();
    }
    if (rhs.raw_ == 0) {
      return Int(0);
    }
    if (lhs.raw_ >= 0) {
      if (rhs.raw_ > 0) {
        if (lhs.raw_ > (raw_max() / rhs.raw_)) {
          return na();
        }
      } else {
        if (lhs.raw_ > (raw_min() / rhs.raw_)) {
          return na();
        }
      }
    } else if (rhs.raw_ > 0) {
      if (lhs.raw_ < (raw_min() / rhs.raw_)) {
        return na();
      }
    } else {
      if (lhs.raw_ < (raw_max() / rhs.raw_)) {
        return na();
      }
    }
    return Int(lhs.raw_ * rhs.raw_);
  }
# endif  // GRNXX_WRAP_AROUND
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_INT_HPP
