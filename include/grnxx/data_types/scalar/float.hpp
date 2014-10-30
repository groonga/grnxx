#ifndef GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP
#define GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP

#include <cmath>
#include <limits>

#include "grnxx/data_types/na.hpp"

namespace grnxx {

// NOTE: This implementation assumes IEEE 754.
class Float {
 public:
  Float() = default;
  ~Float() = default;

  constexpr Float(const Float &) = default;
  Float &operator=(const Float &) = default;

  // TODO: +/-0.0 and NaN may be normalized.
  //       -0.0 to +0.0 and various NaN to the quiet NaN.
  explicit constexpr Float(double value) : value_(value) {}
  explicit constexpr Float(NA) : value_(na_value()) {}

  constexpr double value() const {
    return value_;
  }

  constexpr bool is_min() const {
    return value_ == min_value();
  }
  constexpr bool is_max() const {
    return value_ == max_value();
  }
  constexpr bool is_infinity() const {
    return std::isinf(value_);
  }
  constexpr bool is_na() const {
    return std::isnan(value_);
  }

  // -- Unary operators --

  constexpr Float operator+() const {
    return *this;
  }
  constexpr Float operator-() const {
    return Float(-value_);
  }

  // -- Binary operators --

  // -- Arithmetic operators --

  constexpr Float operator+(Float rhs) const {
    return Float(value_ + rhs.value_);
  }
  constexpr Float operator-(Float rhs) const {
    return Float(value_ - rhs.value_);
  }
  constexpr Float operator*(Float rhs) const {
    return Float(value_ * rhs.value_);
  };
  constexpr Float operator/(Float rhs) const {
    return Float(value_ / rhs.value_);
  }
  Float operator%(Float rhs) const {
    return Float(std::fmod(value_, rhs.value_));
  }

  Float &operator+=(Float rhs) & {
    value_ += rhs.value_;
    return *this;
  }
  Float &operator-=(Float rhs) & {
    value_ -= rhs.value_;
    return *this;
  }
  Float &operator*=(Float rhs) & {
    value_ *= rhs.value_;
    return *this;
  }
  Float &operator/=(Float rhs) &{
    value_ /= rhs.value_;
    return *this;
  }
  Float &operator%=(Float rhs) &{
    value_ = std::fmod(value_, rhs.value_);
    return *this;
  }

  // -- Comparison operators --

  constexpr Bool operator==(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ == rhs.value_);
  }
  constexpr Bool operator!=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ != rhs.value_);
  }
  constexpr Bool operator<(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ < rhs.value_);
  }
  constexpr Bool operator>(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ > rhs.value_);
  }
  constexpr Bool operator<=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ <= rhs.value_);
  }
  constexpr Bool operator>=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(value_ >= rhs.value_);
  }

  static constexpr Float min() {
    return Float(min_value());
  }
  static constexpr Float max() {
    return Float(max_value());
  }
  static constexpr Float infinity() {
    return Float(infinity_value());
  }
  static constexpr Float na() {
    return Float(NA());
  }

  static constexpr double min_value() {
    return std::numeric_limits<double>::min();
  }
  static constexpr double max_value() {
    return std::numeric_limits<double>::max();
  }
  static constexpr double infinity_value() {
    return std::numeric_limits<double>::infinity();
  }
  static constexpr double na_value() {
    return std::numeric_limits<double>::quiet_NaN();
  }

 private:
  double value_;
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP
