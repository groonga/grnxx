#ifndef GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP
#define GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP

#include <cmath>
#include <limits>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"

namespace grnxx {

class Int;

// NOTE: This implementation assumes IEEE 754.
class Float {
 public:
  Float() = default;
  ~Float() = default;

  constexpr Float(const Float &) = default;
  Float &operator=(const Float &) = default;

  // TODO: +/-0.0 and NaN may be normalized.
  //       -0.0 to +0.0 and various NaN to the quiet NaN.
  explicit constexpr Float(double raw) : raw_(raw) {}
  explicit constexpr Float(NA) : raw_(raw_na()) {}

  constexpr double raw() const {
    return raw_;
  }

  constexpr bool is_min() const {
    return raw_ == raw_min();
  }
  constexpr bool is_max() const {
    return raw_ == raw_max();
  }
  constexpr bool is_finite() const {
    return std::isfinite(raw_);
  }
  constexpr bool is_infinite() const {
    return std::isinf(raw_);
  }
  constexpr bool is_na() const {
    return std::isnan(raw_);
  }

  uint64_t hash() const {
    double normalized_raw = (raw_ != 0.0) ? raw_ : 0.0;
    uint64_t x;
    std::memcpy(&x, &normalized_raw, sizeof(x));
    x ^= x >> 33;
    x *= uint64_t(0xFF51AFD7ED558CCDULL);
    x ^= x >> 33;
    x *= uint64_t(0xC4CEB9FE1A85EC53ULL);
    x ^= x >> 33;
    return x;
  }

  // -- Unary operators --

  constexpr Float operator+() const {
    return *this;
  }
  constexpr Float operator-() const {
    return Float(-raw_);
  }

  // -- Binary operators --

  // -- Arithmetic operators --

  constexpr Float operator+(Float rhs) const {
    return Float(raw_ + rhs.raw_);
  }
  constexpr Float operator-(Float rhs) const {
    return Float(raw_ - rhs.raw_);
  }
  constexpr Float operator*(Float rhs) const {
    return Float(raw_ * rhs.raw_);
  };
  constexpr Float operator/(Float rhs) const {
    return Float(raw_ / rhs.raw_);
  }
  Float operator%(Float rhs) const {
    return Float(std::fmod(raw_, rhs.raw_));
  }

  Float &operator+=(Float rhs) & {
    raw_ += rhs.raw_;
    return *this;
  }
  Float &operator-=(Float rhs) & {
    raw_ -= rhs.raw_;
    return *this;
  }
  Float &operator*=(Float rhs) & {
    raw_ *= rhs.raw_;
    return *this;
  }
  Float &operator/=(Float rhs) &{
    raw_ /= rhs.raw_;
    return *this;
  }
  Float &operator%=(Float rhs) &{
    raw_ = std::fmod(raw_, rhs.raw_);
    return *this;
  }

  // -- Comparison operators --

  constexpr Bool operator==(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ == rhs.raw_);
  }
  constexpr Bool operator!=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ != rhs.raw_);
  }
  constexpr Bool operator<(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ < rhs.raw_);
  }
  constexpr Bool operator>(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ > rhs.raw_);
  }
  constexpr Bool operator<=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ <= rhs.raw_);
  }
  constexpr Bool operator>=(Float rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() : Bool(raw_ >= rhs.raw_);
  }

  constexpr bool match(Float rhs) const {
    return (is_na() && rhs.is_na()) || (raw_ == rhs.raw_);
  }
  constexpr bool unmatch(Float rhs) const {
    return !(is_na() && rhs.is_na()) && (raw_ != rhs.raw_);
  }

  // Return the next representable toward "to".
  Float next_toward(Float to) const {
    return Float(std::nextafter(raw_, to.raw_));
  }

  // -- Typecast (grnxx/data_types/typecast.hpp) --

  constexpr Int to_int() const;

  static constexpr DataType type() {
    return GRNXX_FLOAT;
  }

  static constexpr Float min() {
    return Float(raw_min());
  }
  static constexpr Float max() {
    return Float(raw_max());
  }
  static constexpr Float normal_min() {
    return Float(raw_normal_min());
  }
  static constexpr Float subnormal_min() {
    return Float(raw_subnormal_min());
  }
  static constexpr Float infinity() {
    return Float(raw_infinity());
  }
  static constexpr Float na() {
    return Float(NA());
  }

  static constexpr double raw_min() {
    return std::numeric_limits<double>::lowest();
  }
  static constexpr double raw_max() {
    return std::numeric_limits<double>::max();
  }
  static constexpr double raw_normal_min() {
    return std::numeric_limits<double>::min();
  }
  static constexpr double raw_subnormal_min() {
    return std::numeric_limits<double>::denorm_min();
  }
  static constexpr double raw_infinity() {
    return std::numeric_limits<double>::infinity();
  }
  static constexpr double raw_na() {
    return std::numeric_limits<double>::quiet_NaN();
  }

 private:
  double raw_;
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_FLOAT_HPP
