#ifndef GRNXX_DATA_TYPES_SCALAR_BOOL_HPP
#define GRNXX_DATA_TYPES_SCALAR_BOOL_HPP

#include <cstdint>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"

namespace grnxx {

class Bool {
 public:
  Bool() = default;
  ~Bool() = default;

  constexpr Bool(const Bool &) = default;
  Bool &operator=(const Bool &) & = default;

  explicit constexpr Bool(bool value)
      : value_(value ? true_value() : false_value()) {}
  explicit constexpr Bool(NA) : value_(na_value()) {}

  constexpr uint8_t value() const {
    return value_;
  }

  constexpr bool is_true() const {
    return value_ == true_value();
  }
  constexpr bool is_false() const {
    return value_ == false_value();
  }
  constexpr bool is_na() const {
    return value_ == na_value();
  }

  explicit constexpr operator bool() const {
    return is_true();
  }

  // -- Unary operators --

  constexpr Bool operator!() const {
    return is_na() ? na() : Bool(static_cast<uint8_t>(value_ ^ true_value()));
  }
  constexpr Bool operator~() const {
    return operator!();
  }

  // -- Binary operators --

  constexpr Bool operator&(Bool rhs) const {
    return Bool(static_cast<uint8_t>(value_ & rhs.value_));
  }
  constexpr Bool operator|(Bool rhs) const {
    return Bool(static_cast<uint8_t>(value_ | rhs.value_));
  }
  constexpr Bool operator^(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(value_ ^ rhs.value_));
  }

  Bool &operator&=(Bool rhs) & {
    value_ &= rhs.value_;
    return *this;
  }
  Bool &operator|=(Bool rhs) & {
    value_ |= rhs.value_;
    return *this;
  }
  Bool &operator^=(Bool rhs) & {
    if (!is_na()) {
      value_ = rhs.is_na() ? na_value() : (value_ ^ rhs.value_);
    }
    return *this;
  }

  // -- Comparison operators --

  constexpr Bool operator==(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(value_ ^ rhs.value_ ^ true_value()));
  }
  constexpr Bool operator!=(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(value_ ^ rhs.value_));
  }

  static constexpr DataType type() {
    return BOOL_DATA;
  }

  static constexpr Bool na() {
    return Bool(NA());
  }

  static constexpr uint8_t true_value() {
    return 0b11;
  }
  static constexpr uint8_t false_value() {
    return 0b00;
  }
  static constexpr uint8_t na_value() {
    return 0b01;
  }

 private:
  uint8_t value_;

  explicit constexpr Bool(uint8_t value) : value_(value) {}
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_BOOL_HPP
