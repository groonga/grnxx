#ifndef GRNXX_NEW_TYPES_BOOL_HPP
#define GRNXX_NEW_TYPES_BOOL_HPP

#include <cstdint>

#include "grnxx/new_types/na.hpp"

namespace grnxx {

class Bool {
 public:
  Bool() = default;
  ~Bool() = default;

  constexpr Bool(const Bool &) = default;
  Bool &operator=(const Bool &) & = default;

  explicit constexpr Bool(bool value) : value_(value) {}
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
    return is_na() ? na() : Bool(!value_);
  }
  constexpr Bool operator~() const {
    return operator!();
  }

  // -- Binary operators --

  // TODO: Difficult branch predictions (for is_false()) should be removed.
  constexpr Bool operator&&(Bool rhs) const {
    return (is_false() || rhs.is_false()) ? Bool(false) :
           ((is_na() || rhs.is_na()) ? na() : Bool(true));
  }
  constexpr Bool operator||(Bool rhs) const {
    return (is_true() || rhs.is_true()) ? Bool(true) :
           ((is_na() || rhs.is_na()) ? na() : Bool(false));
  }

  constexpr Bool operator&(Bool rhs) const {
    return operator&&(rhs);
  }
  constexpr Bool operator|(Bool rhs) const {
    return operator||(rhs);
  }
  constexpr Bool operator^(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Bool(value_ ^ rhs.value_);
  }

  Bool &operator&=(Bool rhs) & {
    return *this = operator&(rhs);
  }
  Bool operator|=(Bool rhs) & {
    return *this = operator|(rhs);
  }
  Bool operator^=(Bool rhs) & {
    return *this = operator^(rhs);
  }

  // -- Comparison operators --

  constexpr Bool operator==(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Bool(value_ == rhs.value_);
  }
  constexpr Bool operator!=(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() : Bool(value_ != rhs.value_);
  }

  static constexpr Bool na() {
    return Bool(NA());
  }

  static constexpr uint8_t true_value() {
    return 1;
  }
  static constexpr uint8_t false_value() {
    return 0;
  }
  static constexpr uint8_t na_value() {
    return 2;
  }

 private:
  uint8_t value_;
};

}  // namespace grnxx

#endif  // GRNXX_NEW_TYPES_BOOL_HPP
