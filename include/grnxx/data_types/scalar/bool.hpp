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
      : raw_(value ? raw_true() : raw_false()) {}
  explicit constexpr Bool(NA) : raw_(raw_na()) {}

  constexpr uint8_t raw() const {
    return raw_;
  }

  constexpr bool is_true() const {
    return raw_ == raw_true();
  }
  constexpr bool is_false() const {
    return raw_ == raw_false();
  }
  constexpr bool is_na() const {
    return raw_ == raw_na();
  }

  // -- Unary operators --

  constexpr Bool operator!() const {
    return is_na() ? na() : Bool(static_cast<uint8_t>(raw_ ^ raw_true()));
  }
  constexpr Bool operator~() const {
    return operator!();
  }

  // -- Binary operators --

  constexpr Bool operator&(Bool rhs) const {
    return Bool(static_cast<uint8_t>(raw_ & rhs.raw_));
  }
  constexpr Bool operator|(Bool rhs) const {
    return Bool(static_cast<uint8_t>(raw_ | rhs.raw_));
  }
  constexpr Bool operator^(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(raw_ ^ rhs.raw_));
  }

  Bool &operator&=(Bool rhs) & {
    raw_ &= rhs.raw_;
    return *this;
  }
  Bool &operator|=(Bool rhs) & {
    raw_ |= rhs.raw_;
    return *this;
  }
  Bool &operator^=(Bool rhs) & {
    if (!is_na()) {
      raw_ = rhs.is_na() ? raw_na() : (raw_ ^ rhs.raw_);
    }
    return *this;
  }

  // -- Comparison operators --

  constexpr Bool operator==(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(raw_ ^ rhs.raw_ ^ raw_true()));
  }
  constexpr Bool operator!=(Bool rhs) const {
    return (is_na() || rhs.is_na()) ? na() :
           Bool(static_cast<uint8_t>(raw_ ^ rhs.raw_));
  }

  constexpr bool match(Bool rhs) const {
    return raw_ == rhs.raw_;
  }
  constexpr bool unmatch(Bool rhs) const {
    return raw_ != rhs.raw_;
  }

  static constexpr DataType type() {
    return BOOL_DATA;
  }

  static constexpr Bool na() {
    return Bool(NA());
  }

  static constexpr uint8_t raw_true() {
    return 0b11;
  }
  static constexpr uint8_t raw_false() {
    return 0b00;
  }
  static constexpr uint8_t raw_na() {
    return 0b01;
  }

 private:
  uint8_t raw_;

  explicit constexpr Bool(uint8_t raw) : raw_(raw) {}
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_BOOL_HPP
