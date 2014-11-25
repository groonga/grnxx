#ifndef GRNXX_DATA_TYPES_TYPECAST_HPP
#define GRNXX_DATA_TYPES_TYPECAST_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar.hpp"
#include "grnxx/data_types/vector.hpp"

namespace grnxx {

inline constexpr Float Int::to_float() const {
  return is_na() ? Float::na() : Float(static_cast<double>(raw_));
}

// NOTE: This implementation assumes that an integer overflow in conversion
//       from double-precision floating-point number to 64-bit signed integer
//       results in the value associated with N/A (0x80...).
//       The CVTTSD2SI instruction of x86_64 works as mentioned.
inline constexpr Int Float::to_int() const {
  return Int(static_cast<int64_t>(value_));
}

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_TYPECAST_HPP
