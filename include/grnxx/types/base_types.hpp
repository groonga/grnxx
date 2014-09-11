#ifndef GRNXX_TYPES_BASE_TYPES_HPP
#define GRNXX_TYPES_BASE_TYPES_HPP

#include <cinttypes>
#include <limits>
#include <memory>

namespace grnxx {

// Fixed-width signed integer types.
using std::int8_t;
using std::int16_t;
using std::int32_t;
using std::int64_t;

// Fixed-width unsigned integer types.
using std::uint8_t;
using std::uint16_t;
using std::uint32_t;
using std::uint64_t;

// Note that PRI*8/16/32/64 are available as format macro constants.
// For example, "%" PRIi64 is used to print a 64-bit signed integer.
// Also, "%" PRIu64 is used to print a 64-bit unsigned integer.

// Integer type for representing offset and size.
using std::size_t;

// Limitations.
using std::numeric_limits;

// Smart pointer type.
using std::unique_ptr;

// An object to make a memory allocation (new) returns nullptr on failure.
using std::nothrow;

// Built-in data types.
using Bool  = bool;
using Int   = int64_t;
using UInt  = uint64_t;
using Float = double;

}  // namespace grnxx

#endif  // GRNXX_TYPES_BASE_TYPES_HPP
