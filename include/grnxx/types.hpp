#ifndef GRNXX_TYPES_HPP
#define GRNXX_TYPES_HPP

#include <cinttypes>
#include <cstring>
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

// Smart pointer type.
using std::unique_ptr;

// An object to make a memory allocation (new) returns nullptr on failure.
using std::nothrow;

// Reference to a byte string.
class String {
 public:
  // The default constructor does nothing.
  String() = default;

  // Refer to a zero-terminated string.
  String(const char *str) : data_(str), size_(str ? std::strlen(str) : 0) {}

  // Refer to an arbitrary byte string.
  String(const char *data, size_t size) : data_(data), size_(size) {}

  const char &operator[](size_t i) const {
    return data_[i];
  }

  const char *data() const {
    return data_;
  }
  size_t size() const {
    return size_;
  }

 private:
  const char *data_;
  size_t size_;
};

inline bool operator==(String lhs, String rhs) {
  return (lhs.size() == rhs.size()) &&
         (std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0);
}

inline bool operator!=(String lhs, String rhs) {
  return (lhs.size() != rhs.size()) ||
         (std::memcmp(lhs.data(), rhs.data(), lhs.size()) != 0);
}

inline bool operator<(String lhs, String rhs) {
  size_t min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() < rhs.size()));
}

inline bool operator>(String lhs, String rhs) {
  size_t min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() > rhs.size()));
}

inline bool operator<=(String lhs, String rhs) {
  size_t min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() <= rhs.size()));
}

inline bool operator>=(String lhs, String rhs) {
  size_t min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() >= rhs.size()));
}

// Error information.
class Error;

// Database object types.
class DB;
class Table;
class Column;
class Index;

// Database option types.
struct DBOptions;
struct TableOptions;
struct ColumnOptions;
struct IndexOptions;

// Database built-in data types.
enum DataType {
  INVALID_DATA,

  // Type: Boolean.
  // Value: True or false.
  // Default: false.
  BOOL_DATA,
  // Type: Integer.
  // Value: 64-bit signed integer.
  // Default: 0.
  INT_DATA,
  // Type: Floating point number.
  // Value: Double precision (64-bit) floating point number.
  // Default: 0.0.
  FLOAT_DATA,
  // TODO
  // Type: Time.
  // Value: Microseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
  // Default: The Unix epoch.
  TIME_DATA,
  // TODO
  // Type: GeoPoint.
  // Value: Latitude-longitude in milliseconds.
  // Default: (0, 0).
  GEO_POINT_DATA,
  // Type: String.
  // Value: Byte string.
  // Default: "".
  TEXT_DATA,
  // Type: Reference.
  // Value: Reference to a row.
  // Default: NULL.
  ROW_REF_DATA,

  // TODO
  // Type: Vector.
  // Value: Vector of above data types.
  // Default: {}.
  BOOL_VECTOR_DATA,
  INT_VECTOR_DATA,
  FLOAT_VECTOR_DATA,
  TIME_VECTOR_DATA,
  GEO_POINT_VECTOR_DATA,
  TEXT_VECTOR_DATA,
  ROW_REF_VECTOR_DATA
};

using Bool  = bool;

using Int   = int64_t;

using Float = double;

class Time {
 public:
  // The default constructor does nothing.
  Time() = default;

  explicit Time(Int tick_count) : tick_count_(tick_count) {}

  void set_tick_count(Int tick_count) {
    tick_count_ = tick_count;
  }

  Int tick_count() const {
    return tick_count_;
  }

 private:
  Int tick_count_;  // Microseconds since the Unix epoch
                    // (1970-01-01 00:00:00 UTC).
};

inline bool operator==(Time lhs, Time rhs) {
  return lhs.tick_count() == rhs.tick_count();
}

inline bool operator!=(Time lhs, Time rhs) {
  return lhs.tick_count() != rhs.tick_count();
}

inline bool operator<(Time lhs, Time rhs) {
  return lhs.tick_count() < rhs.tick_count();
}

inline bool operator>(Time lhs, Time rhs) {
  return lhs.tick_count() > rhs.tick_count();
}

inline bool operator<=(Time lhs, Time rhs) {
  return lhs.tick_count() <= rhs.tick_count();
}

inline bool operator>=(Time lhs, Time rhs) {
  return lhs.tick_count() >= rhs.tick_count();
}

class GeoPoint {
 public:
  // The default constructor does nothing.
  GeoPoint() = default;

  // The upper 32 bits of "latitude" and "longitude" are ignored.
  GeoPoint(Int latitude, Int longitude)
      : latitude_(),
        longitude_() {
    if ((latitude < DEGREES(-90)) || (latitude > DEGREES(90)) ||
        (longitude < DEGREES(-180)) || (longitude >= DEGREES(180))) {
      // Fix out-of-range values.
      fix(&latitude, &longitude);
    }
    // The south pole or the north pole.
    if ((latitude == DEGREES(-90)) || (latitude == DEGREES(90))) {
      longitude = 0;
    }
    latitude_ = static_cast<int32_t>(latitude);
    longitude_ = static_cast<int32_t>(longitude);
  }

  Int latitude() const {
    return latitude_;
  }
  Int longitude() const {
    return longitude_;
  }

 private:
  int32_t latitude_;  // Latitude in milliseconds.
  int32_t longitude_;  // Longitude in milliseconds.

  static constexpr Int DEGREES(Int value) {
    return value * 60 * 60 * 1000;
  }

  static void fix(Int *latitude, Int *longitude);
};

inline bool operator==(GeoPoint lhs, GeoPoint rhs) {
  return (lhs.latitude() == rhs.latitude()) &&
         (lhs.longitude() == rhs.longitude());
}

inline bool operator!=(GeoPoint lhs, GeoPoint rhs) {
  return (lhs.latitude() != rhs.latitude()) ||
         (lhs.longitude() != rhs.longitude());
}

using Text = String;

class RowRef {
 public:
  // The default constructor does nothing.
  RowRef() = default;

  // Set the table and the row ID.
  RowRef(Table *table, Int row_id) : table_(table), row_id_(row_id) {}

  // Set the table.
  void set_table(Table *table) {
    table_ = table;
  }
  // Set the row ID.
  void set_row_id(Int row_id) {
    row_id_ = row_id;
  }

  // Return the table.
  Table *table() const {
    return table_;
  }
  // Return the row ID.
  Int row_id() const {
    return row_id_;
  }

 private:
  Table *table_;  // Target table.
  Int row_id_;  // Row ID number.
};

// Zero is reserved for representing a null reference.
constexpr Int NULL_ROW_ID = 0;
constexpr Int MIN_ROW_ID  = 1;
constexpr Int MAX_ROW_ID  = (Int(1) << 40) - 1;

// TODO
struct CursorOptions;

class Datum;
class Cursor;
class RecordSet;

enum IndexType {
  TREE_INDEX,
  HASH_INDEX
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP