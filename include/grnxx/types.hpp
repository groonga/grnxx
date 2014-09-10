#ifndef GRNXX_TYPES_HPP
#define GRNXX_TYPES_HPP

#include <cinttypes>
#include <cstring>
#include <initializer_list>
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

// Error information.
class Error;

// Database persistent object types.
class DB;
class Table;
class Column;
class Index;

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
  // Type: GeoPoint.
  // Value: Latitude-longitude in milliseconds.
  // Default: (0, 0).
  GEO_POINT_DATA,
  // Type: String.
  // Value: Byte string.
  // Default: "".
  TEXT_DATA,

  // TODO: Not supported yet.
  //       Data types for I/O are required.
  //       BOOL_VECTOR and TEXT_VECTOR are the problem.
  //
  // Type: Vector.
  // Value: Vector of above data types.
  // Default: {}.
  BOOL_VECTOR_DATA,
  INT_VECTOR_DATA,
  FLOAT_VECTOR_DATA,
  GEO_POINT_VECTOR_DATA,
  TEXT_VECTOR_DATA,
};

using Bool  = bool;

using Int   = int64_t;

using Float = double;

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

// Reference to a byte string.
class String {
 public:
  // The default constructor does nothing.
  String() = default;

  // Refer to a zero-terminated string.
  String(const char *str) : data_(str), size_(str ? std::strlen(str) : 0) {}

  // Refer to an arbitrary byte string.
  String(const char *data, Int size) : data_(data), size_(size) {}

  const char &operator[](Int i) const {
    return data_[i];
  }

  const char *data() const {
    return data_;
  }
  Int size() const {
    return size_;
  }

 private:
  const char *data_;
  Int size_;
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
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() < rhs.size()));
}

inline bool operator>(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() > rhs.size()));
}

inline bool operator<=(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result < 0) || ((result == 0) && (lhs.size() <= rhs.size()));
}

inline bool operator>=(String lhs, String rhs) {
  Int min_size = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
  int result = std::memcmp(lhs.data(), rhs.data(), min_size);
  return (result > 0) || ((result == 0) && (lhs.size() >= rhs.size()));
}

using Text = String;

template <typename T> class Vector;

// A Vector<Bool> contains at most 58 Bool values.
template <>
class Vector<Bool> {
 public:
  Vector() = default;
  Vector(std::initializer_list<Bool> bits) : data_(0) {
    uint64_t size = static_cast<uint64_t>(bits.size());
    if (size > 58) {
      size = 58;
    }
    uint64_t i = 0;
    for (auto it = bits.begin(); it != bits.end(); ++it) {
      if (*it) {
        data_ |= uint64_t(1) << i;
      }
      ++i;
    }
    data_ |= size << 58;
  }
  Vector(uint64_t bits, Int size)
      : data_((bits & mask(size)) |
              (static_cast<uint64_t>(std::min(size, Int(58))) << 58)) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Bool values.
  Int size() const {
    return static_cast<Int>(data_ >> 58);
  }
  // Return the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  Bool get(Int i) const {
    return (data_ & (uint64_t(1) << i)) != 0;
  }
  // Set the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  void set(Int i, Bool value) {
    if (value) {
      data_ |= uint64_t(1) << i;
    } else {
      data_ &= ~(uint64_t(1) << i);
    }
  }

  // Return the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  Bool operator[](Int i) const {
    return get(i);
  }

  // Return the set of Bool values.
  uint64_t bits() const {
    return data_ & mask(58);
  }

 private:
  uint64_t data_;

  static uint64_t mask(Int size) {
    return (uint64_t(1) << size) - 1;
  }
};

inline Bool operator==(Vector<Bool> lhs, Vector<Bool> rhs) {
  return (lhs.size() == rhs.size()) &&
         ((lhs.bits() ^ rhs.bits()) == 0);
}
inline Bool operator!=(Vector<Bool> lhs, Vector<Bool> rhs) {
  return (lhs.size() == rhs.size()) ||
         ((lhs.bits() ^ rhs.bits()) != 0);
}

template <>
class Vector<Int> {
 public:
  Vector() = default;
  Vector(const Int *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Int values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th Int value.
  //
  // If "i" is invalid, the result is undefined.
  Int get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th Int value.
  //
  // If "i" is invalid, the result is undefined.
  Int operator[](Int i) const {
    return get(i);
  }

 private:
  const Int *data_;
  Int size_;
};

inline Bool operator==(Vector<Int> lhs, Vector<Int> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Int> lhs, Vector<Int> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

template <>
class Vector<Float> {
 public:
  Vector() = default;
  Vector(const Float *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Float values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th Float value.
  //
  // If "i" is invalid, the result is undefined.
  Float get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th Float value.
  //
  // If "i" is invalid, the result is undefined.
  Float operator[](Int i) const {
    return get(i);
  }

 private:
  const Float *data_;
  Int size_;
};

inline Bool operator==(Vector<Float> lhs, Vector<Float> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Float> lhs, Vector<Float> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

template <>
class Vector<GeoPoint> {
 public:
  Vector() = default;
  Vector(const GeoPoint *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of GeoPoint values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th GeoPoint value.
  //
  // If "i" is invalid, the result is undefined.
  GeoPoint get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th GeoPoint value.
  //
  // If "i" is invalid, the result is undefined.
  GeoPoint operator[](Int i) const {
    return get(i);
  }

 private:
  const GeoPoint *data_;
  Int size_;
};

inline Bool operator==(Vector<GeoPoint> lhs, Vector<GeoPoint> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<GeoPoint> lhs, Vector<GeoPoint> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

// TODO: Improve the implementation of Vector<Text>.
template <>
class Vector<Text> {
 public:
  Vector() = default;
  Vector(const Text *data, Int size)
      : is_direct_(1),
        size_(size),
        data_(data) {}
  Vector(const void *headers, const char *bodies, Int size)
      : is_direct_(0),
        size_(size),
        headers_(static_cast<const Header *>(headers)),
        bodies_(bodies) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Text values.
  Int size() const {
    return static_cast<Int>(size_);
  }
  // Return the "i"-th Text value.
  //
  // If "i" is invalid, the result is undefined.
  Text get(Int i) const {
    if (is_direct_) {
      return data_[i];
    } else {
      return Text(&bodies_[headers_[i].offset], headers_[i].size);
    }
  }

  // Return the "i"-th Text value.
  //
  // If "i" is invalid, the result is undefined.
  Text operator[](Int i) const {
    return get(i);
  }

 private:
  struct Header {
    Int offset;
    Int size;
  };
  bool is_direct_;
  Int size_;
  union {
    const Text *data_;
    struct {
      const Header *headers_;
      const char *bodies_;
    };
  };
};

inline Bool operator==(Vector<Text> lhs, Vector<Text> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Text> lhs, Vector<Text> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

using BoolVector     = Vector<Bool>;
using IntVector      = Vector<Int>;
using FloatVector    = Vector<Float>;
using GeoPointVector = Vector<GeoPoint>;
using TextVector     = Vector<Text>;

// Type information.
template <typename T> struct TypeTraits;
template <> struct TypeTraits <Bool> {
  static DataType data_type() {
    return BOOL_DATA;
  }
  static Bool default_value() {
    return false;
  }
};
template <> struct TypeTraits <Int> {
  static DataType data_type() {
    return INT_DATA;
  }
  static Int default_value() {
    return 0;
  }
};
template <> struct TypeTraits <Float> {
  static DataType data_type() {
    return FLOAT_DATA;
  }
  static Float default_value() {
    return 0.0;
  }
};
template <> struct TypeTraits <GeoPoint> {
  static DataType data_type() {
    return GEO_POINT_DATA;
  }
  static GeoPoint default_value() {
    return GeoPoint(0, 0);
  }
};
template <> struct TypeTraits <Text> {
  static DataType data_type() {
    return TEXT_DATA;
  }
  static Text default_value() {
    return Text("", 0);
  }
};
template <> struct TypeTraits <Vector<Bool>> {
  static DataType data_type() {
    return BOOL_VECTOR_DATA;
  }
  static Vector<Bool> default_value() {
    return Vector<Bool>(0, 0);
  }
};
template <> struct TypeTraits <Vector<Int>> {
  static DataType data_type() {
    return INT_VECTOR_DATA;
  }
  static Vector<Int> default_value() {
    return Vector<Int>(nullptr, 0);
  }
};
template <> struct TypeTraits <Vector<Float>> {
  static DataType data_type() {
    return FLOAT_VECTOR_DATA;
  }
  static Vector<Float> default_value() {
    return Vector<Float>(nullptr, 0);
  }
};
template <> struct TypeTraits <Vector<GeoPoint>> {
  static DataType data_type() {
    return GEO_POINT_VECTOR_DATA;
  }
  static Vector<GeoPoint> default_value() {
    return Vector<GeoPoint>(nullptr, 0);
  }
};
template <> struct TypeTraits <Vector<Text>> {
  static DataType data_type() {
    return TEXT_VECTOR_DATA;
  }
  static Vector<Text> default_value() {
    return Vector<Text>(nullptr, 0);
  }
};

// Zero is reserved for representing a null reference.
constexpr Int NULL_ROW_ID = 0;
constexpr Int MIN_ROW_ID  = 1;
constexpr Int MAX_ROW_ID  = (Int(1) << 40) - 1;

struct Record {
  Int row_id;
  Float score;

  Record() = default;
  Record(Int row_id, Float score) : row_id(row_id), score(score) {}

  Record &operator=(const Record &) & = default;
};

enum IndexType {
  TREE_INDEX,
  HASH_INDEX
};

enum OrderType {
  // The natural order.
  REGULAR_ORDER,

  // The reverse order of REGULAR_ORDER.
  REVERSE_ORDER
};

// Database persistent object option types.
struct DBOptions {
  DBOptions();
};

struct TableOptions {
  TableOptions();
};

struct ColumnOptions {
  // The referenced (parent) table.
  String ref_table_name;

  ColumnOptions();
};

struct IndexOptions {
  IndexOptions();
};

struct CursorOptions {
  // The first "offset" records are skipped (default: 0).
  Int offset;

  // At most "limit" records are read (default: numeric_limits<Int>::max()).
  Int limit;

  // The order of records (default: REGULAR_ORDER).
  OrderType order_type;

  CursorOptions();
};

struct ExpressionOptions {
  // Records are evaluated per block.
  Int block_size;

  ExpressionOptions();
};

struct SorterOptions {
  // The first "offset" records are skipped (default: 0).
  Int offset;

  // At most "limit" records are sorted (default: numeric_limits<Int>::max()).
  Int limit;

  SorterOptions();
};

struct PipelineOptions {
  PipelineOptions();
};

// Database temporary object types.
class Datum;
class Cursor;
class Expression;
class ExpressionBuilder;
class Sorter;

struct SortOrder {
  unique_ptr<Expression> expression;
  OrderType type;

  SortOrder();
  explicit SortOrder(unique_ptr<Expression> &&expression,
                     OrderType type = REGULAR_ORDER);
  SortOrder(SortOrder &&order);
  ~SortOrder();
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_HPP
