#ifndef GRNXX_IMPL_COLUMN_VECTOR_INT_HPP
#define GRNXX_IMPL_COLUMN_VECTOR_INT_HPP

#include <limits>
#include <cstdint>

#include "grnxx/impl/column/base.hpp"

namespace grnxx {
namespace impl {

template <typename T> class Column;

template <>
class Column<Vector<Int>> : public ColumnBase {
 public:
  // -- Public API (grnxx/column.hpp) --

  Column(Table *table, const String &name, const ColumnOptions &options);
  ~Column();

  void set(Int row_id, const Datum &datum);
  void get(Int row_id, Datum *datum) const;

  bool contains(const Datum &datum) const;
  Int find_one(const Datum &datum) const;

  // -- Internal API (grnxx/impl/column/base.hpp) --

  void unset(Int row_id);

  // -- Internal API --

  // Return a value.
  //
  // If "row_id" is valid, returns the stored value.
  // If "row_id" is invalid, returns N/A.
  //
  // TODO: Vector cannot reuse allocated memory because of this interface.
  Vector<Int> get(Int row_id) const {
    size_t value_id = row_id.value();
    if (value_id >= headers_.size()) {
      return Vector<Int>::na();
    }
    if (headers_[value_id] == na_header()) {
      return Vector<Int>::na();
    }
    size_t size = headers_[value_id] & 0xFFFF;
    if (size == 0) {
      return Vector<Int>(nullptr, 0);
    }
    size_t offset = headers_[value_id] >> 16;
    if (size < 0xFFFF) {
      return Vector<Int>(&bodies_[offset], size);
    } else {
      // The size of a long vector is stored in front of the body.
      size = *reinterpret_cast<const uint64_t *>(&bodies_[offset]);
      return Vector<Int>(&bodies_[offset + 1], size);
    }
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Vector<Int>> values) const;

 private:
  Array<uint64_t> headers_;
  Array<Int> bodies_;

  static constexpr uint64_t na_header() {
    return std::numeric_limits<uint64_t>::max();
  }

  static Vector<Int> parse_datum(const Datum &datum);
};

//// TODO
//template <>
//class Column<Vector<Int>> : public ColumnBase {
// public:
//  // -- Public API --

//  bool set(Error *error, Int row_id, const Datum &datum);
//  bool get(Error *error, Int row_id, Datum *datum) const;

//  // -- Internal API --

//  // Create a new column.
//  //
//  // Returns a pointer to the column on success.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  static unique_ptr<Column> create(Error *error,
//                                   Table *table,
//                                   const StringCRef &name,
//                                   const ColumnOptions &options);

//  ~Column();

//  bool set_default_value(Error *error, Int row_id);
//  void unset(Int row_id);

//  void clear_references(Int row_id);

//  // Return a value identified by "row_id".
//  //
//  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
//  Vector<Int> get(Int row_id) const {
//    Int size = static_cast<Int>(headers_[row_id] & 0xFFFF);
//    if (size == 0) {
//      return Vector<Int>(nullptr, 0);
//    }
//    Int offset = static_cast<Int>(headers_[row_id] >> 16);
//    if (size < 0xFFFF) {
//      return Vector<Int>(&bodies_[offset], size);
//    } else {
//      // The size of a long vector is stored in front of the body.
//      size = bodies_[offset];
//      return Vector<Int>(&bodies_[offset + 1], size);
//    }
//  }

//  // Read values.
//  void read(ArrayCRef<Record> records, ArrayRef<Vector<Int>> values) const {
//    for (Int i = 0; i < records.size(); ++i) {
//      values.set(i, get(records.get_row_id(i)));
//    }
//  }

// private:
//  Array<UInt> headers_;
//  Array<Int> bodies_;

//  Column();
//};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_VECTOR_INT_HPP
