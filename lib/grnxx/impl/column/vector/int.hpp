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
    size_t value_id = row_id.raw();
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

  // Return the active column size.
  size_t get_valid_size() const;

  // Parse "datum" as Vector<Int>.
  //
  // On success, returns the result.
  // On failure, throws an exception.
  static Vector<Int> parse_datum(const Datum &datum);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_VECTOR_INT_HPP
