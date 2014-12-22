#ifndef GRNXX_IMPL_COLUMN_SCALAR_INT_HPP
#define GRNXX_IMPL_COLUMN_SCALAR_INT_HPP

#include "grnxx/impl/column/base.hpp"

namespace grnxx {
namespace impl {

template <typename T> class Column;

template <>
class Column<Int> : public ColumnBase {
 public:
  // -- Public API (grnxx/column.hpp) --

  Column(Table *table, const String &name, const ColumnOptions &options);
  ~Column();

  void set(Int row_id, const Datum &datum);
  void get(Int row_id, Datum *datum) const;

  bool contains(const Datum &datum) const;
  Int find_one(const Datum &datum) const;

  // -- Internal API (grnxx/impl/column/base.hpp) --

  void set_key_attribute();
  void unset_key_attribute();

  void set_key(Int row_id, const Datum &key);
  void unset(Int row_id);
  void clear_references(Int row_id);

  // -- Internal API --

  // Return a value.
  //
  // If "row_id" is valid, returns the stored value.
  // If "row_id" is invalid, returns N/A.
  Int get(Int row_id) const {
    size_t value_id = row_id.raw();
    if (value_id >= size_) {
      return Int::na();
    }
    return _get(value_id);
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Int> values) const;

 private:
  size_t value_size_;
  union {
    void *buffer_;
    int8_t *values_8_;
    int16_t *values_16_;
    int32_t *values_32_;
    Int *values_64_;
  };
  size_t size_;
  size_t capacity_;

  // Return the stored value.
  Int _get(size_t i) const {
    switch (value_size_) {
      case 8: {
        return (values_8_[i] != na_value_8()) ?
               Int(values_8_[i]) : Int::na();
      }
      case 16: {
        return (values_16_[i] != na_value_16()) ?
               Int(values_16_[i]) : Int::na();
      }
      case 32: {
        return (values_32_[i] != na_value_32()) ?
               Int(values_32_[i]) : Int::na();
      }
      default: {
        return values_64_[i];
      }
    }
  }

  // Return the active column size.
  size_t get_valid_size() const;

  // Reserve memory for a new value.
  void reserve(size_t size, Int value);

  // Parse "datum" as Int.
  //
  // On success, returns the result.
  // On failure, throws an exception.
  static Int parse_datum(const Datum &datum);

  static constexpr int8_t na_value_8() {
    return std::numeric_limits<int8_t>::min();
  }
  static constexpr int8_t min_value_8() {
    return std::numeric_limits<int8_t>::min() + 1;
  }
  static constexpr int8_t max_value_8() {
    return std::numeric_limits<int8_t>::max();
  }

  static constexpr int16_t na_value_16() {
    return std::numeric_limits<int16_t>::min();
  }
  static constexpr int16_t min_value_16() {
    return std::numeric_limits<int16_t>::min() + 1;
  }
  static constexpr int16_t max_value_16() {
    return std::numeric_limits<int16_t>::max();
  }

  static constexpr int32_t na_value_32() {
    return std::numeric_limits<int32_t>::min();
  }
  static constexpr int32_t min_value_32() {
    return std::numeric_limits<int32_t>::min() + 1;
  }
  static constexpr int32_t max_value_32() {
    return std::numeric_limits<int32_t>::max();
  }
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_SCALAR_INT_HPP
