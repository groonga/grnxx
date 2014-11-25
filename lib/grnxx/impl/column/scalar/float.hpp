#ifndef GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP
#define GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP

#include "grnxx/impl/column/base.hpp"

namespace grnxx {
namespace impl {

template <typename T> class Column;

template <>
class Column<Float> : public ColumnBase {
 public:
  // -- Public API (grnxx/column.hpp) --

  Column(Table *table, const String &name, const ColumnOptions &options);
  ~Column();

  void set(Int row_id, const Datum &datum);
  void get(Int row_id, Datum *datum) const;

  bool contains(const Datum &datum) const;
  Int find_one(const Datum &datum) const;

  // -- Internal API (grnxx/impl/column/base.hpp) --

  // Unset the value.
  void unset(Int row_id);

  // -- Internal API --

  // Return a value.
  //
  // If "row_id" is valid, returns the stored value.
  // If "row_id" is invalid, returns N/A.
  Float get(Int row_id) const {
    size_t value_id = row_id.value();
    if (value_id >= values_.size()) {
      return Float::na();
    }
    return values_[value_id];
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Float> values) const;

 private:
  Array<Float> values_;

  // Return the active column size.
  size_t get_valid_size() const;

  // Parse "datum" as Float.
  //
  // On success, returns the result.
  // On failure, throws an exception.
  static Float parse_datum(const Datum &datum);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP
