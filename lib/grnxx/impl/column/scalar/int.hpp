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
    if (value_id >= values_.size()) {
      return Int::na();
    }
    return values_[value_id];
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Int> values) const;

 private:
  Array<Int> values_;

  // Return the active column size.
  size_t get_valid_size() const;

  // Parse "datum" as Int.
  //
  // On success, returns the result.
  // On failure, throws an exception.
  static Int parse_datum(const Datum &datum);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_SCALAR_INT_HPP
