#ifndef GRNXX_IMPL_COLUMN_SCALAR_BOOL_HPP
#define GRNXX_IMPL_COLUMN_SCALAR_BOOL_HPP

#include "grnxx/impl/column/base.hpp"

namespace grnxx {
namespace impl {

template <typename T> class Column;

template <>
class Column<Bool> : public ColumnBase {
 public:
  // -- Public API (grnxx/column.hpp) --

  Column(Table *table, const String &name, const ColumnOptions &options);
  ~Column();

  void set(Int row_id, const Datum &datum);
  void get(Int row_id, Datum *datum) const;

  bool contains(const Datum &datum) const;
  Int find_one(const Datum &datum) const;

  // -- Internal API (grnxx/impl/column/base.hpp) --

  // Create a new column.
  //
  // On success, returns the column.
  // On failure, throws an exception.
  static std::unique_ptr<ColumnBase> create(
      Table *table,
      const String &name,
      DataType data_type,
      const ColumnOptions &options);

  // Unset the value.
  void unset(Int row_id);

  // -- Internal API --

  // Return a value.
  //
  // If "row_id" is valid, returns the stored value.
  // If "row_id" is invalid, returns N/A.
  Bool get(Int row_id) const {
    size_t value_id = row_id.value();
    if (value_id >= values_.size()) {
      return Bool::na();
    }
    return values_[value_id];
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Bool> values) const;

 protected:
  Array<Bool> values_;

  static Bool parse_datum(const Datum &datum);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_SCALAR_BOOL_HPP
