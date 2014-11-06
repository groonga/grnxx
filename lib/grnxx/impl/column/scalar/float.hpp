#ifndef GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP
#define GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP

#include "grnxx/impl/column/column.hpp"

namespace grnxx {
namespace impl {

template <>
class Column<Float> : public ColumnBase {
 public:
  // -- Public API (grnxx/column.hpp) --

  bool set(Error *error, Int row_id, const Datum &datum);
  bool get(Error *error, Int row_id, Datum *datum) const;

  // -- Internal API (grnxx/impl/column/column_base.hpp) --

  ~Column();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // -- Internal API --

  // Create a new column.
  //
  // Returns a pointer to the column on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Column> create(Error *error,
                                   Table *table,
                                   const StringCRef &name,
                                   const ColumnOptions &options);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Float get(Int row_id) const {
    return values_[row_id];
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Float> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<Float> values_;

  Column();
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_SCALAR_FLOAT_HPP
