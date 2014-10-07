#ifndef GRNXX_IMPL_COLUMN_COLUMN_TEXT_HPP
#define GRNXX_IMPL_COLUMN_COLUMN_TEXT_HPP

#include "grnxx/impl/column/column.hpp"

namespace grnxx {
namespace impl {

template <>
class Column<Text> : public impl::ColumnBase {
 public:
  // -- Public API --

  bool set(Error *error, Int row_id, const Datum &datum);
  bool get(Error *error, Int row_id, Datum *datum) const;

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

  ~Column();

  bool set_key_attribute(Error *error);
  bool unset_key_attribute(Error *error);

  bool set_initial_key(Error *error, Int row_id, const Datum &key);
  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);
  Int find_one(const Datum &datum) const;

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Text get(Int row_id) const {
    Int size = static_cast<Int>(headers_[row_id] & 0xFFFF);
    if (size == 0) {
      return Text("", 0);
    }
    Int offset = static_cast<Int>(headers_[row_id] >> 16);
    if (size < 0xFFFF) {
      return Text(&bodies_[offset], size);
    } else {
      // The size of a long text is stored in front of the body.
      size = *reinterpret_cast<const Int *>(&bodies_[offset]);
      return StringCRef(&bodies_[offset + sizeof(Int)], size);
    }
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Text> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<UInt> headers_;
  Array<char> bodies_;

  Column();
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_COLUMN_TEXT_HPP
