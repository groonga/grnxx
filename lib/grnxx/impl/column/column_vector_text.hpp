#ifndef GRNXX_IMPL_COLUMN_COLUMN_VECTOR_TEXT_HPP
#define GRNXX_IMPL_COLUMN_COLUMN_VECTOR_TEXT_HPP

#include "grnxx/impl/column/column.hpp"

namespace grnxx {
namespace impl {

template <>
class Column<Vector<Text>> : public ColumnBase {
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

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Vector<Text> get(Int row_id) const {
    return Vector<Text>(&text_headers_[headers_[row_id].offset],
                        bodies_.data(), headers_[row_id].size);
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Vector<Text>> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  struct Header {
    Int offset;
    Int size;
  };
  Array<Header> headers_;
  Array<Header> text_headers_;
  Array<char> bodies_;

  Column();
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_COLUMN_VECTOR_TEXT_HPP
