#ifndef GRNXX_IMPL_COLUMN_VECTOR_TEXT_HPP
#define GRNXX_IMPL_COLUMN_VECTOR_TEXT_HPP

#include "grnxx/impl/column/base.hpp"

namespace grnxx {
namespace impl {

template <typename T> class Column;

template <>
class Column<Vector<Text>> : public ColumnBase {
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
  Vector<Text> get(Int row_id) const {
    size_t value_id = row_id.raw();
    if (value_id >= headers_.size()) {
      return Vector<Text>::na();
    }
    if (headers_[value_id].size.is_na()) {
      return Vector<Text>::na();
    }
    return Vector<Text>(&text_headers_[headers_[value_id].offset],
                        bodies_.data(), headers_[value_id].size);
  }
  // Read values.
  //
  // On failure, throws an exception.
  void read(ArrayCRef<Record> records, ArrayRef<Vector<Text>> values) const;

 private:
  struct Header {
    size_t offset;
    Int size;
  };
  Array<Header> headers_;
  Array<Header> text_headers_;
  Array<char> bodies_;

  static constexpr Header na_header() {
    return Header{ 0, Int::na() };
  }

  // Scan the column to find "value".
  //
  // If found, returns the row ID.
  // If not found, returns N/A.
  Int scan(const Vector<Text> &value) const;

  // Return the active column size.
  size_t get_valid_size() const;

  // Parse "datum" as Vector<Text>.
  //
  // On success, returns the result.
  // On failure, throws an exception.
  static Vector<Text> parse_datum(const Datum &datum);
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_VECTOR_TEXT_HPP
