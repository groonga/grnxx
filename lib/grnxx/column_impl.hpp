#ifndef GRNXX_COLUMN_IMPL_HPP
#define GRNXX_COLUMN_IMPL_HPP

#include "grnxx/array.hpp"
#include "grnxx/column.hpp"

namespace grnxx {

template <typename T>
class ColumnImpl : public Column {
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
  static unique_ptr<ColumnImpl> create(Error *error,
                                       Table *table,
                                       String name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  T get(Int row_id) const {
    return values_[row_id];
  }

 protected:
  Array<T> values_;

  ColumnImpl();
};

template <>
class ColumnImpl<Text> : public Column {
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
  static unique_ptr<ColumnImpl> create(Error *error,
                                       Table *table,
                                       String name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

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
      return String(&bodies_[offset + sizeof(Int)], size);
    }
  }

 protected:
  Array<uint64_t> headers_;
  Array<char> bodies_;

  ColumnImpl();
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_IMPL_HPP
