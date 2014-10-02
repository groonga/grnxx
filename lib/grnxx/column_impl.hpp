#ifndef GRNXX_COLUMN_IMPL_HPP
#define GRNXX_COLUMN_IMPL_HPP

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
                                       const StringCRef &name,
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

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<T> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<T> values_;

  ColumnImpl();
};

template <>
class ColumnImpl<Int> : public Column {
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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_key_attribute(Error *error);
  bool unset_key_attribute(Error *error);

  bool set_initial_key(Error *error, Int row_id, const Datum &key);
  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);
  Int find_one(const Datum &datum) const;

  void clear_references(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Int get(Int row_id) const {
    return values_[row_id];
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Int> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<Int> values_;

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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

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

  ColumnImpl();
};

template <>
class ColumnImpl<Vector<Int>> : public Column {
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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  void clear_references(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Vector<Int> get(Int row_id) const {
    Int size = static_cast<Int>(headers_[row_id] & 0xFFFF);
    if (size == 0) {
      return Vector<Int>(nullptr, 0);
    }
    Int offset = static_cast<Int>(headers_[row_id] >> 16);
    if (size < 0xFFFF) {
      return Vector<Int>(&bodies_[offset], size);
    } else {
      // The size of a long vector is stored in front of the body.
      size = bodies_[offset];
      return Vector<Int>(&bodies_[offset + 1], size);
    }
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Vector<Int>> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<UInt> headers_;
  Array<Int> bodies_;

  ColumnImpl();
};

template <>
class ColumnImpl<Vector<Float>> : public Column {
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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Vector<Float> get(Int row_id) const {
    Int size = static_cast<Int>(headers_[row_id] & 0xFFFF);
    if (size == 0) {
      return Vector<Float>(nullptr, 0);
    }
    Int offset = static_cast<Int>(headers_[row_id] >> 16);
    if (size < 0xFFFF) {
      return Vector<Float>(&bodies_[offset], size);
    } else {
      // The size of a long vector is stored in front of the body.
      std::memcpy(&size, &bodies_[offset], sizeof(Int));
      return Vector<Float>(&bodies_[offset + 1], size);
    }
  }

  // Read values.
  void read(ArrayCRef<Record> records, ArrayRef<Vector<Float>> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<UInt> headers_;
  Array<Float> bodies_;

  ColumnImpl();
};

template <>
class ColumnImpl<Vector<GeoPoint>> : public Column {
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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Vector<GeoPoint> get(Int row_id) const {
    Int size = static_cast<Int>(headers_[row_id] & 0xFFFF);
    if (size == 0) {
      return Vector<GeoPoint>(nullptr, 0);
    }
    Int offset = static_cast<Int>(headers_[row_id] >> 16);
    if (size < 0xFFFF) {
      return Vector<GeoPoint>(&bodies_[offset], size);
    } else {
      // The size of a long vector is stored in front of the body.
      std::memcpy(&size, &bodies_[offset], sizeof(Int));
      return Vector<GeoPoint>(&bodies_[offset + 1], size);
    }
  }

  // Read values.
  void read(ArrayCRef<Record> records,
            ArrayRef<Vector<GeoPoint>> values) const {
    for (Int i = 0; i < records.size(); ++i) {
      values.set(i, get(records.get_row_id(i)));
    }
  }

 private:
  Array<UInt> headers_;
  Array<GeoPoint> bodies_;

  ColumnImpl();
};

// TODO: Improve the implementation.
template <>
class ColumnImpl<Vector<Text>> : public Column {
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
                                       const StringCRef &name,
                                       const ColumnOptions &options);

  ~ColumnImpl();

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

  ColumnImpl();
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_IMPL_HPP
