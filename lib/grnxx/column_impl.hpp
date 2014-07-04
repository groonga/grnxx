#ifndef GRNXX_COLUMN_BASE_HPP
#define GRNXX_COLUMN_BASE_HPP

#include <vector>

#include "grnxx/column.hpp"

namespace grnxx {

class BoolColumn : public Column {
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
  static unique_ptr<BoolColumn> create(Error *error,
                                       Table *table,
                                       String name,
                                       const ColumnOptions &options);

  ~BoolColumn();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Bool get(Int row_id) const {
    return values_[row_id];
  }

 protected:
  std::vector<Bool> values_;

  BoolColumn();
};

class IntColumn : public Column {
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
  static unique_ptr<IntColumn> create(Error *error,
                                      Table *table,
                                      String name,
                                      const ColumnOptions &options);

  ~IntColumn();

  bool set_default_value(Error *error, Int row_id);
  void unset(Int row_id);

  // Return a value identified by "row_id".
  //
  // Assumes that "row_id" is valid. Otherwise, the result is undefined.
  Int get(Int row_id) const {
    return values_[row_id];
  }

 protected:
  std::vector<Int> values_;

  IntColumn();
};

}  // namespace grnxx

#endif  // GRNXX_COLUMN_BASE_HPP
