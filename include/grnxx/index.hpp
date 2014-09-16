#ifndef GRNXX_INDEX_HPP
#define GRNXX_INDEX_HPP

#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

enum EndPointType {
  INCLUSIVE_END_POINT,
  EXCLUSIVE_END_POINT
};

struct EndPoint {
  Datum value;
  EndPointType type;
};

class IndexRange {
 public:
  IndexRange()
      : has_lower_bound_(false),
        has_upper_bound_(false),
        lower_bound_(),
        upper_bound_() {}

  bool has_lower_bound() const {
    return has_lower_bound_;
  }
  bool has_upper_bound() const {
    return has_upper_bound_;
  }

  const EndPoint &lower_bound() const {
    return lower_bound_;
  }
  const EndPoint &upper_bound() const {
    return upper_bound_;
  }

  void set_lower_bound(const Datum &value,
                       EndPointType type = INCLUSIVE_END_POINT) {
    has_lower_bound_ = true;
    lower_bound_.value = value;
    lower_bound_.type = type;
  }
  void set_upper_bound(const Datum &value,
                       EndPointType type = INCLUSIVE_END_POINT) {
    has_upper_bound_ = true;
    upper_bound_.value = value;
    upper_bound_.type = type;
  }

  void unset_lower_bound() {
    has_lower_bound_ = false;
  }
  void unset_upper_bound() {
    has_lower_bound_ = false;
  }

 private:
  bool has_lower_bound_;
  bool has_upper_bound_;
  EndPoint lower_bound_;
  EndPoint upper_bound_;
};

class Index {
 public:
  virtual ~Index();

  // Return the owner column.
  Column *column() const {
    return column_;
  }
  // Return the name.
  String name() const {
    return name_.ref();
  }
  // Return the index type.
  IndexType type() const {
    return type_;
  }

  // Create a cursor to get records.
  //
  // On success, returns a pointer to the cursor.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual unique_ptr<Cursor> find(
      Error *error,
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const;

  // Create a cursor to get records.
  //
  // Returns a pointer to the cursor on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual unique_ptr<Cursor> create_cursor(
      Error *error,
      const IndexRange &range = IndexRange(),
      const CursorOptions &options = CursorOptions()) const;

  // Insert a new entry.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool insert(Error *error, Int row_id, const Datum &value) = 0;
  // Insert an entry.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool remove(Error *error, Int row_id, const Datum &value) = 0;

 protected:
  Column *column_;
  Name name_;
  IndexType type_;

  Index();

  // Initialize the base members.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool initialize_base(Error *error,
                       Column *column,
                       String name,
                       IndexType type,
                       const IndexOptions &options);

 private:
  // Create a new index.
  //
  // Returns a pointer to the index on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<Index> create(
      Error *error,
      Column *column,
      String name,
      IndexType type,
      const IndexOptions &options = IndexOptions());

  // Change the index name.
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, String new_name);

  // Return whether the index is removable or not.
  bool is_removable();

  friend class Column;
};

}  // namespace grnxx

#endif  // GRNXX_INDEX_HPP
