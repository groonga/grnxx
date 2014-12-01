#ifndef GRNXX_INDEX_HPP
#define GRNXX_INDEX_HPP

#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/data_types.hpp"
#include "grnxx/index.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

class Column;

enum EndPointType {
  INCLUSIVE_END_POINT,
  EXCLUSIVE_END_POINT
};

struct EndPoint {
  Datum value;
  EndPointType type;

  EndPoint() : value(NA()), type(INCLUSIVE_END_POINT) {}
};

class IndexRange {
 public:
  IndexRange() = default;
  ~IndexRange() = default;

  const EndPoint &lower_bound() const {
    return lower_bound_;
  }
  const EndPoint &upper_bound() const {
    return upper_bound_;
  }

  void set_lower_bound(const Datum &value,
                       EndPointType type = INCLUSIVE_END_POINT) {
    lower_bound_.value = value;
    lower_bound_.type = type;
  }
  void set_upper_bound(const Datum &value,
                       EndPointType type = INCLUSIVE_END_POINT) {
    upper_bound_.value = value;
    upper_bound_.type = type;
  }

  void unset_lower_bound() {
    lower_bound_.value = NA();
  }
  void unset_upper_bound() {
    upper_bound_.value = NA();
  }

 private:
  EndPoint lower_bound_;
  EndPoint upper_bound_;
};

enum IndexType {
  // TODO: Tree indexes support range search.
  TREE_INDEX,
  // TODO: Hash indexes support exact match search.
  HASH_INDEX
};

struct IndexOptions {
};

class Index {
 public:
  Index() = default;

  // Return the owner column.
  virtual Column *column() const = 0;
  // Return the name.
  virtual String name() const = 0;
  // Return the index type.
  virtual IndexType type() const = 0;

  // Insert a new entry.
  //
  // On failure, throws an exception.
  virtual void insert(Int row_id, const Datum &value) = 0;
  // Remove an entry.
  //
  // On failure, throws an exception.
  virtual void remove(Int row_id, const Datum &value) = 0;

  // Return whether "value" is registered or not.
  virtual bool contains(const Datum &value) const = 0;

  // Find "datum".
  //
  // If found, returns the row ID.
  // If not found, returns N/A.
  virtual Int find_one(const Datum &value) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns the cursor.
  // On failure, throws an exception.
  virtual std::unique_ptr<Cursor> find(
      const Datum &value,
      const CursorOptions &options = CursorOptions()) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns the cursor.
  // On failure, throws an exception.
  virtual std::unique_ptr<Cursor> find_in_range(
      const IndexRange &range = IndexRange(),
      const CursorOptions &options = CursorOptions()) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns the cursor.
  // On failure, throws an exception.
  virtual std::unique_ptr<Cursor> find_starts_with(
      const EndPoint &prefix,
      const CursorOptions &options = CursorOptions()) const = 0;

  // Create a cursor to get records.
  //
  // On success, returns the cursor.
  // On failure, throws an exception.
  virtual std::unique_ptr<Cursor> find_prefixes(
      const Datum &datum,
      const CursorOptions &options = CursorOptions()) const = 0;

 protected:
  virtual ~Index() = default;
};

//class Index {
// public:
//  virtual ~Index() = default;

//  // Return the owner column.
//  Column *column() const {
//    return column_;
//  }
//  // Return the name.
//  StringCRef name() const {
//    return name_.ref();
//  }
//  // Return the index type.
//  IndexType type() const {
//    return type_;
//  }

//  // Check if "datum" is registered or not.
//  //
//  // If registered, returns true.
//  // Otherwise, returns false.
//  virtual bool contains(const Datum &datum) const;

//  // Search the index for "datum".
//  //
//  // On success, returns the row ID of one of the matched values.
//  // On failure, returns NULL_ROW_ID.
//  virtual Int find_one(const Datum &datum) const;

//  // Create a cursor to get records.
//  //
//  // On success, returns a pointer to the cursor.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual unique_ptr<Cursor> find(
//      Error *error,
//      const Datum &datum,
//      const CursorOptions &options = CursorOptions()) const;

//  // Create a cursor to get records.
//  //
//  // Returns a pointer to the cursor on success.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual unique_ptr<Cursor> find_in_range(
//      Error *error,
//      const IndexRange &range = IndexRange(),
//      const CursorOptions &options = CursorOptions()) const;

//  // Create a cursor to get records.
//  //
//  // Returns a pointer to the cursor on success.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual unique_ptr<Cursor> find_starts_with(
//      Error *error,
//      const EndPoint &prefix,
//      const CursorOptions &options = CursorOptions()) const;

//  // Create a cursor to get records.
//  //
//  // Returns a pointer to the cursor on success.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual unique_ptr<Cursor> find_prefixes(
//      Error *error,
//      const Datum &datum,
//      const CursorOptions &options = CursorOptions()) const;

//  // Insert a new entry.
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual bool insert(Error *error, Int row_id, const Datum &value) = 0;
//  // Insert an entry.
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  virtual bool remove(Error *error, Int row_id, const Datum &value) = 0;

// protected:
//  Column *column_;
//  Name name_;
//  IndexType type_;

//  Index();

//  // Initialize the base members.
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  bool initialize_base(Error *error,
//                       Column *column,
//                       const StringCRef &name,
//                       IndexType type,
//                       const IndexOptions &options);

// private:
//  // Create a new index.
//  //
//  // Returns a pointer to the index on success.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  static unique_ptr<Index> create(
//      Error *error,
//      Column *column,
//      const StringCRef &name,
//      IndexType type,
//      const IndexOptions &options = IndexOptions());

//  // Change the index name.
//  //
//  // Returns true on success.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  bool rename(Error *error, const StringCRef &new_name);

//  // Return whether the index is removable or not.
//  bool is_removable();

// protected:
//  Index() = default;
//};

}  // namespace grnxx

#endif  // GRNXX_INDEX_HPP
