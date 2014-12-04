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

  // Test the uniqueness of the owner column.
  //
  // If the onwer column has no duplicate values, except N/A, returns true.
  // Otherwise, returns false.
  virtual bool test_uniqueness() const = 0;

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
      const Datum &value,
      const CursorOptions &options = CursorOptions()) const = 0;

 protected:
  virtual ~Index() = default;
};

}  // namespace grnxx

#endif  // GRNXX_INDEX_HPP
