#ifndef GRNXX_INDEX_HPP
#define GRNXX_INDEX_HPP

#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

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
  // Returns a pointer to the cursor on success.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  virtual unique_ptr<Cursor> create_cursor(
      Error *error,
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
                       const IndexOptions &options = IndexOptions());

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
