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

 private:
  Column *column_;
  Name name_;
  IndexType type_;

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

  Index();

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
