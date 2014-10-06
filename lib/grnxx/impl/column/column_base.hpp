#ifndef GRNXX_IMPL_COLUMN_COLUMN_BASE_HPP
#define GRNXX_IMPL_COLUMN_COLUMN_BASE_HPP

#include "grnxx/column.hpp"
#include "grnxx/name.hpp"
#include "grnxx/types.hpp"

namespace grnxx {
namespace impl {

class Table;

class ColumnBase : public grnxx::Column {
 public:
  // -- Public API (grnxx/column.hpp) --

  ColumnBase();
  virtual ~ColumnBase();

  grnxx::Table *table() const;
  StringCRef name() const {
    return name_.ref();
  }
  DataType data_type() const {
    return data_type_;
  }
  grnxx::Table *ref_table() const;
  bool has_key_attribute() const {
    return has_key_attribute_;
  }
  Int num_indexes() const {
    return indexes_.size();
  }

  Index *create_index(
      Error *error,
      const StringCRef &name,
      IndexType type,
      const IndexOptions &options = IndexOptions());
  bool remove_index(Error *error, const StringCRef &name);
  bool rename_index(Error *error,
                    const StringCRef &name,
                    const StringCRef &new_name);
  bool reorder_index(Error *error,
                     const StringCRef &name,
                     const StringCRef &prev_name);

  Index *get_index(Int index_id) const {
    return indexes_[index_id].get();
  }
  Index *find_index(Error *error, const StringCRef &name) const;

  // -- Internal API --

  // Create a new column.
  //
  // On success, returns a pointer to the column.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<ColumnBase> create(
      Error *error,
      Table *table,
      const StringCRef &name,
      DataType data_type,
      const ColumnOptions &options = ColumnOptions());

  // Return the owner table.
  Table *_table() const {
    return table_;
  }
  // Return the referenced (parent) table, or nullptr if the column is not a
  // reference column.
  Table *_ref_table() const {
    return ref_table_;
  }

  // Set a value.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set(Error *error, Int row_id, const Datum &datum);

  // Get a value.
  //
  // Stores a value identified by "row_id" into "*datum".
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool get(Error *error, Int row_id, Datum *datum) const;

  // Check if "datum" exists in the column or not.
  //
  // If exists, returns true.
  // Otherwise, returns false.
  virtual bool contains(const Datum &datum) const;

  // Find "datum" in the column.
  //
  // If found, returns the row ID of the matched value.
  // Otherwise, returns NULL_ROW_ID.
  virtual Int find_one(const Datum &datum) const;

  // Change the column name.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool rename(Error *error, const StringCRef &new_name);

  // Return whether the column is removable or not.
  bool is_removable();

  // Set the key attribute.
  virtual bool set_key_attribute(Error *error);
  // Unset the key attribute.
  virtual bool unset_key_attribute(Error *error);

  // Set the initial key.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_initial_key(Error *error, Int row_id, const Datum &key);

  // Set the default value.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  virtual bool set_default_value(Error *error, Int row_id) = 0;

  // Unset the value.
  virtual void unset(Int row_id) = 0;

  // Replace references to "row_id" with NULL.
  virtual void clear_references(Int row_id);

 protected:
  Table *table_;
  Name name_;
  DataType data_type_;
  Table *ref_table_;
  bool has_key_attribute_;
  Array<unique_ptr<Index>> indexes_;

  // Initialize the base members.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool initialize_base(Error *error,
                       Table *table,
                       const StringCRef &name,
                       DataType data_type,
                       const ColumnOptions &options = ColumnOptions());

 private:
  // Find an index with its ID.
  //
  // On success, returns a pointer to the index.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  Index *find_index_with_id(Error *error,
                            const StringCRef &name,
                            Int *column_id) const;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_COLUMN_BASE_HPP
