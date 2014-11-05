#ifndef GRNXX_IMPL_COLUMN_BASE_HPP
#define GRNXX_IMPL_COLUMN_BASE_HPP

#include <memory>

#include "grnxx/column.hpp"

namespace grnxx {
namespace impl {

using TableInterface = grnxx::Table;
using ColumnInterface = grnxx::Column;

class Table;

class Index;  // TODO

class ColumnBase : public ColumnInterface {
 public:
  // -- Public API (grnxx/column.hpp) --

  ColumnBase(Table *table,
             const String &name,
             DataType data_type,
             Table *reference_table = nullptr);
  virtual ~ColumnBase();

  TableInterface *table() const;
  String name() const {
    return name_;
  }
  DataType data_type() const {
    return data_type_;
  }
  TableInterface *reference_table() const;
  bool has_key_attribute() const {
    return has_key_attribute_;
  }
  size_t num_indexes() const {
    return indexes_.size();
  }

//  Index *create_index(
//      Error *error,
//      const String &name,
//      IndexType type,
//      const IndexOptions &options = IndexOptions());
//  bool remove_index(Error *error, const String &name);
//  bool rename_index(Error *error,
//                    const String &name,
//                    const String &new_name);
//  bool reorder_index(Error *error,
//                     const String &name,
//                     const String &prev_name);

//  Index *get_index(Int index_id) const {
//    return indexes_[index_id].get();
//  }
//  Index *find_index(Error *error, const String &name) const;

//  bool set(Error *error, Int row_id, const Datum &datum);
//  bool get(Error *error, Int row_id, Datum *datum) const;

//  bool contains(const Datum &datum) const;
  Int find_one(const Datum &datum) const;

  // -- Internal API --

  // Create a new column.
  //
  // On success, returns the column.
  // On failure, throws an exception.
  static std::unique_ptr<ColumnBase> create(
      Table *table,
      const String &name,
      DataType data_type,
      const ColumnOptions &options = ColumnOptions());

  // Return the owner table.
  Table *_table() const {
    return table_;
  }
  // Return the referenced (parent) table.
  // If "this" is not a reference column, returns nullptr.
  Table *_reference_table() const {
    return reference_table_;
  }

  // Change the column name.
  //
  // On failure, throws an exception.
  void rename(const String &new_name);

  // Return whether the column is removable or not.
  bool is_removable() const;

  // Set the key attribute.
  //
  // On failure, throws an exception.
  virtual void set_key_attribute();
  // Unset the key attribute.
  //
  // On failure, throws an exception.
  virtual void unset_key_attribute();

  // Set the initial key.
  //
  // On failure, throws an exception.
  virtual void set_initial_key(Int row_id, const Datum &key);

  // Unset the value.
  virtual void unset(Int row_id) = 0;

  // Replace references to "row_id" with NULL.
  virtual void clear_references(Int row_id);

 protected:
  Table *table_;
  String name_;
  DataType data_type_;
  Table *reference_table_;
  bool has_key_attribute_;
  Array<std::unique_ptr<Index>> indexes_;

//  // Initialize the base members.
//  //
//  // On success, returns true.
//  // On failure, returns false and stores error information into "*error" if
//  // "error" != nullptr.
//  bool initialize_base(Error *error,
//                       Table *table,
//                       const String &name,
//                       DataType data_type,
//                       const ColumnOptions &options = ColumnOptions());

// private:
//  // Find an index with its ID.
//  //
//  // On success, returns a pointer to the index.
//  // On failure, returns nullptr and stores error information into "*error" if
//  // "error" != nullptr.
//  Index *find_index_with_id(Error *error,
//                            const String &name,
//                            Int *column_id) const;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_COLUMN_BASE_HPP
