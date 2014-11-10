#ifndef GRNXX_IMPL_COLUMN_BASE_HPP
#define GRNXX_IMPL_COLUMN_BASE_HPP

#include <memory>

#include "grnxx/column.hpp"
#include "grnxx/impl/index.hpp"
#include "grnxx/table.hpp"

namespace grnxx {
namespace impl {

using TableInterface = grnxx::Table;
using ColumnInterface = grnxx::Column;

class Table;

class ColumnBase : public ColumnInterface {
 public:
  // -- Public API (grnxx/column.hpp) --

  ColumnBase(Table *table, const String &name, DataType data_type);
  virtual ~ColumnBase();

  TableInterface *table() const;
  String name() const {
    return name_;
  }
  DataType data_type() const {
    return data_type_;
  }
  TableInterface *reference_table() const;
  bool is_key() const {
    return is_key_;
  }
  size_t num_indexes() const {
    return indexes_.size();
  }

  Index *create_index(
      const String &name,
      IndexType type,
      const IndexOptions &options);
  void remove_index(const String &name);
  void rename_index(const String &name, const String &new_name);
  void reorder_index(const String &name, const String &prev_name);

  Index *get_index(size_t i) const {
    return indexes_[i].get();
  }
  Index *find_index(const String &name) const;

  virtual void set(Int row_id, const Datum &datum) = 0;
  virtual void get(Int row_id, Datum *datum) const = 0;

  virtual bool contains(const Datum &datum) const;
  virtual Int find_one(const Datum &datum) const;

  // -- Internal API --

  // Create a new column.
  //
  // On success, returns the column.
  // On failure, throws an exception.
  static std::unique_ptr<ColumnBase> create(
      Table *table,
      const String &name,
      DataType data_type,
      const ColumnOptions &options);

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
  virtual void set_key(Int row_id, const Datum &key);

  // Unset the value.
  virtual void unset(Int row_id) = 0;

//  // Replace references to "row_id" with NULL.
//  virtual void clear_references(Int row_id);

 protected:
  Table *table_;
  String name_;
  DataType data_type_;
  Table *reference_table_;
  bool is_key_;
  Array<std::unique_ptr<Index>> indexes_;

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
