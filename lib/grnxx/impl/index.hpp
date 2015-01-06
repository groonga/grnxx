#ifndef GRNXX_IMPL_INDEX_HPP
#define GRNXX_IMPL_INDEX_HPP

#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/index.hpp"

namespace grnxx {
namespace impl {

using ColumnInterface = grnxx::Column;
using IndexInterface = grnxx::Index;

class ColumnBase;

class Index : public IndexInterface {
 public:
  // -- Public API (grnxx/index.hpp) --

  Index(ColumnBase *column, const String &name);
  virtual ~Index();

  ColumnInterface *column() const;
  String name() const {
    return name_;
  }

  virtual bool test_uniqueness() const = 0;

  virtual void insert(Int row_id, const Datum &value) = 0;
  virtual void remove(Int row_id, const Datum &value) = 0;

  virtual bool contains(const Datum &value) const;
  virtual Int find_one(const Datum &value) const;
  virtual std::unique_ptr<Cursor> find(
      const Datum &value,
      const CursorOptions &options = CursorOptions()) const;
  virtual std::unique_ptr<Cursor> find_in_range(
      const IndexRange &range = IndexRange(),
      const CursorOptions &options = CursorOptions()) const;
  virtual std::unique_ptr<Cursor> find_starts_with(
      const EndPoint &prefix,
      const CursorOptions &options = CursorOptions()) const;
  virtual std::unique_ptr<Cursor> find_prefixes(
      const Datum &value,
      const CursorOptions &options = CursorOptions()) const;

  // -- Internal API --

  // Create a new index.
  //
  // On success, returns the index.
  // On failure, throws an exception.
  static Index *create(ColumnBase *column,
                       const String &name,
                       IndexType type,
                       const IndexOptions &options);

  // Return the owner table.
  ColumnBase *_column() const {
    return column_;
  }

  // Change the column name.
  //
  // On failure, throws an exception.
  void rename(const String &new_name);

  // Return whether the index is removable or not.
  bool is_removable() const;

 private:
  ColumnBase *column_;
  String name_;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_INDEX_HPP
