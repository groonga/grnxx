#ifndef GRNXX_IMPL_INDEX_HPP
#define GRNXX_IMPL_INDEX_HPP

#include <memory>

#include "grnxx/cursor.hpp"
#include "grnxx/index.hpp"

namespace grnxx {
namespace impl {

using ColumnInterface = grnxx::Column;
using IndexInterface = grnxx::Index;

class Index : public IndexInterface {
 public:
  // -- Public API (grnxx/index.hpp) --

  Index(Column *column, const String &name);
  virtual ~Index();

  // Return the owner column.
  ColumnInterface *column() const;
  // Return the name.
  String name() const {
    return name_;
  }

  // -- Internal API --

  // Create a new index.
  //
  // On success, returns the column.
  // On failure, throws an exception.
  static std::unique_ptr<Index> create(
      Column *column,
      const String &name,
      IndexType type,
      const IndexOptions &options);

  // Return the owner table.
  Column *_column() const {
    return column_;
  }

  // Change the column name.
  //
  // On failure, throws an exception.
  void rename(const String &new_name);

  // Return whether the column is removable or not.
  bool is_removable() const;

 private:
  Column *column_;
  String name_;
};

}  // namespace impl
}  // namespace grnxx

#endif  // GRNXX_IMPL_INDEX_HPP
