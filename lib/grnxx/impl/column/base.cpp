#include "grnxx/impl/column/base.hpp"

#include "grnxx/impl/column/scalar.hpp"
#include "grnxx/impl/column/vector.hpp"
#include "grnxx/impl/index.hpp"
#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

ColumnBase::ColumnBase(Table *table,
                       const String &name,
                       DataType data_type)
    : ColumnInterface(),
      table_(table),
      name_(name.clone()),
      data_type_(data_type),
      reference_table_(nullptr),
      is_key_(false),
      indexes_() {}

ColumnBase::~ColumnBase() {}

TableInterface *ColumnBase::table() const {
  return table_;
}

TableInterface *ColumnBase::reference_table() const {
  return reference_table_;
}

Index *ColumnBase::create_index(
    const String &name,
    IndexType type,
    const IndexOptions &options) {
  if (find_index(name)) {
    throw "Index already exists";  // TODO
  }
  indexes_.reserve(indexes_.size() + 1);
  std::unique_ptr<Index> new_index(Index::create(this, name, type, options));
  indexes_.push_back(std::move(new_index));
  return indexes_.back().get();
}

void ColumnBase::remove_index(const String &name) {
  size_t index_id;
  if (!find_index_with_id(name, &index_id)) {
    throw "Index not found";  // TODO
  }
  if (!indexes_[index_id]->is_removable()) {
    throw "Index not removable";  // TODO
  }
  indexes_.erase(index_id);
}

void ColumnBase::rename_index(const String &name, const String &new_name) {
  size_t index_id;
  if (!find_index_with_id(name, &index_id)) {
    throw "Index not found";  // TODO
  }
  if (name == new_name) {
    return;
  }
  if (find_index(new_name)) {
    throw "Index already exists";  // TODO
  }
  indexes_[index_id]->rename(new_name);
}

void ColumnBase::reorder_index(const String &name, const String &prev_name) {
  size_t index_id;
  if (!find_index_with_id(name, &index_id)) {
    throw "Index not found";  // TODO
  }
  size_t new_index_id = 0;
  if (prev_name.size() != 0) {
    size_t prev_index_id;
    if (!find_index_with_id(prev_name, &prev_index_id)) {
      throw "Index not found";  // TODO
    }
    if (index_id <= prev_index_id) {
      new_index_id = prev_index_id;
    } else {
      new_index_id = prev_index_id + 1;
    }
  }
  for ( ; index_id < new_index_id; ++index_id) {
    std::swap(indexes_[index_id], indexes_[index_id + 1]);
  }
  for ( ; index_id > new_index_id; --index_id) {
    std::swap(indexes_[index_id], indexes_[index_id - 1]);
  }
}

Index *ColumnBase::find_index(const String &name) const {
  for (size_t i = 0; i < num_indexes(); ++i) {
    if (name == indexes_[i]->name()) {
      return indexes_[i].get();
    }
  }
  return nullptr;
}

std::unique_ptr<ColumnBase> ColumnBase::create(
    Table *table,
    const String &name,
    DataType data_type,
    const ColumnOptions &options) try {
  std::unique_ptr<ColumnBase> column;
  switch (data_type) {
    case BOOL_DATA: {
      column.reset(new impl::Column<Bool>(table, name, options));
      break;
    }
    case INT_DATA: {
      column.reset(new impl::Column<Int>(table, name, options));
      break;
    }
    case FLOAT_DATA: {
      column.reset(new impl::Column<Float>(table, name, options));
      break;
    }
    case GEO_POINT_DATA: {
      column.reset(new impl::Column<GeoPoint>(table, name, options));
      break;
    }
    case TEXT_DATA: {
      column.reset(new impl::Column<Text>(table, name, options));
      break;
    }
    case BOOL_VECTOR_DATA: {
      column.reset(new impl::Column<Vector<Bool>>(table, name, options));
      break;
    }
    case INT_VECTOR_DATA: {
      column.reset(new impl::Column<Vector<Int>>(table, name, options));
      break;
    }
    case FLOAT_VECTOR_DATA: {
      column.reset(new impl::Column<Vector<Float>>(table, name, options));
      break;
    }
    case GEO_POINT_VECTOR_DATA: {
      column.reset(new impl::Column<Vector<GeoPoint>>(table, name, options));
      break;
    }
    case TEXT_VECTOR_DATA: {
      column.reset(new impl::Column<Vector<Text>>(table, name, options));
      break;
    }
    default: {
      throw "Not supported";  // TODO
    }
  }
  return column;
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

void ColumnBase::rename(const String &new_name) {
  name_.assign(new_name);
}

bool ColumnBase::is_removable() const {
  // TODO: Reference column is not supported yet.
  return true;
}

void ColumnBase::set_key_attribute() {
  throw "Not supported";  // TODO
}

void ColumnBase::unset_key_attribute() {
  throw "Not supported";  // TODO
}

void ColumnBase::set_key(Int, const Datum &) {
  throw "Not supported";  // TODO
}

//void ColumnBase::clear_references(Int) {
//  throw "Not supported";  // TODO
//}

Index *ColumnBase::find_index_with_id(const String &name,
                                      size_t *index_id) const {
  for (size_t i = 0; i < num_indexes(); ++i) {
    if (name == indexes_[i]->name()) {
      if (index_id) {
        *index_id = i;
      }
      return indexes_[i].get();
    }
  }
  return nullptr;
}

}  // namespace impl
}  // namespace grnxx
