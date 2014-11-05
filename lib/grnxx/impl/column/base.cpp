#include "grnxx/impl/column/base.hpp"

//#include "grnxx/impl/column/scalar.hpp"
//#include "grnxx/impl/column/vector.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"

namespace grnxx {
namespace impl {

ColumnBase::ColumnBase(Table *table,
                       const String &name,
                       DataType data_type,
                       Table *reference_table)
    : ColumnInterface(),
      table_(table),
      name_(name),
      data_type_(data_type),
      reference_table_(reference_table),
      is_key_(false) {}

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
  throw "Not supported yet";  // TODO
}

void ColumnBase::remove_index(const String &name) {
  throw "Not supported yet";  // TODO
}

void ColumnBase::rename_index(const String &name, const String &new_name) {
  throw "Not supported yet";  // TODO
}

void ColumnBase::reorder_index(const String &name, const String &prev_name) {
  throw "Not supported yet";  // TODO
}

Index *ColumnBase::find_index(const String &name) const {
  throw "Not supported yet";  // TODO
}

void ColumnBase::set(Int row_id, const Datum &datum) {
  throw "Not supported yet";  // TODO
}

void ColumnBase::get(Int row_id, Datum *datum) const {
  throw "Not supported yet";  // TODO
}

bool ColumnBase::contains(const Datum &datum) const {
  throw "Not supported yet";  // TODO
}

Int ColumnBase::find_one(const Datum &datum) const {
  throw "Not supported yet";  // TODO
}

//Index *ColumnBase::create_index(Error *error,
//                                const StringCRef &name,
//                                IndexType type,
//                                const IndexOptions &options) {
//  if (find_index(nullptr, name)) {
//    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
//                    "Index already exists: name = \"%.*s\"",
//                    static_cast<int>(name.size()), name.data());
//    return nullptr;
//  }
//  if (!indexes_.reserve(error, indexes_.size() + 1)) {
//    return nullptr;
//  }
//  unique_ptr<Index> new_index =
//      Index::create(error, this, name, type, options);
//  if (!new_index) {
//    return nullptr;
//  }
//  indexes_.push_back(error, std::move(new_index));
//  return indexes_.back().get();
//}

//bool ColumnBase::remove_index(Error *error, const StringCRef &name) {
//  Int index_id;
//  if (!find_index_with_id(error, name, &index_id)) {
//    return false;
//  }
//  if (!indexes_[index_id]->is_removable()) {
//    GRNXX_ERROR_SET(error, NOT_REMOVABLE,
//                    "Index is not removable: name = \"%.*s\"",
//                    static_cast<int>(name.size()), name.data());
//    return false;
//  }
//  indexes_.erase(index_id);
//  return true;
//}

//bool ColumnBase::rename_index(Error *error,
//                              const StringCRef &name,
//                              const StringCRef &new_name) {
//  Int index_id;
//  if (!find_index_with_id(error, name, &index_id)) {
//    return false;
//  }
//  if (name == new_name) {
//    return true;
//  }
//  if (find_index(nullptr, new_name)) {
//    GRNXX_ERROR_SET(error, ALREADY_EXISTS,
//                    "Index already exists: new_name = \"%.*s\"",
//                    static_cast<int>(new_name.size()), new_name.data());
//    return false;
//  }
//  return indexes_[index_id]->rename(error, new_name);
//}

//bool ColumnBase::reorder_index(Error *error,
//                               const StringCRef &name,
//                               const StringCRef &prev_name) {
//  Int index_id;
//  if (!find_index_with_id(error, name, &index_id)) {
//    return false;
//  }
//  Int new_index_id = 0;
//  if (prev_name.size() != 0) {
//    Int prev_index_id;
//    if (!find_index_with_id(error, prev_name, &prev_index_id)) {
//      return false;
//    }
//    if (index_id <= prev_index_id) {
//      new_index_id = prev_index_id;
//    } else {
//      new_index_id = prev_index_id + 1;
//    }
//  }
//  for ( ; index_id < new_index_id; ++index_id) {
//    std::swap(indexes_[index_id], indexes_[index_id + 1]);
//  }
//  for ( ; index_id > new_index_id; --index_id) {
//    std::swap(indexes_[index_id], indexes_[index_id - 1]);
//  }
//  return true;
//}

//Index *ColumnBase::find_index(Error *error, const StringCRef &name) const {
//  for (Int index_id = 0; index_id < num_indexes(); ++index_id) {
//    if (name == indexes_[index_id]->name()) {
//      return indexes_[index_id].get();
//    }
//  }
//  GRNXX_ERROR_SET(error, NOT_FOUND, "Index not found");
//  return nullptr;
//}

//bool ColumnBase::set(Error *error, Int, const Datum &) {
//  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
//  return false;
//}

//bool ColumnBase::get(Error *error, Int, Datum *) const {
//  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
//  return false;
//}

//bool ColumnBase::contains(const Datum &datum) const {
//  return find_one(datum) != NULL_ROW_ID;
//}

//Int ColumnBase::find_one(const Datum &) const {
//  // TODO: This function should be pure virtual.
//  return Int::na();
//}

std::unique_ptr<ColumnBase> ColumnBase::create(Table *table,
                                               const String &name,
                                               DataType data_type,
                                               const ColumnOptions &options) {
  throw "Not supported yet";  // TODO
//  switch (data_type) {
//    case BOOL_DATA: {
//      return impl::Column<Bool>::create(error, table, name, options);
//    }
//    case INT_DATA: {
//      return impl::Column<Int>::create(error, table, name, options);
//    }
//    case FLOAT_DATA: {
//      return impl::Column<Float>::create(error, table, name, options);
//    }
//    case GEO_POINT_DATA: {
//      return impl::Column<GeoPoint>::create(error, table, name, options);
//    }
//    case TEXT_DATA: {
//      return impl::Column<Text>::create(error, table, name, options);
//    }
//    case BOOL_VECTOR_DATA: {
//      return impl::Column<Vector<Bool>>::create(error, table, name, options);
//    }
//    case INT_VECTOR_DATA: {
//      return impl::Column<Vector<Int>>::create(error, table, name, options);
//    }
//    case FLOAT_VECTOR_DATA: {
//      return impl::Column<Vector<Float>>::create(error, table, name, options);
//    }
//    case GEO_POINT_VECTOR_DATA: {
//      return impl::Column<Vector<GeoPoint>>::create(error, table, name, options);
//    }
//    case TEXT_VECTOR_DATA: {
//      return impl::Column<Vector<Text>>::create(error, table, name, options);
//    }
//    default: {
//      // TODO: Other data types are not supported yet.
//      GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not suported yet");
//      return nullptr;
//    }
//  }
}

void ColumnBase::rename(const String &new_name) {
  name_.assign(new_name);
}

bool ColumnBase::is_removable() const {
  // TODO: Reference column is not supported yet.
  return true;
}

void ColumnBase::set_key_attribute() {
  throw "Not supported yet";  // TODO
}

void ColumnBase::unset_key_attribute() {
  throw "Not supported yet";  // TODO
}

void ColumnBase::set_initial_key(Int, const Datum &) {
  throw "Not supported yet";  // TODO
}

void ColumnBase::clear_references(Int) {
  throw "Not supported yet";  // TODO
}

//bool ColumnBase::initialize_base(Error *error,
//                                 Table *table,
//                                 const StringCRef &name,
//                                 DataType data_type,
//                                 const ColumnOptions &options) {
//  table_ = table;
//  if (!name_.assign(error, name)) {
//    return false;
//  }
//  data_type_ = data_type;
//  if ((data_type == INT_DATA) || (data_type == INT_VECTOR_DATA)) {
//    if (options.ref_table_name.size() != 0) {
//      auto ref_table = table_->_db()->find_table(error, options.ref_table_name);
//      if (!ref_table) {
//        return false;
//      }
//      ref_table_ = ref_table;
//    }
//  }
//  return true;
//}

//Index *ColumnBase::find_index_with_id(Error *error,
//                                      const StringCRef &name,
//                                      Int *index_id) const {
//  for (Int i = 0; i < num_indexes(); ++i) {
//    if (name == indexes_[i]->name()) {
//      if (index_id != nullptr) {
//        *index_id = i;
//      }
//      return indexes_[i].get();
//    }
//  }
//  GRNXX_ERROR_SET(error, NOT_FOUND, "Index not found: name = \"%.*s\"",
//                  static_cast<int>(name.size()), name.data());
//  return nullptr;
//}

}  // namespace impl
}  // namespace grnxx
