#include "grnxx/impl/column/column.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/index.hpp"

#include <set>

namespace grnxx {
namespace impl {

template <typename T>
bool Column<T>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TypeTraits<T>::data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  // Note that a Bool object does not have its own address.
  T old_value = get(row_id);
  T new_value;
  datum.force(&new_value);
  // TODO: Note that NaN != NaN.
  if (new_value != old_value) {
    for (Int i = 0; i < num_indexes(); ++i) {
      if (!indexes_[i]->insert(error, row_id, datum)) {
        for (Int j = 0; j < i; ++i) {
          indexes_[j]->remove(nullptr, row_id, datum);
        }
        return false;
      }
    }
    for (Int i = 0; i < num_indexes(); ++i) {
      indexes_[i]->remove(nullptr, row_id, old_value);
    }
  }
  values_.set(row_id, new_value);
  return true;
}

template <typename T>
bool Column<T>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

template <typename T>
unique_ptr<Column<T>> Column<T>::create(Error *error,
                                        Table *table,
                                        const StringCRef &name,
                                        const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               TypeTraits<T>::data_type(), options)) {
    return nullptr;
  }
  if (!column->values_.resize(error, table->max_row_id() + 1,
                              TypeTraits<T>::default_value())) {
    return nullptr;
  }
  return column;
}

template <typename T>
Column<T>::~Column() {}

template <typename T>
bool Column<T>::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1, TypeTraits<T>::default_value())) {
      return false;
    }
  }
  T value = TypeTraits<T>::default_value();
  for (Int i = 0; i < num_indexes(); ++i) {
    if (!indexes_[i]->insert(error, row_id, value)) {
      for (Int j = 0; j < i; ++j) {
        indexes_[j]->remove(nullptr, row_id, value);
      }
      return false;
    }
  }
  values_.set(row_id, value);
  return true;
}

template <typename T>
void Column<T>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  values_.set(row_id, TypeTraits<T>::default_value());
}

template <typename T>
Column<T>::Column() : ColumnBase(), values_() {}

template class Column<Bool>;
template class Column<Float>;
template class Column<GeoPoint>;
template class Column<Vector<Bool>>;

}  // namespace impl
}  // namespace grnxx
