#include "grnxx/impl/column/column.hpp"

#include "grnxx/cursor.hpp"
#include "grnxx/impl/db.hpp"
#include "grnxx/impl/table.hpp"
#include "grnxx/index.hpp"

#include <cmath>

namespace grnxx {
namespace impl {

bool Column<Float>::set(Error *error, Int row_id, const Datum &datum) {
  if (datum.type() != TypeTraits<Float>::data_type()) {
    GRNXX_ERROR_SET(error, INVALID_ARGUMENT, "Wrong data type");
    return false;
  }
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  Float old_value = get(row_id);
  Float new_value = datum.force_float();
  if ((std::isnan(new_value) && std::isnan(old_value)) ||
      (new_value != old_value)) {
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

bool Column<Float>::get(Error *error, Int row_id, Datum *datum) const {
  if (!table_->test_row(error, row_id)) {
    return false;
  }
  *datum = values_[row_id];
  return true;
}

unique_ptr<Column<Float>> Column<Float>::create(Error *error,
                                                Table *table,
                                                const StringCRef &name,
                                                const ColumnOptions &options) {
  unique_ptr<Column> column(new (nothrow) Column);
  if (!column) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  if (!column->initialize_base(error, table, name,
                               TypeTraits<Float>::data_type(), options)) {
    return nullptr;
  }
  if (!column->values_.resize(error, table->max_row_id() + 1,
                              TypeTraits<Float>::default_value())) {
    return nullptr;
  }
  return column;
}

Column<Float>::~Column() {}

bool Column<Float>::set_default_value(Error *error, Int row_id) {
  if (row_id >= values_.size()) {
    if (!values_.resize(error, row_id + 1,
                        TypeTraits<Float>::default_value())) {
      return false;
    }
  }
  Float value = TypeTraits<Float>::default_value();
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

void Column<Float>::unset(Int row_id) {
  for (Int i = 0; i < num_indexes(); ++i) {
    indexes_[i]->remove(nullptr, row_id, get(row_id));
  }
  values_.set(row_id, TypeTraits<Float>::default_value());
}

Column<Float>::Column() : ColumnBase(), values_() {}

}  // namespace impl
}  // namespace grnxx
