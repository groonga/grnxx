#include "grnxx/order.hpp"

#include "grnxx/error.hpp"
#include "grnxx/expression.hpp"

namespace grnxx {

OrderSet::~OrderSet() {}

OrderSet::OrderSet() : orders_() {}

bool OrderSet::append(Error *error, Order &&order) {
  try {
    orders_.push_back(std::move(order));
    return true;
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
}

unique_ptr<OrderSetBuilder> OrderSetBuilder::create(Error *error,
                                                    const Table *table) {
  unique_ptr<OrderSetBuilder> builder(new (nothrow) OrderSetBuilder);
  if (!builder) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  builder->table_ = table;
  return builder;
}

OrderSetBuilder::~OrderSetBuilder() {}

bool OrderSetBuilder::append(Error *error,
                             unique_ptr<Expression> &&expression,
                             OrderType type) {
  try {
    orders_.push_back(Order(std::move(expression), type));
    return true;
  } catch (...) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return false;
  }
}

void OrderSetBuilder::clear() {
  orders_.clear();
}

unique_ptr<OrderSet> OrderSetBuilder::release(Error *error) {
  unique_ptr<OrderSet> order_set(new (nothrow) OrderSet);
  if (!order_set) {
    GRNXX_ERROR_SET(error, NO_MEMORY, "Memory allocation failed");
    return nullptr;
  }
  // TODO: The current status should not be broken on failure.
  for (size_t i = 0; i < orders_.size(); ++i) {
    if (!order_set->append(error, std::move(orders_[i]))) {
      orders_.clear();
      return nullptr;
    }
  }
  orders_.clear();
  return order_set;
}

OrderSetBuilder::OrderSetBuilder() : table_(nullptr), orders_() {};

}  // namespace grnxx
