#ifndef GRNXX_ORDER_HPP
#define GRNXX_ORDER_HPP

#include <vector>

#include "grnxx/types.hpp"

namespace grnxx {

// TODO: 無駄に複雑になっているので，単純化したい．

struct Order {
  unique_ptr<Expression> expression;
  OrderType type;

  Order() = default;
  Order(unique_ptr<Expression> &&expression, OrderType type)
      : expression(std::move(expression)),
        type(type) {}
};

class OrderSet {
 public:
  ~OrderSet();

  // Return the number of sort conditions.
  Int size() const {
    return static_cast<Int>(orders_.size());
  }

  // Return the "i"-th sort condition.
  //
  // If "i" is invalid, the result is undefined.
  Order &get(Int i) {
    return orders_[i];
  }

 private:
  std::vector<Order> orders_;

  OrderSet();

  // Append a sort condition.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append(Error *error, Order &&order);

  friend OrderSetBuilder;
};

class OrderSetBuilder {
 public:
  // Create an object for building order sets.
  //
  // On success, returns a poitner to the builder.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  static unique_ptr<OrderSetBuilder> create(Error *error, const Table *table);

  ~OrderSetBuilder();

  // Return the associated table.
  const Table *table() const {
    return table_;
  }

  // Append a sort condition.
  //
  // The sort condition is added as the last sort condition.
  // Sort conditions must be appended in order of priority.
  // "_id" is useful to perform stable sorting.
  //
  // On success, returns true.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool append(Error *error,
              unique_ptr<Expression> &&expression,
              OrderType type = REGULAR_ORDER);

  // Clear the building order set.
  void clear();

  // Complete building an order set and clear the builder.
  //
  // Fails if the order set is empty.
  //
  // On success, returns a poitner to the order set.
  // On failure, returns nullptr and stores error information into "*error" if
  // "error" != nullptr.
  unique_ptr<OrderSet> release(Error *error);

 private:
  const Table *table_;
  std::vector<Order> orders_;

  OrderSetBuilder();
};

}  // namespace grnxx

#endif  // GRNXX_ORDER_HPP
