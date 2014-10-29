#ifndef GRNXX_NEW_TYPES_ROW_ID_HPP
#define GRNXX_NEW_TYPES_ROW_ID_HPP

#include <cstdint>
#include <limits>

#include "grnxx/new_types/na.hpp"

namespace grnxx {

class RowID {
 public:
  RowID() = default;
  ~RowID() = default;

  constexpr RowID(const RowID &) = default;
  RowID &operator=(const RowID &) = default;

  explicit constexpr RowID(uint64_t value) : value_(value) {}
  explicit constexpr RowID(NA) : value_(na_value()) {}

  constexpr uint64_t value() const {
    return value_;
  }

  constexpr bool is_min() const {
    return value_ == min_value();
  }
  constexpr bool is_max() const {
    return value_ == max_value();
  }
  constexpr bool is_na() const {
    return value_ == na_value();
  }

  static constexpr RowID min() {
    return RowID(min_value());
  }
  static constexpr RowID max() {
    return RowID(max_value());
  }
  static constexpr RowID na() {
    return RowID(NA());
  }

  static constexpr uint64_t min_value() {
    return 0;
  }
  static constexpr uint64_t max_value() {
    return std::numeric_limits<uint64_t>::max() - 1;
  }
  static constexpr uint64_t na_value() {
    return std::numeric_limits<uint64_t>::max();
  }

 private:
  uint64_t value_;
};

}  // namespace grnxx

#endif  // GRNXX_NEW_TYPES_ROW_ID_HPP
