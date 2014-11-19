#ifndef GRNXX_DATA_TYPES_VECTOR_INT_HPP
#define GRNXX_DATA_TYPES_VECTOR_INT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/int.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Int> {
 public:
  Vector() = default;
  ~Vector() = default;

  constexpr Vector(const Vector &) = default;
  Vector &operator=(const Vector &) = default;

  constexpr Vector(const Int *data, size_t size) : data_(data), size_(size) {}
  explicit constexpr Vector(NA) : data_(nullptr), size_(NA()) {}

  Int operator[](Int i) const {
    if (is_na() || (static_cast<uint64_t>(i.value()) >=
                    static_cast<uint64_t>(size_.value()))) {
      return Int::na();
    }
    return data_[i.value()];
  }
  // TODO: To be removed.
  const Int &operator[](size_t i) const {
    return data_[i];
  }
  constexpr const Int *data() const {
    return data_;
  }
  constexpr Int size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return size_.value() == 0;
  }
  constexpr bool is_na() const {
    return size_.is_na();
  }

  // TODO: The behavior of N/A in vector is not fixed yet (#107).
  Bool operator==(const Vector &rhs) const {
    Bool has_equal_size = (size_ == rhs.size_);
    if (has_equal_size.is_true()) {
      return Bool(std::memcmp(data_, rhs.data_,
                              sizeof(Int) * size_.value()) == 0);
    }
    return has_equal_size;
  }
  // TODO: The behavior of N/A in vector is not fixed yet (#107).
  Bool operator!=(const Vector &rhs) const {
    Bool has_not_equal_size = (size_ != rhs.size_);
    if (has_not_equal_size.is_false()) {
      return Bool(std::memcmp(data_, rhs.data_,
                              sizeof(Int) * size_.value()) != 0);
    }
    return has_not_equal_size;
  }

  bool match(const Vector &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return false;
    }
    if (is_na()) {
      return true;
    }
    return std::memcmp(data_, rhs.data_, sizeof(Int) * size_.value()) == 0;
  }
  bool unmatch(const Vector &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return true;
    }
    if (is_na()) {
      return false;
    }
    return std::memcmp(data_, rhs.data_, sizeof(Int) * size_.value()) != 0;
  }

  static constexpr DataType type() {
    return INT_VECTOR_DATA;
  }

  static constexpr Vector empty() {
    return Vector(nullptr, 0);
  }
  static constexpr Vector na() {
    return Vector(NA());
  }

 private:
  const Int *data_;
  Int size_;
};

using IntVector = Vector<Int>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_INT_HPP
