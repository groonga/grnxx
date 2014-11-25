#ifndef GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP
#define GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/float.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Float> {
 public:
  Vector() = default;
  ~Vector() = default;

  constexpr Vector(const Vector &) = default;
  Vector &operator=(const Vector &) = default;

  constexpr Vector(const Float *data, size_t size)
      : data_(data),
        size_(size) {}
  explicit constexpr Vector(NA) : data_(nullptr), size_(NA()) {}

  Float operator[](Int i) const {
    if (is_na() || (static_cast<uint64_t>(i.raw()) >=
                    static_cast<uint64_t>(size_.raw()))) {
      return Float::na();
    }
    return data_[i.raw()];
  }
  // TODO: To be removed.
  const Float &operator[](size_t i) const {
    return data_[i];
  }
  constexpr const Float *data() const {
    return data_;
  }
  constexpr Int size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return size_.raw() == 0;
  }
  constexpr bool is_na() const {
    return size_.is_na();
  }

  // TODO: The behavior of N/A in vector is not fixed yet (#107).
  Bool operator==(const Vector &rhs) const {
    Bool has_equal_size = (size_ == rhs.size_);
    if (has_equal_size.is_true()) {
      size_t size = size_.raw();
      for (size_t i = 0; i < size; ++i) {
        if ((data_[i].value() != rhs.data_[i].value()) &&
            (!data_[i].is_na() || !rhs.data_[i].is_na())) {
          return Bool(false);
        }
      }
    }
    return has_equal_size;
  }
  // TODO: The behavior of N/A in vector is not fixed yet (#107).
  Bool operator!=(const Vector &rhs) const {
    Bool has_not_equal_size = (size_ != rhs.size_);
    if (has_not_equal_size.is_false()) {
      size_t size = size_.raw();
      for (size_t i = 0; i < size; ++i) {
        if ((data_[i].value() != rhs.data_[i].value()) &&
            (!data_[i].is_na() || !rhs.data_[i].is_na())) {
          return Bool(true);
        }
      }
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
    // TODO: This is because raw values are not normalized.
    size_t size = size_.raw();
    for (size_t i = 0; i < size; ++i) {
      if (data_[i].unmatch(rhs.data_[i])) {
        return false;
      }
    }
    return true;
  }
  bool unmatch(const Vector &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return true;
    }
    if (is_na()) {
      return false;
    }
    // TODO: This is because raw values are not normalized.
    size_t size = size_.raw();
    for (size_t i = 0; i < size; ++i) {
      if (data_[i].unmatch(rhs.data_[i])) {
        return true;
      }
    }
    return false;
  }

  static constexpr DataType type() {
    return FLOAT_VECTOR_DATA;
  }

  static constexpr Vector empty() {
    return Vector(nullptr, 0);
  }
  static constexpr Vector na() {
    return Vector(NA());
  }

 private:
  const Float *data_;
  Int size_;
};

using FloatVector = Vector<Float>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_FLOAT_HPP
