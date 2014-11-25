#ifndef GRNXX_DATA_TYPES_VECTOR_TEXT_HPP
#define GRNXX_DATA_TYPES_VECTOR_TEXT_HPP

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/text.hpp"

namespace grnxx {

template <typename T> class Vector;

template <>
class Vector<Text> {
 public:
  Vector() = default;
  ~Vector() = default;

  constexpr Vector(const Vector &) = default;
  Vector &operator=(const Vector &) = default;

  constexpr Vector(const Text *data, size_t size)
      : is_direct_(true),
        size_(size),
        data_(data) {}
  Vector(const void *headers, const char *bodies, Int size)
      : is_direct_(false),
        size_(size),
        headers_(static_cast<const Header *>(headers)),
        bodies_(bodies) {}
  explicit constexpr Vector(NA)
      : is_direct_(true),
        size_(NA()),
        data_(nullptr) {}

  Text operator[](Int i) const {
    if (is_na() || (static_cast<uint64_t>(i.raw()) >=
                    static_cast<uint64_t>(size_.raw()))) {
      return Text::na();
    }
    if (is_direct_) {
      return data_[i.raw()];
    } else {
      return Text(&bodies_[headers_[i.raw()].offset],
                  headers_[i.raw()].size.raw());
    }
  }
  // TODO: To be removed.
  const Text &operator[](size_t i) const {
    return data_[i];
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
        Text lhs_text = (*this)[grnxx::Int(i)];
        Text rhs_text = rhs[grnxx::Int(i)];
        // TODO: Binary not equal should be used.
        if (!(lhs_text.is_na() && rhs_text.is_na()) &&
            (!(lhs_text == rhs_text)).is_true()) {
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
        Text lhs_text = (*this)[grnxx::Int(i)];
        Text rhs_text = rhs[grnxx::Int(i)];
        // TODO: Binary not equal should be used.
        if (!(lhs_text.is_na() && rhs_text.is_na()) &&
            (!(lhs_text == rhs_text)).is_true()) {
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
      // TODO: This can be improved.
      if (operator[](grnxx::Int(i)).unmatch(rhs[grnxx::Int(i)])) {
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
      // TODO: This can be improved.
      if (operator[](grnxx::Int(i)).unmatch(rhs[grnxx::Int(i)])) {
        return true;
      }
    }
    return false;
  }

  static constexpr DataType type() {
    return TEXT_VECTOR_DATA;
  }

  static constexpr Vector empty() {
    return Vector(nullptr, 0);
  }
  static constexpr Vector na() {
    return Vector(NA());
  }

 private:
  struct Header {
    size_t offset;
    Int size;
  };

  bool is_direct_;
  Int size_;
  union {
    const Text *data_;
    struct {
      const Header *headers_;
      const char *bodies_;
    };
  };
};

using TextVector = Vector<Text>;

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_VECTOR_TEXT_HPP
