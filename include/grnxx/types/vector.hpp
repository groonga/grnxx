#ifndef GRNXX_TYPES_VECTOR_HPP
#define GRNXX_TYPES_VECTOR_HPP

#include <initializer_list>

#include "grnxx/types/base_types.hpp"
#include "grnxx/types/geo_point.hpp"
#include "grnxx/types/string.hpp"

namespace grnxx {

template <typename T> class Vector;

// A Vector<Bool> contains at most 58 Bool values.
template <>
class Vector<Bool> {
 public:
  Vector() = default;
  Vector(std::initializer_list<Bool> bits) : data_(0) {
    UInt size = static_cast<UInt>(bits.size());
    if (size > 58) {
      size = 58;
    }
    UInt i = 0;
    for (auto it = bits.begin(); it != bits.end(); ++it) {
      if (*it) {
        data_ |= UInt(1) << i;
      }
      ++i;
    }
    data_ |= size << 58;
  }
  Vector(UInt bits, Int size)
      : data_((bits & mask(size)) |
              (static_cast<UInt>(std::min(size, Int(58))) << 58)) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Bool values.
  Int size() const {
    return static_cast<Int>(data_ >> 58);
  }
  // Return the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  Bool get(Int i) const {
    return (data_ & (UInt(1) << i)) != 0;
  }
  // Set the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  void set(Int i, Bool value) {
    if (value) {
      data_ |= UInt(1) << i;
    } else {
      data_ &= ~(UInt(1) << i);
    }
  }

  // Return the "i"-th Bool value.
  //
  // If "i" is invalid, the result is undefined.
  Bool operator[](Int i) const {
    return get(i);
  }

  // Return the set of Bool values.
  UInt bits() const {
    return data_ & mask(58);
  }

 private:
  UInt data_;

  static UInt mask(Int size) {
    return (UInt(1) << size) - 1;
  }
};

inline Bool operator==(Vector<Bool> lhs, Vector<Bool> rhs) {
  return (lhs.size() == rhs.size()) &&
         ((lhs.bits() ^ rhs.bits()) == 0);
}
inline Bool operator!=(Vector<Bool> lhs, Vector<Bool> rhs) {
  return (lhs.size() == rhs.size()) ||
         ((lhs.bits() ^ rhs.bits()) != 0);
}

template <>
class Vector<Int> {
 public:
  Vector() = default;
  Vector(const Int *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Int values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th Int value.
  //
  // If "i" is invalid, the result is undefined.
  Int get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th Int value.
  //
  // If "i" is invalid, the result is undefined.
  Int operator[](Int i) const {
    return get(i);
  }

 private:
  const Int *data_;
  Int size_;
};

inline Bool operator==(Vector<Int> lhs, Vector<Int> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Int> lhs, Vector<Int> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

template <>
class Vector<Float> {
 public:
  Vector() = default;
  Vector(const Float *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Float values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th Float value.
  //
  // If "i" is invalid, the result is undefined.
  Float get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th Float value.
  //
  // If "i" is invalid, the result is undefined.
  Float operator[](Int i) const {
    return get(i);
  }

 private:
  const Float *data_;
  Int size_;
};

inline Bool operator==(Vector<Float> lhs, Vector<Float> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Float> lhs, Vector<Float> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

template <>
class Vector<GeoPoint> {
 public:
  Vector() = default;
  Vector(const GeoPoint *data, Int size) : data_(data), size_(size) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of GeoPoint values.
  Int size() const {
    return size_;
  }
  // Return the "i"-th GeoPoint value.
  //
  // If "i" is invalid, the result is undefined.
  GeoPoint get(Int i) const {
    return data_[i];
  }

  // Return the "i"-th GeoPoint value.
  //
  // If "i" is invalid, the result is undefined.
  GeoPoint operator[](Int i) const {
    return get(i);
  }

 private:
  const GeoPoint *data_;
  Int size_;
};

inline Bool operator==(Vector<GeoPoint> lhs, Vector<GeoPoint> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<GeoPoint> lhs, Vector<GeoPoint> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

// TODO: Improve the implementation of Vector<Text>.
template <>
class Vector<Text> {
 public:
  Vector() = default;
  Vector(const Text *data, Int size)
      : is_direct_(1),
        size_(size),
        data_(data) {}
  Vector(const void *headers, const char *bodies, Int size)
      : is_direct_(0),
        size_(size),
        headers_(static_cast<const Header *>(headers)),
        bodies_(bodies) {}
  Vector(const Vector &) = default;

  Vector &operator=(const Vector &) = default;

  // Return the number of Text values.
  Int size() const {
    return static_cast<Int>(size_);
  }
  // Return the "i"-th Text value.
  //
  // If "i" is invalid, the result is undefined.
  Text get(Int i) const {
    if (is_direct_) {
      return data_[i];
    } else {
      return Text(&bodies_[headers_[i].offset], headers_[i].size);
    }
  }

  // Return the "i"-th Text value.
  //
  // If "i" is invalid, the result is undefined.
  Text operator[](Int i) const {
    return get(i);
  }

 private:
  struct Header {
    Int offset;
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

inline Bool operator==(Vector<Text> lhs, Vector<Text> rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return false;
    }
  }
  return true;
}
inline Bool operator!=(Vector<Text> lhs, Vector<Text> rhs) {
  if (lhs.size() != rhs.size()) {
    return true;
  }
  for (Int i = 0; i < lhs.size(); ++i) {
    if (lhs[i] != rhs[i]) {
      return true;
    }
  }
  return false;
}

}  // namespace grnxx

#endif  // GRNXX_TYPES_VECTOR_HPP
