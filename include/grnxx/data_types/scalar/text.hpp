#ifndef GRNXX_DATA_TYPES_SCALAR_TEXT_HPP
#define GRNXX_DATA_TYPES_SCALAR_TEXT_HPP

#include <cstdint>
#include <cstring>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/int.hpp"
#include "grnxx/string.hpp"

namespace grnxx {

// Reference to a byte string.
class Text {
 public:
  Text() = default;
  ~Text() = default;

  constexpr Text(const Text &) = default;
  Text &operator=(const Text &) = default;

  explicit Text(const char *string)
      : data_(string),
        size_(std::strlen(string)) {}
  constexpr Text(const char *data, size_t size) : data_(data), size_(size) {}
  explicit Text(const String &string)
      : data_(string.data()),
        size_(string.size()) {}
  explicit constexpr Text(NA) : data_(nullptr), size_(NA()) {}

  constexpr const char *raw_data() const {
    return data_;
  }
  constexpr size_t raw_size() const {
    return size_.raw();
  }
  constexpr Int size() const {
    return size_;
  }

  constexpr bool is_empty() const {
    return raw_size() == 0;
  }
  constexpr bool is_na() const {
    return size_.is_na();
  }

  uint64_t hash() const;

  Bool operator==(const Text &rhs) const {
    Bool has_equal_size = (size_ == rhs.size_);
    if (has_equal_size.is_true()) {
      return Bool(std::memcmp(data_, rhs.data_, raw_size()) == 0);
    }
    return has_equal_size;
  }
  Bool operator!=(const Text &rhs) const {
    Bool has_not_equal_size = (size_ != rhs.size_);
    if (has_not_equal_size.is_false()) {
      return Bool(std::memcmp(data_, rhs.data_, raw_size()) != 0);
    }
    return has_not_equal_size;
  }
  Bool operator<(const Text &rhs) const {
    Bool has_less_size = (size_ < rhs.size_);
    if (has_less_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_size.is_true() ?
                      raw_size() : rhs.raw_size();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_size : Bool(false));
  }
  Bool operator>(const Text &rhs) const {
    return rhs < *this;
  }
  Bool operator<=(const Text &rhs) const {
    Bool has_less_or_equal_size = (size_ <= rhs.size_);
    if (has_less_or_equal_size.is_na()) {
      return Bool::na();
    }
    size_t min_size = has_less_or_equal_size.is_true() ?
                      raw_size() : rhs.raw_size();
    int data_result = std::memcmp(data_, rhs.data_, min_size);
    return (data_result < 0) ? Bool(true) :
           ((data_result == 0) ? has_less_or_equal_size : Bool(false));
  }
  Bool operator>=(const Text &rhs) const {
    return rhs <= *this;
  }

  Bool starts_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size.is_true()) {
      return Bool(std::memcmp(data_, rhs.data_, rhs.raw_size()) == 0);
    }
    return has_greater_or_equal_size;
  }
  Bool ends_with(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size.is_true()) {
      return Bool(std::memcmp(data_ + raw_size() - rhs.raw_size(),
                              rhs.data_, rhs.raw_size()) == 0);
    }
    return has_greater_or_equal_size;
  }

  Bool contains(const Text &rhs) const {
    Bool has_greater_or_equal_size = (size_ >= rhs.size_);
    if (has_greater_or_equal_size.is_true()) {
      size_t end_offset = raw_size() - rhs.raw_size() + 1;
      for (size_t offset = 0; offset < end_offset; ++offset) {
        if (std::memcmp(data_ + offset, rhs.data_, rhs.raw_size()) == 0) {
          return Bool(true);
        }
      }
      return Bool(false);
    }
    return has_greater_or_equal_size;
  }

  bool match(const Text &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return false;
    }
    if (is_na()) {
      return true;
    }
    return std::memcmp(data_, rhs.data_, raw_size()) == 0;
  }
  bool unmatch(const Text &rhs) const {
    if (size_.unmatch(rhs.size_)) {
      return true;
    }
    if (is_na()) {
      return false;
    }
    return std::memcmp(data_, rhs.data_, raw_size()) != 0;
  }

  static constexpr DataType type() {
    return TEXT_DATA;
  }

  static constexpr Text empty() {
    return Text(nullptr, 0);
  }
  static constexpr Text na() {
    return Text(NA());
  }

  static constexpr size_t raw_na_size() {
    return Int::na().raw();
  }

 private:
  const char *data_;
  Int size_;

  static uint64_t rotate(uint64_t x, uint8_t n) {
    return (x << n) | (x >> (64 - n));
  }
  static uint64_t mix(uint64_t x) {
    x ^= x >> 33;
    x *= uint64_t(0xFF51AFD7ED558CCDULL);
    x ^= x >> 33;
    x *= uint64_t(0xC4CEB9FE1A85EC53ULL);
    x ^= x >> 33;
    return x;
  }
};

// NOTE: This returns the first 64-bit of MurmurHash3's output (128-bit).
//
// TODO: This implementation should not be inlined.
inline uint64_t Text::hash() const {
  if (is_na()) {
    return 0;
  }

  const uint8_t *data = reinterpret_cast<const uint8_t *>(data_);
  const size_t len = size_.raw();
  const size_t nblocks = len / 16;

  uint64_t h1 = 0;
  uint64_t h2 = 0;

  const uint64_t c1 = uint64_t(0x87C37B91114253D5ULL);
  const uint64_t c2 = uint64_t(0x4CF5AD432745937FULL);

  //----------
  // body

  const uint64_t *blocks = reinterpret_cast<const uint64_t *>(data);
  for (size_t i = 0; i < nblocks; i++) {
    uint64_t k1 = blocks[i * 2];
    uint64_t k2 = blocks[(i * 2) + 1];

    k1 *= c1;
    k1  = rotate(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = rotate(h1, 27);
    h1 += h2;
    h1 = (h1 * 5) + 0x52DCE729;

    k2 *= c2;
    k2  = rotate(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = rotate(h2, 31);
    h2 += h1;
    h2 = (h2 * 5) + 0x38495AB5;
  }

  //----------
  // tail

  const uint8_t *tail = data + (nblocks * 16);

  uint64_t k1 = 0;
  uint64_t k2 = 0;

  switch (len & 15) {
    case 15: {
      k2 ^= static_cast<uint64_t>(tail[14]) << 48;
    }
    case 14: {
      k2 ^= static_cast<uint64_t>(tail[13]) << 40;
    }
    case 13: {
      k2 ^= static_cast<uint64_t>(tail[12]) << 32;
    }
    case 12: {
      k2 ^= static_cast<uint64_t>(tail[11]) << 24;
    }
    case 11: {
      k2 ^= static_cast<uint64_t>(tail[10]) << 16;
    }
    case 10: {
      k2 ^= static_cast<uint64_t>(tail[9]) << 8;
    }
    case 9: {
      k2 ^= static_cast<uint64_t>(tail[8]) << 0;
      k2 *= c2;
      k2 = rotate(k2, 33);
      k2 *= c1;
      h2 ^= k2;
    }
    case 8: {
      k1 ^= static_cast<uint64_t>(tail[7]) << 56;
    }
    case 7: {
      k1 ^= static_cast<uint64_t>(tail[6]) << 48;
    }
    case 6: {
      k1 ^= static_cast<uint64_t>(tail[5]) << 40;
    }
    case 5: {
      k1 ^= static_cast<uint64_t>(tail[4]) << 32;
    }
    case 4: {
      k1 ^= static_cast<uint64_t>(tail[3]) << 24;
    }
    case 3: {
      k1 ^= static_cast<uint64_t>(tail[2]) << 16;
    }
    case 2: {
      k1 ^= static_cast<uint64_t>(tail[1]) << 8;
    }
    case 1: {
      k1 ^= static_cast<uint64_t>(tail[0]) << 0;
      k1 *= c1;
      k1 = rotate(k1, 31);
      k1 *= c2;
      h1 ^= k1;
    }
  };

  //----------
  // finalization

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = mix(h1);
  h2 = mix(h2);

  h1 += h2;
  h2 += h1;

  return h1;
}

}  // namespace grnxx

#endif   // GRNXX_DATA_TYPES_SCALAR_TEXT_HPP
