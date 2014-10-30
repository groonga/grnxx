#ifndef GRNXX_STRING_HPP
#define GRNXX_STRING_HPP

#include <cstring>

#include "grnxx/error.hpp"
#include "grnxx/memory.hpp"

namespace grnxx {

// Reference to a byte string.
class StringCRef {
 public:
  StringCRef() = default;
  ~StringCRef() = default;

  constexpr StringCRef(const StringCRef &) = default;
  StringCRef &operator=(const StringCRef &) = default;

  StringCRef(const char *str)
      : data_(str),
        size_(str ? std::strlen(str) : 0) {}
  constexpr StringCRef(const char *data, size_t size)
      : data_(data),
        size_(size) {}

  // Return the "i"-th byte.
  const char &operator[](size_t i) const {
    return data_[i];
  }
  // Return the address.
  constexpr const char *data() const {
    return data_;
  }
  // Return the number of bytes.
  constexpr size_t size() const {
    return size_;
  }

  // Compare strings.
  bool operator==(const StringCRef &rhs) const {
    return (size_ == rhs.size_) && (std::memcmp(data_, rhs.data_, size_) == 0);
  }
  bool operator!=(const StringCRef &rhs) const {
    return (size_ != rhs.size_) || (std::memcmp(data_, rhs.data_, size_) != 0);
  }
  bool operator<(const StringCRef &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ < rhs.size_));
  }
  bool operator>(const StringCRef &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ > rhs.size_));
  }
  bool operator<=(const StringCRef &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ <= rhs.size_));
  }
  bool operator>=(const StringCRef &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ >= rhs.size_));
  }

  // Compare a string with a zero-terminated string.
  bool operator==(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if ((rhs[i] == '\0') || (data_[i] != rhs[i])) {
        return false;
      }
    }
    return rhs[size_] == '\0';
  }
  bool operator!=(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if ((rhs[i] == '\0') || (data_[i] != rhs[i])) {
        return true;
      }
    }
    return rhs[size_] != '\0';
  }
  bool operator<(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return false;
      }
      if (data_[i] != rhs[i]) {
        return static_cast<unsigned char>(data_[i]) <
               static_cast<unsigned char>(rhs[i]);
      }
    }
    return rhs[size_] != '\0';
  }
  bool operator>(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return true;
      }
      if (data_[i] != rhs[i]) {
        return static_cast<unsigned char>(data_[i]) >
               static_cast<unsigned char>(rhs[i]);
      }
    }
    return false;
  }
  bool operator<=(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return false;
      }
      if (data_[i] != rhs[i]) {
        return static_cast<unsigned char>(data_[i]) <
               static_cast<unsigned char>(rhs[i]);
      }
    }
    return true;
  }
  bool operator>=(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return true;
      }
      if (data_[i] != rhs[i]) {
        return static_cast<unsigned char>(data_[i]) >
               static_cast<unsigned char>(rhs[i]);
      }
    }
    return rhs[size_] == '\0';
  }

  // Return true if "*this" starts with "rhs".
  bool starts_with(const StringCRef &rhs) const {
    if (size_ < rhs.size_) {
      return false;
    }
    return std::memcmp(data_, rhs.data_, rhs.size_) == 0;
  }
  bool starts_with(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return true;
      } else if (data_[i] != rhs[i]) {
        return false;
      }
    }
    return rhs[size_] == '\0';
  }

  // Return true if "*this" ends with "rhs".
  bool ends_with(const StringCRef &rhs) const {
    if (size_ < rhs.size_) {
      return false;
    }
    return std::memcmp(data_ + size_ - rhs.size_, rhs.data_, rhs.size_) == 0;
  }
  bool ends_with(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if (rhs[i] == '\0') {
        return std::memcmp(data_ + size_ - i, rhs, i) == 0;
      }
    }
    return (rhs[size_] == '\0') ?
           (std::memcmp(data_, rhs, size_) == 0) : false;
  }

 private:
  const char *data_;
  size_t size_;
};

// Compare a null-terminated string with a string.
inline bool operator==(const char *lhs, const StringCRef &rhs) {
  return rhs == lhs;
}
inline bool operator!=(const char *lhs, const StringCRef &rhs) {
  return rhs != lhs;
}
inline bool operator<(const char *lhs, const StringCRef &rhs) {
  return rhs > lhs;
}
inline bool operator>(const char *lhs, const StringCRef &rhs) {
  return rhs < lhs;
}
inline bool operator<=(const char *lhs, const StringCRef &rhs) {
  return rhs >= lhs;
}
inline bool operator>=(const char *lhs, const StringCRef &rhs) {
  return rhs <= lhs;
}

class String {
 public:
  constexpr String() : buf_(nullptr), size_(0), capacity_(0) {}
  ~String();

  String(const String &) = delete;
  String& operator=(const String &) = delete;

  String(String &&rhs)
      : buf_(rhs.buf_),
        size_(rhs.size_),
        capacity_(rhs.capacity_) {
    rhs.buf_ = nullptr;
    rhs.size_ = 0;
    rhs.capacity_ = 0;
  }
  String &operator=(String &&rhs) {
    buf_ = rhs.buf_;
    size_ = rhs.size_;
    capacity_ = rhs.capacity_;
    rhs.buf_ = nullptr;
    rhs.size_ = 0;
    rhs.capacity_ = 0;
    return *this;
  }

  operator StringCRef() const {
    return cref();
  }

  StringCRef cref(size_t offset = 0) const {
    return StringCRef(buf_ + offset, size_ - offset);
  }
  StringCRef cref(size_t offset, size_t size) const {
    return StringCRef(buf_ + offset, size);
  }

  char &operator[](size_t i) {
    return buf_[i];
  }
  const char &operator[](size_t i) const {
    return buf_[i];
  }

  char &front() {
    return buf_[0];
  }
  const char &front() const {
    return buf_[0];
  }

  char &back() {
    return buf_[size_ - 1];
  }
  const char &back() const {
    return buf_[size_ - 1];
  }

  char *data() {
    return buf_;
  }
  const char *data() const {
    return buf_;
  }
  size_t size() const {
    return size_;
  }
  size_t capacity() const {
    return capacity_;
  }

  bool reserve(Error *error, size_t new_size) {
    if (new_size <= capacity_) {
      return true;
    }
    return resize_buf(error, new_size);
  }

  bool assign(Error *error, const StringCRef &rhs) {
    if (rhs.size() > capacity_) {
      if (!resize_buf(error, rhs.size())) {
        return false;
      }
    }
    std::memcpy(buf_, rhs.data(), rhs.size());
    size_ = rhs.size();
    return true;
  }
  bool assign(Error *error, const char *data, size_t size) {
    return assign(error, StringCRef(data, size));
  }

  bool resize(Error *error, size_t new_size) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    size_ = new_size;
    return true;
  }
  bool resize(Error *error, size_t new_size, char value) {
    if (new_size > capacity_) {
      if (!resize_buf(error, new_size)) {
        return false;
      }
    }
    if (new_size > size_) {
      std::memset(buf_ + size_, value, new_size - size_);
    }
    size_ = new_size;
    return true;
  }

  void clear() {
    size_ = 0;
  }

  bool push_back(Error *error, char value) {
    if (size_ == capacity_) {
      if (!resize_buf(error, size_ + 1)) {
        return false;
      }
    }
    buf_[size_] = value;
    ++size_;
    return true;
  }
  void pop_back() {
    --size_;
  }

  bool append(Error *error, const StringCRef &rhs) {
    if ((size_ + rhs.size()) > capacity_) {
      // NOTE: If "rhs" is a part of "*this", "rhs" is destoyed in
      //       resize_buf(), so append_overlap() is required.
      if ((rhs.data() >= buf_) && (rhs.data() < (buf_ + size_))) {
        return append_overlap(error, rhs);
      } else if (!resize_buf(error, (size_ + rhs.size()))) {
        return false;
      }
    }
    std::memcpy(buf_ + size_, rhs.data(), rhs.size());
    size_ += rhs.size();
    return true;
  }
  bool append(Error *error, const char *data, size_t size) {
    return append(error, StringCRef(data, size));
  }

  // Compare a strings.
  bool operator==(const StringCRef &rhs) const {
    return cref() == rhs;
  }
  bool operator!=(const StringCRef &rhs) const {
    return cref() != rhs;
  }
  bool operator<(const StringCRef &rhs) const {
    return cref() < rhs;
  }
  bool operator>(const StringCRef &rhs) const {
    return cref() > rhs;
  }
  bool operator<=(const StringCRef &rhs) const {
    return cref() <= rhs;
  }
  bool operator>=(const StringCRef &rhs) const {
    return cref() >= rhs;
  }

  // Compare a string with a zero-terminated string.
  bool operator==(const char *rhs) const {
    return cref() == rhs;
  }
  bool operator!=(const char *rhs) const {
    return cref() != rhs;
  }
  bool operator<(const char *rhs) const {
    return cref() < rhs;
  }
  bool operator>(const char *rhs) const {
    return cref() > rhs;
  }
  bool operator<=(const char *rhs) const {
    return cref() <= rhs;
  }
  bool operator>=(const char *rhs) const {
    return cref() >= rhs;
  }

  // Return true if "*this" starts with "rhs".
  bool starts_with(const StringCRef &rhs) const {
    return cref().starts_with(rhs);
  }
  bool starts_with(const char *rhs) const {
    return cref().starts_with(rhs);
  }

  // Return true if "*this" ends with "rhs".
  bool ends_with(const StringCRef &rhs) const {
    return cref().ends_with(rhs);
  }
  bool ends_with(const char *rhs) const {
    return cref().ends_with(rhs);
  }

 private:
  char *buf_;
  size_t size_;
  size_t capacity_;

  // Assume new_size > capacity_.
  bool resize_buf(Error *error, size_t new_size);
  // Resize the internal buffer and append a part of "*this".
  bool append_overlap(Error *error, const StringCRef &rhs);
};

// Compare a null-terminated string with a string.
inline bool operator==(const char *lhs, const String &rhs) {
  return rhs == lhs;
}
inline bool operator!=(const char *lhs, const String &rhs) {
  return rhs != lhs;
}
inline bool operator<(const char *lhs, const String &rhs) {
  return rhs > lhs;
}
inline bool operator>(const char *lhs, const String &rhs) {
  return rhs < lhs;
}
inline bool operator<=(const char *lhs, const String &rhs) {
  return rhs >= lhs;
}
inline bool operator>=(const char *lhs, const String &rhs) {
  return rhs <= lhs;
}

}  // namespace grnxx

#endif  // GRNXX_STRING_HPP
