#ifndef GRNXX_STRING_HPP
#define GRNXX_STRING_HPP

#include <cstring>

namespace grnxx {

class String {
 public:
  // Create an empty string.
  String() : data_(nullptr), size_(0), capacity_(0) {}
  // Free allocated memory.
  ~String();

  // Create a reference to "string".
  String(const String &string)
      : data_(string.data_),
        size_(string.size_),
        capacity_(0) {}
  // Create a reference to "rhs".
  String &operator=(const String &rhs);

  // Move the ownership of a string.
  String(String &&string)
      : buffer_(string.buffer_),
        size_(string.size_),
        capacity_(string.capacity_) {
    string.capacity_ = 0;
  }
  // Move the ownership of a string.
  String &operator=(String &&rhs) {
    buffer_ = rhs.buffer_;
    size_ = rhs.size_;
    capacity_ = rhs.capacity_;
    rhs.capacity_ = 0;
    return *this;
  }

  // Create a reference to a null-terminated string.
  //
  // If "string" == nullptr, the behavior is undefined.
  String(const char *string)
      : data_(string),
        size_(std::strlen(string)),
        capacity_(0) {}
  // Create a reference to a byte string.
  String(const char *data, size_t size)
      : data_(data),
        size_(size),
        capacity_(0) {}
  // Create an instance.
  //
  // On failure, throws an exception.
  explicit String(size_t size);
  // Create an instance filled with "byte".
  //
  // On failure, throws an exception.
  String(size_t size, char byte);

  // Create a reference to a substring.
  String substring(size_t offset = 0) const {
    return String(data_ + offset, size_ - offset);
  }
  // Create a reference to a substring.
  String substring(size_t offset, size_t size) const {
    return String(data_ + offset, size);
  }

  // Return whether "this" is empty or not.
  bool is_empty() const {
    return size_ == 0;
  }
  // Return whether "this" is a reference or not.
  bool is_reference() const {
    return capacity_ == 0;
  }
  // Return whether "this" is an instance or not.
  bool is_instance() const {
    return capacity_ != 0;
  }

  // Create a clone instance of "this".
  //
  // On success, returns the instance.
  // On failure, throws an exception.
  String clone() const;

  // Instanciate the string.
  //
  // On success, returns a reference to "this".
  // On failure, throws an exception.
  String &instantiate();

  // Return a reference to the "i"-th byte.
  //
  // If "i" is too large, the behavior is undefined.
  // If "this" is a reference, the contents must not be modified.
  char &operator[](size_t i) {
    return buffer_[i];
  }
  // Return a reference to the "i"-th byte.
  //
  // If "i" is too large, the behavior is undefined.
  const char &operator[](size_t i) const {
    return data_[i];
  }

  // Return a reference to the first byte.
  //
  // If "this" is empty, the behavior is undefined.
  // If "this" is a reference, the contents must not be modified.
  char &front() {
    return buffer_[0];
  }
  // Return a reference to the first byte.
  //
  // If "this" is empty, the behavior is undefined.
  const char &front() const {
    return data_[0];
  }

  // Return a reference to the last byte.
  //
  // If "this" is empty, the behavior is undefined.
  // If "this" is a reference, the contents must not be modified.
  char &back() {
    return buffer_[size_ - 1];
  }
  // Return a reference to the last byte.
  //
  // If "this" is empty, the behavior is undefined.
  const char &back() const {
    return data_[size_ - 1];
  }

  // Return a pointer to the buffer.
  //
  // If "this" is a reference, the contents must not be modified.
  char *buffer() {
    return buffer_;
  }
  // Return a pointer to the contents.
  const char *data() const {
    return data_;
  }
  // Return the length in bytes.
  size_t size() const {
    return size_;
  }
  // Return the buffer size.
  size_t capacity() const {
    return capacity_;
  }

  // Reserve memory for at least "new_size" bytes.
  //
  // On failure, throws an exception.
  void reserve(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
  }

  // Store a string.
  //
  // On failure, throws an exception.
  String &assign(const String &string) {
    assign(string.data(), string.size());
    return *this;
  }
  // Store a string.
  //
  // On failure, throws an exception.
  String &assign(const char *data, size_t size) {
    if (size > capacity_) {
      resize_buffer(size);
    }
    std::memcpy(buffer_, data, size);
    size_ = size;
    return *this;
  }

  // Resize the string.
  //
  // On failure, throws an exception.
  void resize(size_t new_size) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    size_ = new_size;
  }
  // Resize the string and fill the new bytes with "byte".
  //
  // On failure, throws an exception.
  void resize(size_t new_size, char byte) {
    if (new_size > capacity_) {
      resize_buffer(new_size);
    }
    if (new_size > size_) {
      std::memset(buffer_ + size_, byte, new_size - size_);
    }
    size_ = new_size;
  }

  // Clear the contents.
  void clear() {
    size_ = 0;
  }

  // Concatenate strings.
  String operator+(const String rhs) const {
    String result(size_ + rhs.size_);
    std::memcpy(result.buffer_, data_, size_);
    std::memcpy(result.buffer_ + size_, rhs.data_, rhs.size_);
    return result;
  }

  // Append "rhs" to the end.
  //
  // On failure, throws an exception.
  String &operator+=(char byte) {
    return append(byte);
  }
  // Append "rhs" to the end.
  //
  // On failure, throws an exception.
  String &operator+=(const String &rhs) {
    return append(rhs.data_, rhs.size_);
  }

  // Append "byte" to the end.
  //
  // On failure, throws an exception.
  String &append(char byte) {
    if (size_ >= capacity_) {
      resize_buffer(size_ + 1);
    }
    buffer_[size_] = byte;
    ++size_;
    return *this;
  }
  // Append "string" to the end.
  //
  // On failure, throws an exception.
  String &append(const String &string) {
    append(string.data(), string.size());
    return *this;
  }
  // Append a string to the end.
  //
  // On failure, throws an exception.
  String &append(const char *data, size_t size) {
    if ((size_ + size) > capacity_) {
      // NOTE: If the given string is a part of this instance, it will be
      //       moved to the new address in resize_buffer().
      if ((capacity_ != 0) &&
          (data >= buffer_) && (data < (buffer_ + size_))) {
        append_overlap(data, size);
        return *this;
      } else {
        resize_buffer(size_ + size);
      }
    }
    std::memcpy(buffer_ + size_, data, size);
    size_ += size;
    return *this;
  }

  // Return whether "this" == "rhs" or not.
  bool operator==(const String &rhs) const {
    return (size_ == rhs.size_) && (std::memcmp(data_, rhs.data_, size_) == 0);
  }
  // Return whether "this" != "rhs" or not.
  bool operator!=(const String &rhs) const {
    return (size_ != rhs.size_) || (std::memcmp(data_, rhs.data_, size_) != 0);
  }
  // Return whether "this" < "rhs" or not.
  bool operator<(const String &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ < rhs.size_));
  }
  // Return whether "this" > "rhs" or not.
  bool operator>(const String &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ > rhs.size_));
  }
  // Return whether "this" <= "rhs" or not.
  bool operator<=(const String &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ <= rhs.size_));
  }
  // Return whether "this" >= "rhs" or not.
  bool operator>=(const String &rhs) const {
    size_t min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result > 0) || ((result == 0) && (size_ >= rhs.size_));
  }

  // Return whether "this" == "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
  bool operator==(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if ((rhs[i] == '\0') || (data_[i] != rhs[i])) {
        return false;
      }
    }
    return rhs[size_] == '\0';
  }
  // Return whether "this" != "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
  bool operator!=(const char *rhs) const {
    for (size_t i = 0; i < size_; ++i) {
      if ((rhs[i] == '\0') || (data_[i] != rhs[i])) {
        return true;
      }
    }
    return rhs[size_] != '\0';
  }
  // Return whether "this" < "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
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
  // Return whether "this" > "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
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
  // Return whether "this" <= "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
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
  // Return whether "this" >= "rhs" or not.
  //
  // If "rhs" == nullptr, the behavior is undefined.
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

  // Return whether "this" starts with "rhs" or not.
  bool starts_with(const String &rhs) const {
    if (size_ < rhs.size_) {
      return false;
    }
    return std::memcmp(data_, rhs.data_, rhs.size_) == 0;
  }
  // Return whether "this" starts with "rhs" or not.
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

  // Return whether "this" ends with "rhs" or not.
  bool ends_with(const String &rhs) const {
    if (size_ < rhs.size_) {
      return false;
    }
    return std::memcmp(data_ + size_ - rhs.size_, rhs.data_, rhs.size_) == 0;
  }
  // Return whether "this" ends with "rhs" or not.
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
  union {
    char *buffer_;
    const char *data_;
  };
  size_t size_;
  size_t capacity_;

  // Resize the buffer for at least "new_size".
  //
  // Assumes that "new_size" is greater than "capacity_".
  //
  // On failure, throws an exception.
  void resize_buffer(size_t new_size);

  // Resize the buffer and append a part of "this".
  //
  // On failure, throws an exception.
  void append_overlap(const char *data, size_t size);
};

// Return whether "lhs" == "rhs" or not.
inline bool operator==(const char *lhs, const String &rhs) {
  return rhs == lhs;
}
// Return whether "lhs" != "rhs" or not.
inline bool operator!=(const char *lhs, const String &rhs) {
  return rhs != lhs;
}
// Return whether "lhs" < "rhs" or not.
inline bool operator<(const char *lhs, const String &rhs) {
  return rhs > lhs;
}
// Return whether "lhs" > "rhs" or not.
inline bool operator>(const char *lhs, const String &rhs) {
  return rhs < lhs;
}
// Return whether "lhs" <= "rhs" or not.
inline bool operator<=(const char *lhs, const String &rhs) {
  return rhs >= lhs;
}
// Return whether "lhs" >= "rhs" or not.
inline bool operator>=(const char *lhs, const String &rhs) {
  return rhs <= lhs;
}

}  // namespace grnxx

#endif  // GRNXX_STRING_HPP
