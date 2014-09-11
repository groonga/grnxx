#ifndef GRNXX_NAME_HPP
#define GRNXX_NAME_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Name {
 public:
  Name() : data_(nullptr), size_(0) {}

  Name(const Name &) = delete;
  Name &operator=(const Name &) = delete;

  const char &operator[](Int i) const {
    return data_[i];
  }

  const char *data() const {
    return data_.get();
  }
  Int size() const {
    return size_;
  }

  // Return a reference to the name string.
  String ref() const {
    return String(data_.get(), size_);
  }

  // Assign a new name.
  // Allocates memory and copies the given name to it.
  // Then, terminates the copied name with a null character '\0'.
  //
  // A name string can contain alphabets (A-Za-z), digits (0-9), and
  // underscores (_).
  //
  // Returns true on success.
  // On failure, returns false and stores error information into "*error" if
  // "error" != nullptr.
  bool assign(Error *error, String name);

 private:
  unique_ptr<char[]> data_;
  Int size_;

  static constexpr Int MIN_SIZE = 1;
  static constexpr Int MAX_SIZE = 1023;

  // Check if "name" is valid as an object name or not.
  //
  // Returns true if "name" is valid.
  // Otherwise, returns false and stores error information into "*error" if
  // "error" != nullptr.
  static bool test(Error *error, String name);
};

}  // namespace grnxx

#endif  // GRNXX_NAME_HPP
