#ifndef GRNXX_NAME_HPP
#define GRNXX_NAME_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Name {
 public:
  Name() : string_() {}

  const char &operator[](Int i) const {
    return string_[i];
  }

  const char *data() const {
    return string_.data();
  }
  Int size() const {
    return string_.size();
  }

  // Return a reference to the name string.
  StringCRef ref() const {
    return string_.ref();
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
  bool assign(Error *error, const StringCRef &name);

 private:
  String string_;

  static constexpr Int MIN_SIZE = 1;
  static constexpr Int MAX_SIZE = 1023;

  // Check if "name" is valid as an object name or not.
  //
  // Returns true if "name" is valid.
  // Otherwise, returns false and stores error information into "*error" if
  // "error" != nullptr.
  static bool test(Error *error, const StringCRef &name);
};

}  // namespace grnxx

#endif  // GRNXX_NAME_HPP
