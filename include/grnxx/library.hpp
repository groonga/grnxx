#ifndef GRNXX_LIBRARY_HPP
#define GRNXX_LIBRARY_HPP

namespace grnxx {

class Library {
 public:
  // Return the library package name.
  static const char *package();

  // Return the library version.
  static const char *version();
};

}  // namespace grnxx

#endif  // GRNXX_LIBRARY_HPP
