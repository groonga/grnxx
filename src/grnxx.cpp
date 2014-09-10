#include <getopt.h>
#include <unistd.h>

#include <iostream>

#include "grnxx/library.hpp"

namespace {

void print_version() {
  std::cout << grnxx::Library::package() << ' '
            << grnxx::Library::version();
  // TODO: Enumerate valid options.
//  std::cout << "\n\noptions:";
  std::cout << std::endl;
}

void print_usage(const char *name) {
  std::cout << "Usage: " << name << " [OPTION]...\n\n"
               "Options:\n"
               "  -h, --help:     print this help\n"
               "  -v, --version:  print grnxx version\n"
            << std::flush;
}

}  // namespace

int main(int argc, char *argv[]) {
  const struct option long_options[] = {
    { "help", 0, nullptr, 'h' },
    { "version", 0, nullptr, 'v' },
    { nullptr, 0, nullptr, 0 }
  };
  int value;
  while ((value = ::getopt_long(argc, argv, "hv",
                                long_options, nullptr)) != -1) {
    switch (value) {
      case 'h': {
        print_usage(argv[0]);
        return 0;
      }
      case 'v': {
        print_version();
        return 0;
      }
      default: {
        print_usage(argv[0]);
        return 1;
      }
    }
  }
  return 0;
}
