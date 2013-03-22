// FIXME
#include <getopt.h>

#include <iostream>

#include "grnxx/grnxx.hpp"

namespace {

void print_version() {
  std::cout << "grnxx " << grnxx::Grnxx::version() << std::endl;
}

void print_usage() {
  std::cout << "Usage: grnxx [options...]\n\n"
            << "Options:\n"
            << "  -h, --help:     show usage\n"
            << "  -v, --version:  show grnxx version"
            << std::endl;
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
        print_usage();
        break;
      }
      case 'v': {
        print_version();
        return 0;
      }
      default: {
        break;
      }
    }
  }
  return 0;
}
