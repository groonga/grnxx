/*
  Copyright (C) 2012  Brazil, Inc.

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
#ifndef GRNXX_BACKTRACE_HPP
#define GRNXX_BACKTRACE_HPP

#include <string>
#include <vector>

#include "grnxx/basic.hpp"

namespace grnxx {

constexpr int BACKTRACE_MIN_SKIP_COUNT = 0;
constexpr int BACKTRACE_MAX_SKIP_COUNT = 16;

constexpr size_t BACKTRACE_MIN_BUF_SIZE = 16;
constexpr size_t BACKTRACE_MAX_BUF_SIZE = 1024;

class Backtrace {
 public:
  // The following functions return true on success, false on failure.

  // backtrace() writes a list of addresses to function calls into 'addresses'.
  // 'skip_count' specfies the number of function calls to be skipped.
  static bool backtrace(int skip_count, std::vector<void *> *addresses);

  // resolve() writes a function call referred to by 'address' in
  // human-readable format into 'entry'.
  static bool resolve(void *address, std::string *entry);

  // pretty_backtrace() writes a list of function calls in human-readable
  // format. 'skip_count' specfies the number of function calls to be skipped.
  static bool pretty_backtrace(int skip_count,
                               std::vector<std::string> *entries);

 private:
  Backtrace();
  ~Backtrace();

  Backtrace(const Backtrace &);
  Backtrace &operator=(const Backtrace &);
};

}  // namespace grnxx

#endif  // GRNXX_BACKTRACE_HPP
