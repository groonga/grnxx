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
#include "grnxx/os.hpp"

#include <cstdlib>
#include <cstring>
#include <cerrno>

#include "grnxx/error.hpp"
#include "grnxx/lock.hpp"
#include "grnxx/logger.hpp"

namespace grnxx {

uint64_t OS::get_page_size() {
#if defined(GRNXX_WINDOWS) || !defined(_SC_PAGESIZE)
  static const uint64_t page_size = 4096;
#else  // defined(GRNXX_WINDOWS) || !defined(_SC_PAGESIZE)
  static const uint64_t page_size = ::sysconf(_SC_PAGESIZE);
#endif  // defined(GRNXX_WINDOWS) || !defined(_SC_PAGESIZE)
  return page_size;
}

char *OS::get_environment_variable(const char *name) {
  if (!name) {
    GRNXX_ERROR() << "invalid argument: name = nullptr";
    return nullptr;
  }
  static Mutex mutex(MUTEX_UNLOCKED);
  Lock lock(&mutex);
#ifdef GRNXX_MSC
  char *value;
  size_t value_size;
  if (::_dupenv_s(&value, &value_size, name) != 0) {
    GRNXX_ERROR() << "failed to get environment variable: name = " << name
                  << ": '::_dupenv_s' " << Error(errno);
    return nullptr;
  }
  char * const result = new (std::nothrow) char[value_size + 1];
  if (!result) {
    GRNXX_ERROR() << "new char[] failed: size = " << (value_size + 1);
    std::free(value);
    return nullptr;
  }
  std::memcpy(result, value, value_size);
  result[value_size] = '\0';
  std::free(value);
  return result;
#else  // GRNXX_MSC
  char * const value = std::getenv(name);
  if (!value) {
    return nullptr;
  }
  const size_t value_size = std::strlen(value);
  char * const result = new (std::nothrow) char[value_size + 1];
  if (!result) {
    GRNXX_ERROR() << "new char[] failed: size = " << (value_size + 1);
    return nullptr;
  }
  std::memcpy(result, value, value_size);
  result[value_size] = '\0';
  return result;
#endif  // GRNXX_MSC
}

}  // namespace grnxx
