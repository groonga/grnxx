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
#ifndef GRNXX_BASIC_HPP
#define GRNXX_BASIC_HPP

#include "features.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iosfwd>
#include <limits>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace grnxx {

using std::size_t;

using std::int8_t;
using std::int16_t;
using std::int32_t;
using std::int64_t;

using std::uint8_t;
using std::uint16_t;
using std::uint32_t;
using std::uint64_t;

using std::intptr_t;
using std::uintptr_t;

// TODO: This should use std::is_pod. However, gcc-4.7 uses the C++03 concept.
#define GRNXX_ASSERT_POD(type)\
  static_assert(std::is_trivial<type>::value &&\
                std::is_standard_layout<type>::value,\
                #type " is not a POD type")

}  // namespace grnxx

#endif  // GRNXX_BASIC_HPP
