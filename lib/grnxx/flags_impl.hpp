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
#ifndef GRNXX_FLAGS_IMPL_HPP
#define GRNXX_FLAGS_IMPL_HPP

#include "grnxx/basic.hpp"

namespace grnxx {

template <typename T, typename U = uint32_t>
class FlagsImpl {
 public:
  using Identifier = T;
  using Type = U;

  FlagsImpl() = default;

  constexpr explicit operator bool() {
    return flags_ != 0;
  }

  constexpr FlagsImpl operator&(FlagsImpl rhs) {
    return FlagsImpl(flags_ & rhs.flags_);
  }
  constexpr FlagsImpl operator|(FlagsImpl rhs) {
    return FlagsImpl(flags_ | rhs.flags_);
  }
  constexpr FlagsImpl operator^(FlagsImpl rhs) {
    return FlagsImpl(flags_ ^ rhs.flags_);
  }
  constexpr FlagsImpl operator~() {
    return FlagsImpl(~flags_);
  }

  constexpr bool operator==(FlagsImpl rhs) {
    return flags_ == rhs.flags_;
  }
  constexpr bool operator!=(FlagsImpl rhs) {
    return flags_ == rhs.flags_;
  }

  FlagsImpl &operator&=(FlagsImpl rhs) {
    flags_ &= rhs.flags_;
    return *this;
  }
  FlagsImpl &operator|=(FlagsImpl rhs) {
    flags_ |= rhs.flags_;
    return *this;
  }
  FlagsImpl &operator^=(FlagsImpl rhs) {
    flags_ ^= rhs.flags_;
    return *this;
  }

  static constexpr FlagsImpl none() {
    return FlagsImpl(0);
  }
  static constexpr FlagsImpl define(Type flags) {
    return FlagsImpl(flags);
  }

 private:
  Type flags_;

  explicit constexpr FlagsImpl(Type flags) : flags_(flags) {}
};

}  // namespace grnxx

#endif  // GRNXX_FLAGS_IMPL_HPP
