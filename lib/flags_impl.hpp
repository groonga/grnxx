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

#include "basic.hpp"

namespace grnxx {

template <typename T, typename U = uint32_t>
class FlagsImpl {
 public:
  typedef T Identifier;
  typedef U Type;

  GRNXX_CONSTEXPR FlagsImpl() : flags_(0) {}
  GRNXX_CONSTEXPR FlagsImpl(const FlagsImpl &flags) : flags_(flags.flags_) {}

  GRNXX_CONSTEXPR GRNXX_EXPLICIT_CONVERSION operator bool() const {
    return flags_ != 0;
  }

  GRNXX_CONSTEXPR FlagsImpl operator&(FlagsImpl rhs) const {
    return FlagsImpl(flags_ & rhs.flags_);
  }
  GRNXX_CONSTEXPR FlagsImpl operator|(FlagsImpl rhs) const {
    return FlagsImpl(flags_ | rhs.flags_);
  }
  GRNXX_CONSTEXPR FlagsImpl operator^(FlagsImpl rhs) const {
    return FlagsImpl(flags_ ^ rhs.flags_);
  }
  GRNXX_CONSTEXPR FlagsImpl operator~() const {
    return FlagsImpl(~flags_);
  }

  GRNXX_CONSTEXPR bool operator==(FlagsImpl rhs) const {
    return flags_ == rhs.flags_;
  }
  GRNXX_CONSTEXPR bool operator!=(FlagsImpl rhs) const {
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

  static GRNXX_CONSTEXPR FlagsImpl define(Type flags) {
    return FlagsImpl(flags);
  }

 private:
  Type flags_;

  explicit GRNXX_CONSTEXPR FlagsImpl(Type flags) : flags_(flags) {}
};

}  // namespace grnxx

#endif  // GRNXX_FLAGS_IMPL_HPP
