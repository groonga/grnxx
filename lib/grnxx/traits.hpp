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
#ifndef GRNXX_TRAITS_HPP
#define GRNXX_TRAITS_HPP

#include "grnxx/features.hpp"

#include <type_traits>

namespace grnxx {

class GeoPoint;

// A simple/complex type should use pass by value/reference.
template <typename T>
struct PreferredArgument {
  using Type = typename std::conditional<std::is_scalar<T>::value,
                                         T, const T &>::type;
};
// GeoPoint is not a scalar type but a simple type.
template <> struct PreferredArgument<GeoPoint> {
  using Type = GeoPoint;
};

//// Check if operator<() is defined or not.
//struct HasLessHelper {
//  template <typename T>
//  static auto check(T *p) -> decltype(p->operator<(*p), std::true_type());
//  template <typename T>
//  static auto check(T *p) -> decltype(operator<(*p, *p), std::true_type());
//  template <typename>
//  static auto check(...) -> decltype(std::false_type());
//};
//// Check if T has operator<() or not.
//template <typename T>
//struct HasLess {
//  static constexpr bool value() {
//    return std::conditional<std::is_scalar<T>::value, std::true_type,
//                            decltype(HasLessHelper::check<T>(0))>::type::value;
//  }
//};

//// Check if T has starts_with() or not.
//struct HasStartsWithHelper {
//  template <typename T>
//  static auto check(T *p) -> decltype(p->starts_with(*p), std::true_type());
//  template <typename>
//  static auto check(...) -> decltype(std::false_type());
//};
//// Check if T has operator<() or not.
//template <typename T>
//struct HasStartsWith {
//  static constexpr bool value() {
//    return decltype(HasStartsWithHelper::check<T>(0))::value;
//  }
//};

// Type traits.
template <typename T>
struct Traits {
  using Type = T;
  using ArgumentType = typename PreferredArgument<T>::Type;

//  static constexpr bool has_less() {
//    return HasLess<T>::value();
//  }
//  static constexpr bool has_starts_with() {
//    return HasStartsWith<T>::value();
//  }
};

}  // namespace grnxx

#endif  // GRNXX_TRAITS_HPP
