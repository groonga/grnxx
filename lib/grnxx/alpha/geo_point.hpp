/*
  Copyright (C) 2013  Brazil, Inc.

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
#ifndef GRNXX_ALPHA_GEO_POINT_HPP
#define GRNXX_ALPHA_GEO_POINT_HPP

#include "grnxx/basic.hpp"
#include "grnxx/string_builder.hpp"

namespace grnxx {
namespace alpha {

// Latitude and longitude (lat/long).
union GeoPoint {
 public:
  // The default constructor does not initialize lat/long.
  GeoPoint() = default;
  // Copy lat/long as uint64_t to force atomic copy.
  GeoPoint(const GeoPoint &x) : value_(x.value_) {}
  // Copy lat/long.
  GeoPoint(int32_t latitude, int32_t longitude)
    : point_{ latitude, longitude } {}

  // Assign lat/long as uint64_t to force atomic assignment.
  GeoPoint &operator=(const GeoPoint &x) {
    value_ = x.value_;
    return *this;
  }

  // Get the latitude.
  int32_t latitude() const {
    return point_.latitude;
  }
  // Get the longitude.
  int32_t longitude() const {
    return point_.longitude;
  }
  // Get lat/long as uint64_t.
  uint64_t value() const {
    return value_;
  }

  // Set the latitude.
  void set_latitude(int32_t x) {
    point_.latitude = x;
  }
  // Set the longitude.
  void set_longitude(int32_t x) {
    point_.longitude = x;
  }
  // Set lat/long as uint64_t.
  void set_value(uint64_t x) {
    value_ = x;
  }

  StringBuilder &write_to(StringBuilder &builder) const;

 private:
  struct Point {
    int32_t latitude;
    int32_t longitude;
  } point_;
  uint64_t value_;
};

inline bool operator==(const GeoPoint &lhs, const GeoPoint &rhs) {
  return lhs.value() == rhs.value();
}
inline bool operator!=(const GeoPoint &lhs, const GeoPoint &rhs) {
  return lhs.value() != rhs.value();
}

inline StringBuilder &operator<<(StringBuilder &builder, const GeoPoint &x) {
  return x.write_to(builder);
}

}  // namespace alpha
}  // namespace grnxx

#endif  // GRNXX_ALPHA_GEO_POINT_HPP
