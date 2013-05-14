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
#ifndef GRNXX_GEO_POINT_HPP
#define GRNXX_GEO_POINT_HPP

#include "grnxx/features.hpp"

#include "grnxx/types.hpp"

namespace grnxx {

class StringBuilder;

// Latitude and longitude (lat/long).
union GeoPoint {
 public:
  // Trivial default constructor.
  GeoPoint() = default;
  // Copy the lat/long as uint64_t to force atomic copy.
  GeoPoint(const GeoPoint &x) : value_(x.value_) {}
  // Copy the lat/long.
  GeoPoint(int32_t latitude, int32_t longitude)
      : point_{ latitude, longitude } {}

  // Assign the lat/long as uint64_t to force atomic assignment.
  GeoPoint &operator=(const GeoPoint &x) {
    value_ = x.value_;
    return *this;
  }

  // Interleave the lat/long.
  uint64_t interleave() const;

  // Return the latitude.
  int32_t latitude() const {
    return point_.latitude;
  }
  // Return the longitude.
  int32_t longitude() const {
    return point_.longitude;
  }
  // Return the lat/long as uint64_t.
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
  // Set the lat/long as uint64_t.
  void set_value(uint64_t x) {
    value_ = x;
  }

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

StringBuilder &operator<<(StringBuilder &builder, const GeoPoint &point);

}  // namespace

#endif  // GRNXX_GEO_POINT_HPP
