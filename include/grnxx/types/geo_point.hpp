#ifndef GRNXX_TYPES_GEO_POINT_HPP
#define GRNXX_TYPES_GEO_POINT_HPP

#include "grnxx/types/geo_point.hpp"

namespace grnxx {

class GeoPoint {
 public:
  // The default constructor does nothing.
  GeoPoint() = default;
  GeoPoint(Int latitude, Int longitude)
      : latitude_(),
        longitude_() {
    if ((latitude < DEGREES(-90)) || (latitude > DEGREES(90)) ||
        (longitude < DEGREES(-180)) || (longitude >= DEGREES(180))) {
      // Fix an out-of-range value.
      fix(&latitude, &longitude);
    }
    // The south pole or the north pole.
    if ((latitude == DEGREES(-90)) || (latitude == DEGREES(90))) {
      longitude = 0;
    }
    latitude_ = static_cast<int32_t>(latitude);
    longitude_ = static_cast<int32_t>(longitude);
  }

  bool operator==(const GeoPoint &arg) const {
    return (latitude_ == arg.latitude_) && (longitude_ == arg.longitude_);
  }
  bool operator!=(const GeoPoint &arg) const {
    return (latitude_ != arg.latitude_) || (longitude_ != arg.longitude_);
  }

  Int latitude() const {
    return latitude_;
  }
  Int longitude() const {
    return longitude_;
  }

 private:
  int32_t latitude_;   // Latitude in milliseconds.
  int32_t longitude_;  // Longitude in milliseconds.

  // Return "value" degrees in milliseconds.
  static constexpr Int DEGREES(Int value) {
    return value * 60 * 60 * 1000;
  }

  // Fix an out-of-range value.
  static void fix(Int *latitude, Int *longitude);
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_GEO_POINT_HPP
