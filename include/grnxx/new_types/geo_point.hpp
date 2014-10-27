#ifndef GRNXX_NEW_TYPES_GEO_POINT_HPP
#define GRNXX_NEW_TYPES_GEO_POINT_HPP

#include <cstdint>

#include "grnxx/new_types/int.hpp"
#include "grnxx/new_types/na.hpp"

namespace grnxx {

class GeoPoint {
 public:
  GeoPoint() = default;
  ~GeoPoint() = default;

  // NOTE: GeoPoint uses int32_t to represent latitude and longitude, but this
  //       constructor takes int64_t in order to avoid problems caused by
  //       implicit type conversion from int64_t to int32_t.
  // NOTE: gcc-4.8 does not support use of the value being constructed in
  //       a constant exression.
  GeoPoint(int64_t latitude, int64_t longitude)
      : latitude_(((latitude < min_latitude()) ||
                   (latitude > max_latitude()) ||
                   (longitude < min_longitude()) ||
                   (longitude > max_longitude())) ?
                  na_latitude() : latitude),
        longitude_((latitude_ == na_latitude()) ?
                   na_longitude() : longitude) {}
  // NOTE: This implementation assumes that Int::na_value() is less than
  //       GeoPoint::min_latitude() and GeoPoint::min_longitude.
  //       If this assumption is wrong, N/A must be excluded.
  // NOTE: gcc-4.8 does not support use of the value being constructed in
  //       a constant exression.
  GeoPoint(Int latitude, Int longitude)
      : latitude_(((latitude.value() < min_latitude()) ||
                   (latitude.value() > max_latitude()) ||
                   (longitude.value() < min_longitude()) ||
                   (longitude.value() > max_longitude())) ?
                  na_latitude() : latitude.value()),
        longitude_((latitude_ == na_latitude()) ?
                   na_longitude() : longitude.value()) {}
  explicit constexpr GeoPoint(NA)
      : latitude_(na_latitude()),
        longitude_(na_longitude()) {}

  constexpr int32_t latitude() const {
    return latitude_;
  }
  constexpr int32_t longitude() const {
    return longitude_;
  }

  constexpr bool is_na() const {
    return latitude_ == na_latitude();
  }

  constexpr Bool operator==(const GeoPoint &rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() :
           Bool((latitude_ == rhs.latitude_) &&
                (longitude_ == rhs.longitude_));
  }
  constexpr Bool operator!=(const GeoPoint &rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() :
           Bool((latitude_ != rhs.latitude_) ||
                (longitude_ != rhs.longitude_));
  }

  static constexpr int32_t min_latitude() {
    return degrees(-90);
  }
  static constexpr int32_t max_latitude() {
    return degrees(90);
  }
  static constexpr int32_t na_latitude() {
    return std::numeric_limits<int32_t>::min();
  }
  static constexpr int32_t min_longitude() {
    return degrees(-180);
  }
  static constexpr int32_t max_longitude() {
    return degrees(180);
  }
  static constexpr int32_t na_longitude() {
    return std::numeric_limits<int32_t>::min();
  }

 private:
  int32_t latitude_;   // Latitude in milliseconds.
  int32_t longitude_;  // Longitude in milliseconds.

  static constexpr int32_t degrees(int32_t value) {
    return value * 60 * 60 * 1000;
  }
};

}  // namespace grnxx

#endif  // GRNXX_NEW_TYPES_GEO_POINT_HPP
