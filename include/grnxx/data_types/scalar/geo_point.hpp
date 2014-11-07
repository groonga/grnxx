#ifndef GRNXX_DATA_TYPES_SCALAR_GEO_POINT_HPP
#define GRNXX_DATA_TYPES_SCALAR_GEO_POINT_HPP

#include <cstdint>
#include <limits>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar/float.hpp"
#include "grnxx/data_types/scalar/int.hpp"

namespace grnxx {

// NOTE: GeoPoint uses int32_t to represent latitude and longitude, but uses
//       int64_t for I/O in order to avoid problems caused by int32_t overflow
//       and implicit type conversion from int64_t to int32_t.
class GeoPoint {
 public:
  GeoPoint() = default;
  ~GeoPoint() = default;

  // NOTE: The following implementation assumes that Int::na()::value() returns
  //       a value less than min_latitude/longitude() or greater than
  //       max_latitude/longitude().
  GeoPoint(Int latitude_in_milliseconds, Int longitude_in_milliseconds)
      : latitude_(),
        longitude_() {
    int64_t latitude = latitude_in_milliseconds.value();
    int64_t longitude = longitude_in_milliseconds.value();
    if ((latitude >= min_latitude()) && (latitude <= max_latitude()) &&
        (longitude >= min_longitude()) && (longitude <= max_longitude())) {
      if ((latitude == min_latitude()) || (latitude == max_latitude())) {
        longitude = 0;
      } else if (longitude == max_longitude()) {
        longitude = min_longitude();
      }
      latitude_ = latitude;
      longitude_ = longitude;
    } else {
      latitude_ = na_latitude();
      longitude_ = na_longitude();
    }
  }
  GeoPoint(Float latitude_in_degrees, Float longitude_in_degrees)
      : latitude_(),
        longitude_() {
    // N/A (NaN) is rejected due to LOGICAL_AND.
    if ((latitude_in_degrees.value() >= -90.0) &&
        (latitude_in_degrees.value() <= 90.0) &&
        (longitude_in_degrees.value() <= -180.0) &&
        (longitude_in_degrees.value() <= 180.0)) {
      int64_t latitude = latitude_in_degrees.value() * 60 * 60 * 1000;
      int64_t longitude = longitude_in_degrees.value() * 60 * 60 * 1000;
      if ((latitude <= min_latitude()) || (latitude >= max_latitude())) {
        longitude = 0;
      } else if (longitude == max_longitude()) {
        longitude = min_longitude();
      }
      latitude_ = latitude;
      longitude_ = longitude;
    } else {
      latitude_ = na_latitude();
      longitude_ = na_longitude();
    }
  }
  explicit constexpr GeoPoint(NA)
      : latitude_(na_latitude()),
        longitude_(na_longitude()) {}

  constexpr int64_t latitude() const {
    return latitude_;
  }
  constexpr Int latitude_in_milliseconds() const {
    return in_milliseconds(latitude_);
  }
  constexpr Float latitude_in_degrees() const {
    return in_degrees(latitude_);
  }

  constexpr int64_t longitude() const {
    return longitude_;
  }
  constexpr Int longitude_in_milliseconds() const {
    return in_milliseconds(longitude_);
  }
  constexpr Float longitude_in_degrees() const {
    return in_degrees(longitude_);
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

  static constexpr DataType type() {
    return GEO_POINT_DATA;
  }

  static constexpr GeoPoint na() {
    return GeoPoint(NA());
  }

  static constexpr int64_t min_latitude() {
    return degrees(-90);
  }
  static constexpr Int min_latitude_in_milliseconds() {
    return in_milliseconds(min_latitude());
  }
  static constexpr Float min_latitude_in_degrees() {
    return in_degrees(min_latitude());
  }
  static constexpr int64_t max_latitude() {
    return degrees(90);
  }
  static constexpr Int max_latitude_in_milliseconds() {
    return in_milliseconds(max_latitude());
  }
  static constexpr Float max_latitude_in_degrees() {
    return in_degrees(max_latitude());
  }
  static constexpr int64_t na_latitude() {
    return na_value();
  }

  static constexpr int64_t min_longitude() {
    return degrees(-180);
  }
  static constexpr Int min_longitude_in_milliseconds() {
    return in_milliseconds(min_longitude());
  }
  static constexpr Float min_longitude_in_degrees() {
    return in_degrees(min_longitude());
  }
  static constexpr int64_t max_longitude() {
    return degrees(180);
  }
  static constexpr Int max_longitude_in_milliseconds() {
    return in_milliseconds(max_longitude());
  }
  static constexpr Float max_longitude_in_degrees() {
    return in_degrees(max_longitude());
  }
  static constexpr int64_t na_longitude() {
    return na_value();
  }

 private:
  int32_t latitude_;   // Latitude in milliseconds.
  int32_t longitude_;  // Longitude in milliseconds.

  // Return a value that indicates N/A.
  static constexpr int64_t na_value() {
    return std::numeric_limits<int32_t>::min();
  }

  // Return the number of milliseconds for "n" degrees.
  static constexpr int64_t degrees(int64_t n) {
    return n * 60 * 60 * 1000;
  }

  // Express "value" in milliseconds with Int.
  static constexpr Int in_milliseconds(int64_t value) {
    return (value == na_value()) ? Int::na() : Int(value);
  }
  // Express "value" in degrees with Float.
  static constexpr Float in_degrees(int64_t value) {
    return (value == na_value()) ?
           Float::na() : Float(value / double(60 * 60 * 1000));
  }
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_GEO_POINT_HPP
