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

  GeoPoint(const GeoPoint &) = default;
  GeoPoint &operator=(const GeoPoint &) = default;

  // NOTE: The following implementation assumes that Int::na()::raw() returns
  //       a value less than min_latitude/longitude() or greater than
  //       max_latitude/longitude().
  GeoPoint(Int latitude_in_milliseconds, Int longitude_in_milliseconds)
      : raw_latitude_(),
        raw_longitude_() {
    int64_t raw_latitude = latitude_in_milliseconds.raw();
    int64_t raw_longitude = longitude_in_milliseconds.raw();
    if ((raw_latitude >= raw_min_latitude()) &&
        (raw_latitude <= raw_max_latitude()) &&
        (raw_longitude >= raw_min_longitude()) &&
        (raw_longitude <= raw_max_longitude())) {
      if ((raw_latitude == raw_min_latitude()) ||
          (raw_latitude == raw_max_latitude())) {
        raw_longitude = 0;
      } else if (raw_longitude == raw_max_longitude()) {
        raw_longitude = raw_min_longitude();
      }
      raw_latitude_ = static_cast<int32_t>(raw_latitude);
      raw_longitude_ = static_cast<int32_t>(raw_longitude);
    } else {
      raw_latitude_ = static_cast<int32_t>(raw_na_latitude());
      raw_longitude_ = static_cast<int32_t>(raw_na_longitude());
    }
  }
  GeoPoint(Float latitude_in_degrees, Float longitude_in_degrees)
      : raw_latitude_(),
        raw_longitude_() {
    // N/A (NaN) is rejected because a comparison for NaN returns false.
    if ((latitude_in_degrees.raw() >= -90.0) &&
        (latitude_in_degrees.raw() <= 90.0) &&
        (longitude_in_degrees.raw() >= -180.0) &&
        (longitude_in_degrees.raw() <= 180.0)) {
      int64_t raw_latitude = latitude_in_degrees.raw() * 60 * 60 * 1000;
      int64_t raw_longitude = longitude_in_degrees.raw() * 60 * 60 * 1000;
      if ((raw_latitude == raw_min_latitude()) ||
          (raw_latitude == raw_max_latitude())) {
        raw_longitude = 0;
      } else if (raw_longitude == raw_max_longitude()) {
        raw_longitude = raw_min_longitude();
      }
      raw_latitude_ = raw_latitude;
      raw_longitude_ = raw_longitude;
    } else {
      raw_latitude_ = raw_na_latitude();
      raw_longitude_ = raw_na_longitude();
    }
  }
  explicit constexpr GeoPoint(NA)
      : raw_latitude_(raw_na_latitude()),
        raw_longitude_(raw_na_longitude()) {}

  constexpr int64_t raw_latitude() const {
    return raw_latitude_;
  }
  constexpr Int latitude_in_milliseconds() const {
    return in_milliseconds(raw_latitude_);
  }
  constexpr Float latitude_in_degrees() const {
    return in_degrees(raw_latitude_);
  }

  constexpr int64_t raw_longitude() const {
    return raw_longitude_;
  }
  constexpr Int longitude_in_milliseconds() const {
    return in_milliseconds(raw_longitude_);
  }
  constexpr Float longitude_in_degrees() const {
    return in_degrees(raw_longitude_);
  }

  constexpr bool is_na() const {
    return raw_latitude_ == raw_na_latitude();
  }

  // TODO: std::memcmp() might be better.
  constexpr Bool operator==(const GeoPoint &rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() :
           Bool((raw_latitude_ == rhs.raw_latitude_) &&
                (raw_longitude_ == rhs.raw_longitude_));
  }
  constexpr Bool operator!=(const GeoPoint &rhs) const {
    return (is_na() || rhs.is_na()) ? Bool::na() :
           Bool((raw_latitude_ != rhs.raw_latitude_) ||
                (raw_longitude_ != rhs.raw_longitude_));
  }

  // TODO: std::memcmp() might be better.
  constexpr bool match(const GeoPoint &rhs) const {
    return (raw_latitude_ == rhs.raw_latitude_) &&
           (raw_longitude_ == rhs.raw_longitude_);
  }
  constexpr bool unmatch(const GeoPoint &rhs) const {
    return (raw_latitude_ != rhs.raw_latitude_) ||
           (raw_longitude_ != rhs.raw_longitude_);
  }

  static constexpr DataType type() {
    return GEO_POINT_DATA;
  }

  static constexpr GeoPoint na() {
    return GeoPoint(NA());
  }

  static constexpr int64_t raw_min_latitude() {
    return degrees(-90);
  }
  static constexpr Int min_latitude_in_milliseconds() {
    return in_milliseconds(raw_min_latitude());
  }
  static constexpr Float min_latitude_in_degrees() {
    return in_degrees(raw_min_latitude());
  }
  static constexpr int64_t raw_max_latitude() {
    return degrees(90);
  }
  static constexpr Int max_latitude_in_milliseconds() {
    return in_milliseconds(raw_max_latitude());
  }
  static constexpr Float max_latitude_in_degrees() {
    return in_degrees(raw_max_latitude());
  }
  static constexpr int64_t raw_na_latitude() {
    return raw_na();
  }

  static constexpr int64_t raw_min_longitude() {
    return degrees(-180);
  }
  static constexpr Int min_longitude_in_milliseconds() {
    return in_milliseconds(raw_min_longitude());
  }
  static constexpr Float min_longitude_in_degrees() {
    return in_degrees(raw_min_longitude());
  }
  static constexpr int64_t raw_max_longitude() {
    return degrees(180);
  }
  static constexpr Int max_longitude_in_milliseconds() {
    return in_milliseconds(raw_max_longitude());
  }
  static constexpr Float max_longitude_in_degrees() {
    return in_degrees(raw_max_longitude());
  }
  static constexpr int64_t raw_na_longitude() {
    return raw_na();
  }

 private:
  int32_t raw_latitude_;   // Latitude in milliseconds.
  int32_t raw_longitude_;  // Longitude in milliseconds.

  // Return a value that indicates N/A.
  static constexpr int64_t raw_na() {
    return std::numeric_limits<int32_t>::min();
  }

  // Return the number of milliseconds for "n" degrees.
  static constexpr int64_t degrees(int64_t n) {
    return n * 60 * 60 * 1000;
  }

  // Express a raw value in milliseconds with Int.
  static constexpr Int in_milliseconds(int64_t raw) {
    return (raw == raw_na()) ? Int::na() : Int(raw);
  }
  // Express a raw value in degrees with Float.
  static constexpr Float in_degrees(int64_t raw) {
    return (raw == raw_na()) ?
           Float::na() : Float(raw / double(60 * 60 * 1000));
  }
};

}  // namespace grnxx

#endif  // GRNXX_DATA_TYPES_SCALAR_GEO_POINT_HPP
