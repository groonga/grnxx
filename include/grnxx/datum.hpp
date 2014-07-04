#ifndef GRNXX_DATUM_HPP
#define GRNXX_DATUM_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Datum {
 public:
  // The default constructor does nothing.
  Datum() = default;

  Datum(Bool value)
      : type_(BOOL_DATA),
        bool_(value) {}
  Datum(Int value)
      : type_(INT_DATA),
        int_(value) {}
  Datum(Float value)
      : type_(FLOAT_DATA),
        float_(value) {}
  Datum(Time value)
      : type_(TIME_DATA),
        time_(value) {}
  Datum(GeoPoint value)
      : type_(GEO_POINT_DATA),
        geo_point_(value) {}
  Datum(String value)
      : type_(TEXT_DATA),
        text_(value) {}

  // Return the data type.
  DataType type() const {
    return type_;
  }

  // TODO: Typecast operators are dangerous.
  //       These should not be used.
  //
  // Force the specified interpretation.
  explicit operator Bool() const {
    return bool_;
  }
  explicit operator Int() const {
    return int_;
  }
  explicit operator Float() const {
    return float_;
  }
  explicit operator Time() const {
    return time_;
  }
  explicit operator GeoPoint() const {
    return geo_point_;
  }
  explicit operator Text() const {
    return text_;
  }

 private:
  DataType type_;
  union {
    Bool bool_;
    Int int_;
    Float float_;
    Time time_;
    GeoPoint geo_point_;
    String text_;
  };
};

}  // namespace grnxx

#endif  // GRNXX_DATUM_HPP
