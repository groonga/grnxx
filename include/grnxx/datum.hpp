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

  // Force the specified interpretation.
  Bool force_bool() const {
    return bool_;
  }
  Int force_int() const {
    return int_;
  }
  Float force_float() const {
    return float_;
  }
  GeoPoint force_geo_point() const {
    return geo_point_;
  }
  Text force_text() const {
    return text_;
  }

  // Force the specified interpretation.
  void force(Bool *value) const {
    *value = bool_;
  }
  void force(Int *value) const {
    *value = int_;
  }
  void force(Float *value) const {
    *value = float_;
  }
  void force(GeoPoint *value) const {
    *value = geo_point_;
  }
  void force(Text *value) const {
    *value = text_;
  }

 private:
  DataType type_;
  union {
    Bool bool_;
    Int int_;
    Float float_;
    GeoPoint geo_point_;
    String text_;
  };
};

}  // namespace grnxx

#endif  // GRNXX_DATUM_HPP
