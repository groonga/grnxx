#ifndef GRNXX_TYPES_DATUM_HPP
#define GRNXX_TYPES_DATUM_HPP

#include <new>
#include <utility>

#include "grnxx/data_types/data_type.hpp"
#include "grnxx/data_types/na.hpp"
#include "grnxx/data_types/scalar.hpp"
#include "grnxx/data_types/vector.hpp"

namespace grnxx {

class Datum {
 public:
  Datum() : type_(NA_DATA), na_() {}
  ~Datum() {
    destruct();
  }

  Datum(const Datum &datum) : type_(), na_() {
    copy_from(datum);
  }
  Datum &operator=(const Datum &datum) & {
    if (type_ != datum.type_) {
      destruct();
      copy_from(datum);
      return *this;
    }
    switch (type_) {
      case NA_DATA: {
        na_ = datum.na_;
        break;
      }
      case BOOL_DATA: {
        bool_ = datum.bool_;
        break;
      }
      case INT_DATA: {
        int_ = datum.int_;
        break;
      }
      case FLOAT_DATA: {
        float_ = datum.float_;
        break;
      }
      case GEO_POINT_DATA: {
        geo_point_ = datum.geo_point_;
        break;
      }
      case TEXT_DATA: {
        text_ = datum.text_;
        break;
      }
      case BOOL_VECTOR_DATA: {
        bool_vector_ = datum.bool_vector_;
        break;
      }
      case INT_VECTOR_DATA: {
        int_vector_ = datum.int_vector_;
        break;
      }
      case FLOAT_VECTOR_DATA: {
        float_vector_ = datum.float_vector_;
        break;
      }
      case GEO_POINT_VECTOR_DATA: {
        geo_point_vector_ = datum.geo_point_vector_;
        break;
      }
      case TEXT_VECTOR_DATA: {
        text_vector_ = datum.text_vector_;
        break;
      }
    }
    return *this;
  }

  Datum(Datum &&datum) : type_(), na_() {
    move_from(std::move(datum));
  }
  Datum &operator=(Datum &&datum) & {
    if (type_ != datum.type_) {
      destruct();
      move_from(std::move(datum));
      return *this;
    }
    switch (type_) {
      case NA_DATA: {
        na_ = std::move(datum.na_);
        break;
      }
      case BOOL_DATA: {
        bool_ = std::move(datum.bool_);
        break;
      }
      case INT_DATA: {
        int_ = std::move(datum.int_);
        break;
      }
      case FLOAT_DATA: {
        float_ = std::move(datum.float_);
        break;
      }
      case GEO_POINT_DATA: {
        geo_point_ = std::move(datum.geo_point_);
        break;
      }
      case TEXT_DATA: {
        text_ = std::move(datum.text_);
        break;
      }
      case BOOL_VECTOR_DATA: {
        bool_vector_ = std::move(datum.bool_vector_);
        break;
      }
      case INT_VECTOR_DATA: {
        int_vector_ = std::move(datum.int_vector_);
        break;
      }
      case FLOAT_VECTOR_DATA: {
        float_vector_ = std::move(datum.float_vector_);
        break;
      }
      case GEO_POINT_VECTOR_DATA: {
        geo_point_vector_ = std::move(datum.geo_point_vector_);
        break;
      }
      case TEXT_VECTOR_DATA: {
        text_vector_ = std::move(datum.text_vector_);
        break;
      }
    }
    return *this;
  }

  // Create a N/A object.
  Datum(NA) : type_(NA_DATA), na_() {}
  // Create a Bool object.
  Datum(Bool value) : type_(BOOL_DATA), bool_(value) {}
  // Create an Int object.
  Datum(Int value) : type_(INT_DATA), int_(value) {}
  // Create a Float object.
  Datum(Float value) : type_(FLOAT_DATA), float_(value) {}
  // Create a GeoPoint object.
  Datum(GeoPoint value) : type_(GEO_POINT_DATA), geo_point_(value) {}
  // Create a Text object.
  Datum(const Text &value) : type_(TEXT_DATA), text_(value) {}
  // Create a Vector<Bool> object.
  Datum(const Vector<Bool> &value)
      : type_(BOOL_VECTOR_DATA),
        bool_vector_(value) {}
  // Create a Vector<Int> object.
  Datum(const Vector<Int> &value)
      : type_(INT_VECTOR_DATA),
        int_vector_(value) {}
  // Create a Vector<Float> object.
  Datum(const Vector<Float> &value)
      : type_(FLOAT_VECTOR_DATA),
        float_vector_(value) {}
  // Create a Vector<GeoPoint> object.
  Datum(const Vector<GeoPoint> &value)
      : type_(GEO_POINT_VECTOR_DATA),
        geo_point_vector_(value) {}
  // Create a Vector<Text> object.
  Datum(const Vector<Text> &value)
      : type_(TEXT_VECTOR_DATA),
        text_vector_(value) {}

  // Return the data type.
  DataType type() const {
    return type_;
  }

  // Access the content as Bool.
  const Bool &as_bool() const {
    return bool_;
  }
  // Access the content as Int.
  const Int &as_int() const {
    return int_;
  }
  // Access the content as Float.
  const Float &as_float() const {
    return float_;
  }
  // Access the content as GeoPoint.
  const GeoPoint &as_geo_point() const {
    return geo_point_;
  }
  // Access the content as Text.
  const Text &as_text() const {
    return text_;
  }
  // Access the content as Vector<Bool>.
  const Vector<Bool> &as_bool_vector() const {
    return bool_vector_;
  }
  // Access the content as Vector<Int>.
  const Vector<Int> &as_int_vector() const {
    return int_vector_;
  }
  // Access the content as Vector<Float>.
  const Vector<Float> &as_float_vector() const {
    return float_vector_;
  }
  // Access the content as Vector<GeoPoint>.
  const Vector<GeoPoint> &as_geo_point_vector() const {
    return geo_point_vector_;
  }
  // Access the content as Vector<Text>.
  const Vector<Text> &as_text_vector() const {
    return text_vector_;
  }

  // Access the content as Bool.
  Bool &as_bool() {
    return bool_;
  }
  // Access the content as Int.
  Int &as_int() {
    return int_;
  }
  // Access the content as Float.
  Float &as_float() {
    return float_;
  }
  // Access the content as GeoPoint.
  GeoPoint &as_geo_point() {
    return geo_point_;
  }
  // Access the content as Text.
  Text &as_text() {
    return text_;
  }
  // Access the content as Vector<Bool>.
  Vector<Bool> &as_bool_vector() {
    return bool_vector_;
  }
  // Access the content as Vector<Int>.
  Vector<Int> &as_int_vector() {
    return int_vector_;
  }
  // Access the content as Vector<Float>.
  Vector<Float> &as_float_vector() {
    return float_vector_;
  }
  // Access the content as Vector<GeoPoint>.
  Vector<GeoPoint> &as_geo_point_vector() {
    return geo_point_vector_;
  }
  // Access the content as Vector<Text>.
  Vector<Text> &as_text_vector() {
    return text_vector_;
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
  Vector<Bool> force_bool_vector() const {
    return bool_vector_;
  }
  Vector<Int> force_int_vector() const {
    return int_vector_;
  }
  Vector<Float> force_float_vector() const {
    return float_vector_;
  }
  Vector<GeoPoint> force_geo_point_vector() const {
    return geo_point_vector_;
  }
  Vector<Text> force_text_vector() const {
    return text_vector_;
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
  void force(Vector<Bool> *value) const {
    *value = bool_vector_;
  }
  void force(Vector<Int> *value) const {
    *value = int_vector_;
  }
  void force(Vector<Float> *value) const {
    *value = float_vector_;
  }
  void force(Vector<GeoPoint> *value) const {
    *value = geo_point_vector_;
  }
  void force(Vector<Text> *value) const {
    *value = text_vector_;
  }

 private:
  DataType type_;
  union {
    NA na_;
    Bool bool_;
    Int int_;
    Float float_;
    GeoPoint geo_point_;
    Text text_;
    Vector<Bool> bool_vector_;
    Vector<Int> int_vector_;
    Vector<Float> float_vector_;
    Vector<GeoPoint> geo_point_vector_;
    Vector<Text> text_vector_;
  };

  void destruct() {
    switch (type_) {
      case NA_DATA: {
        na_.~NA();
        break;
      }
      case BOOL_DATA: {
        bool_.~Bool();
        break;
      }
      case INT_DATA: {
        int_.~Int();
        break;
      }
      case FLOAT_DATA: {
        float_.~Float();
        break;
      }
      case GEO_POINT_DATA: {
        geo_point_.~GeoPoint();
        break;
      }
      case TEXT_DATA: {
        text_.~Text();
        break;
      }
      case BOOL_VECTOR_DATA: {
        bool_vector_.~BoolVector();
        break;
      }
      case INT_VECTOR_DATA: {
        int_vector_.~IntVector();
        break;
      }
      case FLOAT_VECTOR_DATA: {
        float_vector_.~FloatVector();
        break;
      }
      case GEO_POINT_VECTOR_DATA: {
        geo_point_vector_.~GeoPointVector();
        break;
      }
      case TEXT_VECTOR_DATA: {
        text_vector_.~TextVector();
        break;
      }
    }
  }
  void copy_from(const Datum &datum) {
    type_ = datum.type_;
    switch (type_) {
      case NA_DATA: {
        new (&na_) NA(datum.na_);
        break;
      }
      case BOOL_DATA: {
        new (&bool_) Bool(datum.bool_);
        break;
      }
      case INT_DATA: {
        new (&int_) Int(datum.int_);
        break;
      }
      case FLOAT_DATA: {
        new (&float_) Float(datum.float_);
        break;
      }
      case GEO_POINT_DATA: {
        new (&geo_point_) GeoPoint(datum.geo_point_);
        break;
      }
      case TEXT_DATA: {
        new (&text_) Text(datum.text_);
        break;
      }
      case BOOL_VECTOR_DATA: {
        new (&bool_vector_) BoolVector(datum.bool_vector_);
        break;
      }
      case INT_VECTOR_DATA: {
        new (&int_vector_) IntVector(datum.int_vector_);
        break;
      }
      case FLOAT_VECTOR_DATA: {
        new (&float_vector_) FloatVector(datum.float_vector_);
        break;
      }
      case GEO_POINT_VECTOR_DATA: {
        new (&geo_point_vector_) GeoPointVector(datum.geo_point_vector_);
        break;
      }
      case TEXT_VECTOR_DATA: {
        new (&text_vector_) TextVector(datum.text_vector_);
        break;
      }
    }
  }
  void move_from(Datum &&datum) {
    type_ = datum.type_;
    switch (type_) {
      case NA_DATA: {
        new (&na_) NA(std::move(datum.na_));
        break;
      }
      case BOOL_DATA: {
        new (&bool_) Bool(std::move(datum.bool_));
        break;
      }
      case INT_DATA: {
        new (&int_) Int(std::move(datum.int_));
        break;
      }
      case FLOAT_DATA: {
        new (&float_) Float(std::move(datum.float_));
        break;
      }
      case GEO_POINT_DATA: {
        new (&geo_point_) GeoPoint(std::move(datum.geo_point_));
        break;
      }
      case TEXT_DATA: {
        new (&text_) Text(std::move(datum.text_));
        break;
      }
      case BOOL_VECTOR_DATA: {
        new (&bool_vector_) BoolVector(std::move(datum.bool_vector_));
        break;
      }
      case INT_VECTOR_DATA: {
        new (&int_vector_) IntVector(std::move(datum.int_vector_));
        break;
      }
      case FLOAT_VECTOR_DATA: {
        new (&float_vector_) FloatVector(std::move(datum.float_vector_));
        break;
      }
      case GEO_POINT_VECTOR_DATA: {
        new (&geo_point_vector_)
            GeoPointVector(std::move(datum.geo_point_vector_));
        break;
      }
      case TEXT_VECTOR_DATA: {
        new (&text_vector_) TextVector(std::move(datum.text_vector_));
        break;
      }
    }
  }
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_DATUM_HPP
