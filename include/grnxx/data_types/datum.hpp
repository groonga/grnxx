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
  Datum() : type_(GRNXX_NA), na_() {}
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
      case GRNXX_NA: {
        na_ = datum.na_;
        break;
      }
      case GRNXX_BOOL: {
        bool_ = datum.bool_;
        break;
      }
      case GRNXX_INT: {
        int_ = datum.int_;
        break;
      }
      case GRNXX_FLOAT: {
        float_ = datum.float_;
        break;
      }
      case GRNXX_GEO_POINT: {
        geo_point_ = datum.geo_point_;
        break;
      }
      case GRNXX_TEXT: {
        text_ = datum.text_;
        break;
      }
      case GRNXX_BOOL_VECTOR: {
        bool_vector_ = datum.bool_vector_;
        break;
      }
      case GRNXX_INT_VECTOR: {
        int_vector_ = datum.int_vector_;
        break;
      }
      case GRNXX_FLOAT_VECTOR: {
        float_vector_ = datum.float_vector_;
        break;
      }
      case GRNXX_GEO_POINT_VECTOR: {
        geo_point_vector_ = datum.geo_point_vector_;
        break;
      }
      case GRNXX_TEXT_VECTOR: {
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
      case GRNXX_NA: {
        na_ = std::move(datum.na_);
        break;
      }
      case GRNXX_BOOL: {
        bool_ = std::move(datum.bool_);
        break;
      }
      case GRNXX_INT: {
        int_ = std::move(datum.int_);
        break;
      }
      case GRNXX_FLOAT: {
        float_ = std::move(datum.float_);
        break;
      }
      case GRNXX_GEO_POINT: {
        geo_point_ = std::move(datum.geo_point_);
        break;
      }
      case GRNXX_TEXT: {
        text_ = std::move(datum.text_);
        break;
      }
      case GRNXX_BOOL_VECTOR: {
        bool_vector_ = std::move(datum.bool_vector_);
        break;
      }
      case GRNXX_INT_VECTOR: {
        int_vector_ = std::move(datum.int_vector_);
        break;
      }
      case GRNXX_FLOAT_VECTOR: {
        float_vector_ = std::move(datum.float_vector_);
        break;
      }
      case GRNXX_GEO_POINT_VECTOR: {
        geo_point_vector_ = std::move(datum.geo_point_vector_);
        break;
      }
      case GRNXX_TEXT_VECTOR: {
        text_vector_ = std::move(datum.text_vector_);
        break;
      }
    }
    return *this;
  }

  // Create a N/A object.
  Datum(NA) : type_(GRNXX_NA), na_() {}
  // Create a Bool object.
  Datum(Bool value) : type_(GRNXX_BOOL), bool_(value) {}
  // Create an Int object.
  Datum(Int value) : type_(GRNXX_INT), int_(value) {}
  // Create a Float object.
  Datum(Float value) : type_(GRNXX_FLOAT), float_(value) {}
  // Create a GeoPoint object.
  Datum(GeoPoint value) : type_(GRNXX_GEO_POINT), geo_point_(value) {}
  // Create a Text object.
  Datum(const Text &value) : type_(GRNXX_TEXT), text_(value) {}
  // Create a Vector<Bool> object.
  Datum(const Vector<Bool> &value)
      : type_(GRNXX_BOOL_VECTOR),
        bool_vector_(value) {}
  // Create a Vector<Int> object.
  Datum(const Vector<Int> &value)
      : type_(GRNXX_INT_VECTOR),
        int_vector_(value) {}
  // Create a Vector<Float> object.
  Datum(const Vector<Float> &value)
      : type_(GRNXX_FLOAT_VECTOR),
        float_vector_(value) {}
  // Create a Vector<GeoPoint> object.
  Datum(const Vector<GeoPoint> &value)
      : type_(GRNXX_GEO_POINT_VECTOR),
        geo_point_vector_(value) {}
  // Create a Vector<Text> object.
  Datum(const Vector<Text> &value)
      : type_(GRNXX_TEXT_VECTOR),
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
      case GRNXX_NA: {
        na_.~NA();
        break;
      }
      case GRNXX_BOOL: {
        bool_.~Bool();
        break;
      }
      case GRNXX_INT: {
        int_.~Int();
        break;
      }
      case GRNXX_FLOAT: {
        float_.~Float();
        break;
      }
      case GRNXX_GEO_POINT: {
        geo_point_.~GeoPoint();
        break;
      }
      case GRNXX_TEXT: {
        text_.~Text();
        break;
      }
      case GRNXX_BOOL_VECTOR: {
        bool_vector_.~BoolVector();
        break;
      }
      case GRNXX_INT_VECTOR: {
        int_vector_.~IntVector();
        break;
      }
      case GRNXX_FLOAT_VECTOR: {
        float_vector_.~FloatVector();
        break;
      }
      case GRNXX_GEO_POINT_VECTOR: {
        geo_point_vector_.~GeoPointVector();
        break;
      }
      case GRNXX_TEXT_VECTOR: {
        text_vector_.~TextVector();
        break;
      }
    }
  }
  void copy_from(const Datum &datum) {
    type_ = datum.type_;
    switch (type_) {
      case GRNXX_NA: {
        new (&na_) NA(datum.na_);
        break;
      }
      case GRNXX_BOOL: {
        new (&bool_) Bool(datum.bool_);
        break;
      }
      case GRNXX_INT: {
        new (&int_) Int(datum.int_);
        break;
      }
      case GRNXX_FLOAT: {
        new (&float_) Float(datum.float_);
        break;
      }
      case GRNXX_GEO_POINT: {
        new (&geo_point_) GeoPoint(datum.geo_point_);
        break;
      }
      case GRNXX_TEXT: {
        new (&text_) Text(datum.text_);
        break;
      }
      case GRNXX_BOOL_VECTOR: {
        new (&bool_vector_) BoolVector(datum.bool_vector_);
        break;
      }
      case GRNXX_INT_VECTOR: {
        new (&int_vector_) IntVector(datum.int_vector_);
        break;
      }
      case GRNXX_FLOAT_VECTOR: {
        new (&float_vector_) FloatVector(datum.float_vector_);
        break;
      }
      case GRNXX_GEO_POINT_VECTOR: {
        new (&geo_point_vector_) GeoPointVector(datum.geo_point_vector_);
        break;
      }
      case GRNXX_TEXT_VECTOR: {
        new (&text_vector_) TextVector(datum.text_vector_);
        break;
      }
    }
  }
  void move_from(Datum &&datum) {
    type_ = datum.type_;
    switch (type_) {
      case GRNXX_NA: {
        new (&na_) NA(std::move(datum.na_));
        break;
      }
      case GRNXX_BOOL: {
        new (&bool_) Bool(std::move(datum.bool_));
        break;
      }
      case GRNXX_INT: {
        new (&int_) Int(std::move(datum.int_));
        break;
      }
      case GRNXX_FLOAT: {
        new (&float_) Float(std::move(datum.float_));
        break;
      }
      case GRNXX_GEO_POINT: {
        new (&geo_point_) GeoPoint(std::move(datum.geo_point_));
        break;
      }
      case GRNXX_TEXT: {
        new (&text_) Text(std::move(datum.text_));
        break;
      }
      case GRNXX_BOOL_VECTOR: {
        new (&bool_vector_) BoolVector(std::move(datum.bool_vector_));
        break;
      }
      case GRNXX_INT_VECTOR: {
        new (&int_vector_) IntVector(std::move(datum.int_vector_));
        break;
      }
      case GRNXX_FLOAT_VECTOR: {
        new (&float_vector_) FloatVector(std::move(datum.float_vector_));
        break;
      }
      case GRNXX_GEO_POINT_VECTOR: {
        new (&geo_point_vector_)
            GeoPointVector(std::move(datum.geo_point_vector_));
        break;
      }
      case GRNXX_TEXT_VECTOR: {
        new (&text_vector_) TextVector(std::move(datum.text_vector_));
        break;
      }
    }
  }
};

}  // namespace grnxx

#endif  // GRNXX_TYPES_DATUM_HPP
