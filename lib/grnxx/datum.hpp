#ifndef GRNXX_DATUM_HPP
#define GRNXX_DATUM_HPP

#include "grnxx/string.hpp"
#include "grnxx/types.hpp"

namespace grnxx {

// 汎用データ型．
class Datum {
 public:
  // FALSE として初期化する．
  Datum() : type_(BOOLEAN), boolean_(false) {}
  // 真偽値として初期化する．
  Datum(Boolean value) : type_(BOOLEAN), boolean_(value) {}
  // 整数として初期化する．
  Datum(Int64 value) : type_(INTEGER), integer_(value) {}
  // 浮動小数点数として初期化する．
  Datum(Float value) : type_(FLOAT), float_(value) {}
  // バイト列・文字列として初期化する．
  Datum(const char *value) : type_(STRING), string_(value) {}
  Datum(const String &value)
      : type_(STRING),
        string_(reinterpret_cast<const char *>(value.data()), value.size()) {}
  Datum(const std::string &value) : type_(STRING), string_(value) {}
  // 破棄する．
  ~Datum() {
    if (type_ == STRING) {
      string_.~basic_string();
    }
  }

  // string_ はコピーするとコストが大きくなるので注意．
  Datum(const Datum &datum) : type_(BOOLEAN), boolean_(false) {
    switch (datum.type_) {
      case BOOLEAN: {
        boolean_ = datum.boolean_;
        type_ = datum.type_;
        break;
      }
      case INTEGER: {
        integer_ = datum.integer_;
        type_ = datum.type_;
        break;
      }
      case FLOAT: {
        float_ = datum.float_;
        type_ = datum.type_;
        break;
      }
      case STRING: {
        new (&string_) std::string(datum.string_);
        type_ = datum.type_;
        break;
      }
      default: {
        break;
      }
    }
  }
  // string_ はムーブの方がコストを抑えられるはず．
  Datum(Datum &&datum) : type_(BOOLEAN), boolean_(false) {
    switch (datum.type_) {
      case BOOLEAN: {
        boolean_ = std::move(datum.boolean_);
        type_ = std::move(datum.type_);
        break;
      }
      case INTEGER: {
        integer_ = std::move(datum.integer_);
        type_ = std::move(datum.type_);
        break;
      }
      case FLOAT: {
        float_ = std::move(datum.float_);
        type_ = std::move(datum.type_);
        break;
      }
      case STRING: {
        new (&string_) std::string(std::move(datum.string_));
        type_ = std::move(datum.type_);
        break;
      }
      default: {
        break;
      }
    }
  }

  // データ型を返す．
  DataType type() const {
    return type_;
  }

  // データを代入する．
  Datum &operator=(Boolean rhs) {
    if (type_ == STRING) {
      string_.~basic_string();
    }
    type_ = BOOLEAN;
    boolean_ = rhs;
    return *this;
  }
  Datum &operator=(Int64 rhs) {
    if (type_ == STRING) {
      string_.~basic_string();
    }
    type_ = INTEGER;
    integer_ = static_cast<Int64>(rhs);
    return *this;
  }
  Datum &operator=(Float rhs) {
    if (type_ == STRING) {
      string_.~basic_string();
    }
    type_ = FLOAT;
    float_ = static_cast<Float>(rhs);
    return *this;
  }
  Datum &operator=(const char *rhs) {
    if (type_ != STRING) {
      new (&string_) std::string(rhs);
      type_ = STRING;
    } else {
      string_ = rhs;
    }
    return *this;
  }
  Datum &operator=(const String &rhs) {
    if (type_ != STRING) {
      new (&string_) std::string(reinterpret_cast<const char *>(rhs.data()),
                                 rhs.size());
      type_ = STRING;
    } else {
      string_.assign(reinterpret_cast<const char *>(rhs.data()), rhs.size());
    }
    return *this;
  }
  Datum &operator=(const std::string &rhs) {
    if (type_ != STRING) {
      new (&string_) std::string(rhs);
      type_ = STRING;
    } else {
      string_ = rhs;
    }
    return *this;
  }

  // 型変換．
  explicit operator Boolean() const {
    switch (type_) {
      case BOOLEAN: {
        return boolean_;
      }
      case INTEGER: {
        return static_cast<Boolean>(integer_);
      }
      case FLOAT: {
        return static_cast<Boolean>(float_);
      }
      case STRING: {
        return string_ == "TRUE";
      }
      default: {
        return false;
      }
    }
  }
  explicit operator Int64() const {
    switch (type_) {
      case BOOLEAN: {
        return static_cast<Int64>(boolean_);
      }
      case INTEGER: {
        return integer_;
      }
      case FLOAT: {
        return static_cast<Int64>(float_);
      }
      case STRING: {
        return static_cast<Int64>(std::stol(string_));
      }
      default: {
        return 0;
      }
    }
  }
  explicit operator Float() const {
    switch (type_) {
      case BOOLEAN: {
        return static_cast<Float>(boolean_);
      }
      case INTEGER: {
        return static_cast<Float>(integer_);
      }
      case FLOAT: {
        return float_;
      }
      case STRING: {
        return static_cast<Float>(std::stod(string_));
      }
      default: {
        return 0.0;
      }
    }
  }
  explicit operator std::string() const {
    switch (type_) {
      case BOOLEAN: {
        return boolean_ ? "TRUE" : "FALSE";
      }
      case INTEGER: {
        return std::to_string(integer_);
      }
      case FLOAT: {
        return std::to_string(float_);
      }
      case STRING: {
        return string_;
      }
      default: {
        return "";
      }
    }
  }

  // ストリームに書き出す．
  std::ostream &write_to(std::ostream &stream) const;

 private:
  DataType type_;
  union {
    Boolean boolean_;
    Int64 integer_;
    Float float_;
    std::string string_;
  };
};

std::ostream &operator<<(std::ostream &stream, const Datum &datum);

}  // namespace grnxx

#endif  // GRNXX_DATUM_HPP
