#ifndef GRNXX_DATUM_HPP
#define GRNXX_DATUM_HPP

#include "grnxx/types.hpp"

namespace grnxx {

class Datum {
 public:
  // データを初期化する．
  Datum()
      : type_(NULL_DATUM),
        null_() {}
  Datum(bool value)
      : type_(BOOL_DATUM),
        bool_(value) {}
  Datum(int64_t value)
      : type_(INT_DATUM),
        int_(value) {}
  Datum(double value)
      : type_(FLOAT_DATUM),
        float_(value) {}
  Datum(const std::string &value)
      : type_(TEXT_DATUM),
        text_(value) {}
  Datum(std::string &&value)
      : type_(TEXT_DATUM),
        text_(value) {}
  Datum(const std::vector<bool> &value)
      : type_(BOOL_ARRAY_DATUM),
        bool_array_(value) {}
  Datum(std::vector<bool> &&value)
      : type_(BOOL_ARRAY_DATUM),
        bool_array_(value) {}
  Datum(const std::vector<int64_t> &value)
      : type_(INT_ARRAY_DATUM),
        int_array_(value) {}
  Datum(std::vector<int64_t> &&value)
      : type_(INT_ARRAY_DATUM),
        int_array_(value) {}
  Datum(const std::vector<double> &value)
      : type_(FLOAT_ARRAY_DATUM),
        float_array_(value) {}
  Datum(std::vector<double> &&value)
      : type_(FLOAT_ARRAY_DATUM),
        float_array_(value) {}
  Datum(const std::vector<std::string> &value)
      : type_(TEXT_ARRAY_DATUM),
        text_array_(value) {}
  Datum(std::vector<std::string> &&value)
      : type_(TEXT_ARRAY_DATUM),
        text_array_(value) {}
  // データを破棄する．
  ~Datum() {
    clear();
  }

  // ムーブ．
  Datum(Datum &&datum);
  // ムーブ．
  Datum &operator=(Datum &&datum);

  // 種類を返す．
  DataType type() const {
    return type_;
  }

  // 真偽値を代入する．
  Datum &operator=(bool value);
  // 整数を代入する．
  Datum &operator=(int64_t value);
  // 浮動小数点数を代入する．
  Datum &operator=(double value);
  // 文字列を代入する．
  Datum &operator=(const std::string &value);
  // 真偽値の配列を代入する．
  Datum &operator=(const std::vector<bool> &value);
  // 整数の配列を代入する．
  Datum &operator=(const std::vector<int64_t> &value);
  // 浮動小数点数の配列を代入する．
  Datum &operator=(const std::vector<double> &value);
  // 文字列の配列を代入する．
  Datum &operator=(const std::vector<std::string> &value);

  Datum &operator=(std::string &&value);
  // 真偽値の配列を代入する．
  Datum &operator=(std::vector<bool> &&value);
  // 整数の配列を代入する．
  Datum &operator=(std::vector<int64_t> &&value);
  // 浮動小数点数の配列を代入する．
  Datum &operator=(std::vector<double> &&value);
  // 文字列の配列を代入する．
  Datum &operator=(std::vector<std::string> &&value);

  // 真偽値として参照する．
  const bool &as_bool() const {
    return bool_;
  }
  // 整数として参照する．
  const int64_t &as_int() const {
    return int_;
  }
  // 浮動小数点数として参照する．
  const double &as_float() const {
    return float_;
  }
  // 文字列として参照する．
  const std::string &as_text() const {
    return text_;
  }
  // 真偽値の配列として参照する．
  const std::vector<bool> &as_bool_array() const {
    return bool_array_;
  }
  // 整数の配列として参照する．
  const std::vector<int64_t> &as_int_array() const {
    return int_array_;
  }
  // 浮動小数点数の配列として参照する．
  const std::vector<double> &as_float_array() const {
    return float_array_;
  }
  // 文字列の配列として参照する．
  const std::vector<std::string> &as_text_array() const {
    return text_array_;
  }

  // データを破棄する．
  void clear();

 private:
  DataType type_;
  union {
    std::nullptr_t null_;
    bool bool_;
    int64_t int_;
    double float_;
    std::string text_;
    std::vector<bool> bool_array_;
    std::vector<int64_t> int_array_;
    std::vector<double> float_array_;
    std::vector<std::string> text_array_;
  };
};

}  // namespace grnxx

#endif  // GRNXX_DATUM_HPP
