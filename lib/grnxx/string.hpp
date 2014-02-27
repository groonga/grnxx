#ifndef GRNXX_STRING_HPP
#define GRNXX_STRING_HPP

#include "grnxx/types.hpp"

namespace grnxx {

// 文字列．
class String {
 public:
  // 明示的な初期化はしない．
  String() = default;
  // 空文字列として初期化する．
  String(std::nullptr_t) : data_(nullptr), size_(0) {}
  // NULL 文字を終端とする文字列を参照するように初期化する．
  String(const char *str)
      : data_(reinterpret_cast<const UInt8 *>(str)),
        size_(std::strlen(str)) {}
  // 文字列を参照するように初期化する．
  String(const std::string &str)
      : data_(reinterpret_cast<const UInt8 *>(str.data())),
        size_(str.size()) {}
  // 指定されたバイト列を参照するように初期化する．
  String(const void *data, Int64 size)
      : data_(static_cast<const UInt8 *>(data)),
        size_(size) {}

  // 先頭の n bytes に対するオブジェクトを作成する．
  String prefix(Int64 n) const {
    return String(data_, n);
  }
  // 末尾の n bytes に対するオブジェクトを作成する．
  String suffix(Int64 n) const {
    return String(data_ + size_ - n, n);
  }

  // 先頭の n bytes を取り除いた部分列に対するオブジェクトを作成する．
  String except_prefix(Int64 n) const {
    return String(data_ + n, size_ - n);
  }
  // 末尾の n bytes を取り除いた部分列に対するオブジェクトを作成する．
  String except_suffix(Int64 n) const {
    return String(data_, size_ - n);
  }

  // 先頭の n bytes を飛ばした後，残りの先頭 m bytes に対するオブジェクトを作成する．
  String extract(Int64 n, Int64 m) const {
    return String(data_ + n, m);
  }
  // 先頭の n bytes と末尾の n bytes を取り除いた部分列に対するオブジェクトを作成する．
  String trim(Int64 n, Int64 m) const {
    return String(data_ + n, size_ - n - m);
  }

  // 文字列同士を比較する．
  bool operator==(const String &rhs) const {
    return (size_ == rhs.size_) &&
           (std::memcmp(data_, rhs.data_, size_) == 0);
  }
  bool operator!=(const String &rhs) const {
    return !(*this == rhs);
  }
  bool operator<(const String &rhs) const {
    const Int64 min_size = (size_ < rhs.size_) ? size_ : rhs.size_;
    int result = std::memcmp(data_, rhs.data_, min_size);
    return (result < 0) || ((result == 0) && (size_ < rhs.size_));
  }
  bool operator>(const String &rhs) const {
    return rhs < *this;
  }
  bool operator<=(const String &rhs) const {
    return !(*this > rhs);
  }
  bool operator>=(const String &rhs) const {
    return !(*this < rhs);
  }

  // 指定された文字列と比較して，小さければ負の値を返し，等しければ 0 を返し，
  // 大きければ正の値を返す．
  int compare(const String &bytes) const {
    const Int64 min_size = (size_ < bytes.size_) ? size_ : bytes.size_;
    int result = std::memcmp(data_, bytes.data_, min_size);
    if (result != 0) {
      return result;
    }
    return (size_ < bytes.size_) ? -1 : (size_ > bytes.size_);
  }

  // 指定された文字列で始まっていれば true を返し，そうでなければ false を返す．
  bool starts_with(const String &bytes) const {
    return (size_ >= bytes.size_) && (prefix(bytes.size_) == bytes);
  }
  // 指定された文字列で終わっていれば true を返し，そうでなければ false を返す．
  bool ends_with(const String &bytes) const {
    return (size_ >= bytes.size_) && (suffix(bytes.size_) == bytes);
  }

  // 指定された文字が最初に出現する位置を返す．
  // 先頭の offset bytes は無視する．
  // 出現しないときは npos を返す．
  Int64 find_first_of(UInt8 byte, Int64 offset = 0) const {
    for (Int64 i = offset; i < size_; ++i) {
      if (data_[i] == byte) {
        return i;
      }
    }
    return npos;
  }
  // 指定された文字列に含まれる文字が最初に出現する位置を返す．
  // 先頭の offset bytes は無視する．
  // 出現しないときは npos を返す．
  Int64 find_first_of(const String &str, Int64 offset = 0) const {
    for (Int64 i = offset; i < size_; ++i) {
      if (str.find_first_of(data_[i]) != npos) {
        return i;
      }
    }
    return npos;
  }
  // 指定された文字が最後に出現する位置を返す．
  // 先頭の offset bytes は無視する．
  // 出現しないときは npos を返す．
  Int64 find_last_of(UInt8 byte, Int64 offset = 0) const {
    for (Int64 i = size_; i > offset; ) {
      if (data_[--i] == byte) {
        return i;
      }
    }
    return npos;
  }

  // 指定された文字列に含まれない文字が最初に出現する位置を返す．
  // 出現しないときは npos を返す．
  Int64 find_first_not_of(const String &str, Int64 offset = 0) const {
    for (Int64 i = offset; i < size_; ++i) {
      if (str.find_first_of(data_[i]) == npos) {
        return i;
      }
    }
    return npos;
  }
  // 指定された文字列に含まれない文字が最後に出現する位置を返す．
  // 出現しないときは npos を返す．
  Int64 find_last_not_of(const String &str, Int64 offset = 0) const {
    for (Int64 i = size_; i > offset; ) {
      if (str.find_first_of(data_[--i]) == npos) {
        return i;
      }
    }
    return npos;
  }

  // 文字（バイト）を返す．
  UInt8 operator[](Int64 i) const {
    return data_[i];
  }
  // 開始アドレスを返す．
  const UInt8 *data() const {
    return data_;
  }
  // 長さをバイト単位で返す．
  Int64 size() const {
    return size_;
  }
  // 長さが 0 であれば true を返し，そうでなければ false を返す．
  bool empty() const {
    return size_ == 0;
  }

  static constexpr Int64 npos = -1;

 private:
  const UInt8 *data_;
  Int64 size_;
};

std::ostream &operator<<(std::ostream &stream, const String &string);

}  // namespace grnxx

#endif  // GRNXX_STRING_HPP
