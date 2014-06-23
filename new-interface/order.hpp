#ifndef GRNXX_ORDER_HPP
#define GRNXX_ORDER_HPP

#include "grnxx/types.hpp"

namespace grnxx {

enum OrderType {
  // 基本的に昇順となる．
  REGULAR_ORDER,

  // 基本的に降順となる．
  REVERSE_ORDER

  // 浮動小数点数については，単項演算子 '-' を使うことで降順にできる．
  // 整数については，最小値がオーバーフローするので使うべきではない．
  // 文字列については，単項演算子 '-' をサポートしない．
  // 真偽値については，単項演算子 '-' の代わりに '!' が使える．
};

struct OrderOptions {
  OrderType type;

  OrderOptions();
};

struct OrderUnit {
  std::unique_ptr<Expression> &&expression;
  OrderType type;
};

class Order {
 public:
  Order();
  ~Order();
};

class OrderBuilder {
 public:
  OrderBuilder();
  virtual ~OrderBuilder();

  // 整列条件に追加する．
  // 成功すれば true を返す．
  // 失敗したときは *error にその内容を格納し， false を返す．
  //
  // 整列条件は末尾に追加していくため，優先度の高い整列条件から順に
  // 追加しなければならない．行 ID を最後の整列条件として追加することにより，
  // 整列結果を安定させることができる．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - 指定された式の評価結果の型が整列条件として使えない．
  // - 指定されたオプションが不正である．
  // - リソースを確保できない．
  virtual bool push(Error *error,
                    std::unique_ptr<Expression> &&expression,
                    const OrderOptions &options) = 0;

  // 保持している整列条件を破棄する．
  virtual void clear() = 0;

  // 構築中の順序を完成させ，その所有権を取得する．
  // 成功すれば有効なオブジェクトへのポインタを返す．
  // 失敗したときは *error にその内容を格納し， nullptr を返す．
  //
  // 所有権を返すため，保持している整列条件は破棄する．
  //
  // 失敗する状況としては，以下のようなものが挙げられる．
  // - リソースを確保できない．
  virtual std::unique_ptr<Order> release(Error *error) = 0;
};

}  // namespace grnxx

#endif  // GRNXX_ORDER_HPP
