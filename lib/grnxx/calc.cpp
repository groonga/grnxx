#include "grnxx/calc.hpp"

#include "grnxx/calc_impl.hpp"

namespace grnxx {

// 演算器を作成する．
Calc *CalcHelper::create(const Table *table, const String &query) {
  std::unique_ptr<CalcImpl> calc(new CalcImpl);
  if (!calc->parse(table, query)) {
    return nullptr;
  }
  return calc.release();
}

}  // namespace grnxx
