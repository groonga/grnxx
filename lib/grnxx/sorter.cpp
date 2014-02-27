#include "grnxx/sorter.hpp"

#include "grnxx/sorter_impl.hpp"

namespace grnxx {

// 演算器を作成する．
Sorter *SorterHelper::create(const Table *table, const String &query) {
  std::unique_ptr<SorterImpl> sorter(new SorterImpl);
  if (!sorter->parse(table, query)) {
    return nullptr;
  }
  return sorter.release();
}

}  // namespace grnxx
