#include "grnxx/expression.hpp"

#include <new>

#include "grnxx/impl/expression.hpp"

namespace grnxx {

std::unique_ptr<Expression> Expression::parse(const String &query) {
  return impl::ExpressionParser::parse(query);
}

std::unique_ptr<ExpressionBuilder> ExpressionBuilder::create(
    const Table *table) try {
  return std::unique_ptr<ExpressionBuilder>(
      new impl::ExpressionBuilder(static_cast<const impl::Table *>(table)));
} catch (const std::bad_alloc &) {
  throw "Memory allocation failed";  // TODO
}

}  // namespace grnxx
