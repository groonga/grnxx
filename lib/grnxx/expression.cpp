#include "grnxx/expression.hpp"

#include "grnxx/error.hpp"

namespace grnxx {

unique_ptr<ExpressionBuilder> ExpressionBuilder::create(Error *error,
                                                        const Table *table) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return nullptr;
}

ExpressionBuilder::~ExpressionBuilder() {}

bool ExpressionBuilder::push_datum(Error *error, const Datum &datum) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

bool ExpressionBuilder::push_column(Error *error, String name) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

bool ExpressionBuilder::push_operator(Error *error,
                                      OperatorType operator_type) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return false;
}

void ExpressionBuilder::clear() {
  // TODO: Not supported yet.
}

unique_ptr<Expression> ExpressionBuilder::release(Error *error) {
  // TODO: Not supported yet.
  GRNXX_ERROR_SET(error, NOT_SUPPORTED_YET, "Not supported yet");
  return nullptr;
}

ExpressionBuilder::ExpressionBuilder(Table *table) : table_(table) {}

}  // namespace grnxx
