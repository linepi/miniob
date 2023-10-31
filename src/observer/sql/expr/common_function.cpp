#include "sql/expr/common_function.h"
#include "sql/parser/value.h"
#include <cmath>
#include <cassert>

CommonFunction::CommonFunction(FunctionType func_type) : func_type_(func_type) 
{}

RC CommonFunction::iterate(Value &value) {
  switch (func_type_) {
    case FUNC_LENGTH:
      return func_length(value);
    case FUNC_ROUND:
      return func_round(value);
    case FUNC_DATE_FORMAT:
      return func_date_format(value);
    case FUNC_UNDEFINED:
      return RC::SUCCESS;
    default:
      assert(0);
  }
  return RC::SUCCESS;
}

RC CommonFunction::func_length(Value& value) const {
  if (value.attr_type() == CHARS) {
    value.set_int(value.get_string().length());
    return RC::SUCCESS; 
  }
  return RC::INVALID_ARGUMENT; 
}

RC CommonFunction::func_round(Value& value) const {
  if (value.attr_type() == FLOATS) {
    value.set_int(static_cast<int>(std::round(value.get_float())));
    return RC::SUCCESS;
  }
  return RC::INVALID_ARGUMENT;
}

RC CommonFunction::func_date_format(Value& value) const {
  return RC::SUCCESS;
}