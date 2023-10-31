#pragma once

#include "sql/expr/exprfunc.h"
#include "common/enum.h"
#include <string>

class CommonFunction : public ExprFunc {
public:				
  CommonFunction(FunctionType func_type);
  CommonFunction(FunctionType func_type, std::string param);
  ~CommonFunction() = default;

  RC iterate(Value &value) override;
  RC func_length(Value &value) const;
  RC func_round(Value &value) const;
  RC func_date_format(Value &value) const;
  int type() override { return ExprFunc::COMMON; }
  FunctionType func_type_;
  std::string param_;
};