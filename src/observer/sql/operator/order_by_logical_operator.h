#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "common/rc.h"
#include <vector>

class OrderByLogicalOperator : public LogicalOperator {
public:
  OrderByLogicalOperator(const std::vector<Field>& orderByColumns, std::vector<bool> sort_info, bool if_mult_table)
  : orderByColumns_(orderByColumns), sort_info_(sort_info), if_mult_table(if_mult_table) {};

  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override
  {
    return LogicalOperatorType::ORDER_BY;
  }

  const std::vector<Field>& get_orderByColumns()
  {
    return orderByColumns_;
  }

  std::vector<bool> get_sort_info()
  {
    return sort_info_;
  }

  bool get_if_mult_table(){
    return if_mult_table;
  }

private:
  std::vector<Field> orderByColumns_;
  std::vector<bool> sort_info_;
  // 其他需要的成员变量
  bool if_mult_table;
};
