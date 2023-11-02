#pragma once

#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "common/rc.h"
#include <vector>

class GroupByLogicalOperator : public LogicalOperator
{
public:
  GroupByLogicalOperator(std::vector<Expression *> groupby, Expression *having) 
		: groupby_(groupby), having_(having)
	{}

  virtual ~GroupByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::GROUPBY; }

	std::vector<Expression *> groupby_;
	Expression *having_ = nullptr;
};
