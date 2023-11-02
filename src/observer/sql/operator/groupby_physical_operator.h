// GroupByPhysicalOperator.h
#pragma once

#include "sql/operator/physical_operator.h"
#include "common/rc.h"
#include <queue>
#include <vector>
#include <fstream>

class PhysicalOperator;

class GroupByPhysicalOperator : public PhysicalOperator
{
public:
  GroupByPhysicalOperator(std::vector<Expression *> groupby, Expression *having) 
	: groupby_(groupby), having_(having)
	{}

  virtual ~GroupByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUPBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

	std::vector<Expression *> groupby_;
	Expression *having_ = nullptr;
};
