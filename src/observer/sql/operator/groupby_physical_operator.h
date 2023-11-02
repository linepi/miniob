// GroupByPhysicalOperator.h
#pragma once

#include "sql/operator/physical_operator.h"
#include "common/rc.h"
#include <queue>
#include <vector>
#include <fstream>

class PhysicalOperator;
class Expression;

class GroupByPhysicalOperator : public PhysicalOperator
{
public:
  GroupByPhysicalOperator(std::vector<Expression *> groupby, Expression *having);

  virtual ~GroupByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUPBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

	std::vector<std::vector<RowTuple>> groups;
	std::vector<Field> fields_;
	std::vector<Expression *> groupby_;
	Expression *having_ = nullptr;
};
