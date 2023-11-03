// GroupByPhysicalOperator.h
#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/expr/tuple.h"
#include "common/rc.h"
#include <queue>
#include <vector>
#include <fstream>

class PhysicalOperator;
class Expression;

class GroupByPhysicalOperator : public PhysicalOperator
{
public:
  GroupByPhysicalOperator(std::vector<Expression *> groupby, Expression *having, std::vector<Expression *> select_exprs);

  virtual ~GroupByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUPBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  bool inited_ = false;

  size_t                  res_idx_ = 0;
  std::vector<GroupTuple> results_;

	std::vector<std::vector<Tuple *>> groups_;
	std::vector<Field> fields_;

	std::vector<Expression *> groupby_;
	Expression *having_ = nullptr;
	std::vector<Expression *> select_exprs_;
};
