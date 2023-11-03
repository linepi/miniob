// OrderByPhysicalOperator.h
#pragma once

#include "sql/operator/physical_operator.h"
#include "common/rc.h"
#include <queue>
#include <vector>
#include <fstream>

class PhysicalOperator;

class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(const std::vector<Field> &orderByColumns, std::vector<bool> sort_info, bool is_mult_table)
      : orderByColumns(orderByColumns), sort_info(sort_info), is_mult_table(is_mult_table){};

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  size_t                   current_position_ = 0;
  std::vector<RowTuple>    buffer_;
  std::vector<JoinedTuple> buffer_join_;
  RowTuple                *current_tuple_;
  JoinedTuple             *current_tuple_join_;
  std::vector<Field>       orderByColumns;
  std::vector<bool>        sort_info;
  bool                     is_mult_table;
  // 可能还需要其他成员变量，例如orderByColumns等
  bool inited_ = false;
};
