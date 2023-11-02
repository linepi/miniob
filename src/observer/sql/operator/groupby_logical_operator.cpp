/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/groupby_physical_operator.h"
#include "storage/record/record.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/field/field.h"

RC GroupByPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("GroupBy operator must has one child");
    return RC::INTERNAL;
  }

  return children_[0]->open(trx);
}

RC GroupByPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  return rc;
}

RC GroupByPhysicalOperator::close()
{
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *GroupByPhysicalOperator::current_tuple()
{
  return children_[0]->current_tuple();
}
