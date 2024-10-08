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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <vector>
#include <memory>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt 
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override
  {
    return StmtType::SELECT;
  }

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt);

public:
  const std::vector<Table *> &tables() const
  {
    return tables_;
  }
  const std::vector<Expression *> &query_exprs() const
  {
    return query_exprs_;
  }
  FilterStmt *filter_stmt() const
  {
    return filter_stmt_;
  }

  bool has_order_by(){
    return order_by_;
  }
  bool has_group_by(){
    return groupby_.size() != 0;
  }

  const std::vector<Field> &order_fields() const
  {
    return order_fields_;
  }

  const std::vector<bool> &order_infos() const
  {
    return order_info;
  }

  const std::vector<Expression *> &groupby() const {
    return groupby_;
  }

  Expression *having() const {
    return having_;
  }

private:
  std::vector<Expression *> query_exprs_;
  std::vector<Table *> tables_;
  FilterStmt *filter_stmt_ = nullptr;

  bool order_by_;
  std::vector<Field> order_fields_;
  std::vector<bool> order_info;

  std::vector<Expression *> groupby_;
  Expression *having_ = nullptr;
};
