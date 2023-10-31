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
#include "sql/expr/expression.h"

class Db;
class Table;

static std::string giveout(const std::string &str) {
  std::string result;
  if (str[0] == '\'' || str[0] == '\"') {
    result = str.substr(1, str.size() - 2);
  } else {
    result = str;
  }
  return result;
}

/**
 * @brief 描述算术运算语句
 * @ingroup Statement
 */
class CalcStmt : public Stmt 
{
public:
  CalcStmt() = default;
  virtual ~CalcStmt() override = default;

  StmtType type() const override
  {
    return StmtType::CALC;
  }

public:
  static RC create(CalcSqlNode &calc_sql, Stmt *&stmt)
  {
    CalcStmt *calc_stmt = new CalcStmt();
    for (Expression * const expr : calc_sql.expressions) {
      calc_stmt->expressions_.emplace_back(expr);
      if (!expr->alias().empty()) {
        std::string name = giveout(name);
        calc_stmt->names_.push_back(expr->alias());
      } else {
        std::string name = giveout(expr->name());
        calc_stmt->names_.push_back(name);
      }
    }
    calc_sql.expressions.clear();
    stmt = calc_stmt;
    return RC::SUCCESS;
  }

public:
  std::vector<std::unique_ptr<Expression>> &expressions()
  {
    return expressions_;
  }

  std::vector<std::string> &names() {
    return names_;
  }

private:
  std::vector<std::unique_ptr<Expression>> expressions_;
  std::vector<std::string> names_;
};
