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
// Created by Wangyunlai on 2022/5/22.
//

#include "common/rc.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt()
{
}

RC get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field);

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    Expression *condition, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  if (!condition) return rc;

  auto visitor = [&db, &default_table, &tables, &condition](std::unique_ptr<Expression> &expr){
    RC rc = RC::SUCCESS;
    assert(expr->type() == ExprType::FIELD);
    FieldExpr *field_expr = static_cast<FieldExpr *>(expr.get());

    Table *table = nullptr;
    const FieldMeta *field = nullptr;

    rc = get_table_and_field(db, default_table, tables, field_expr->rel_attr(), table, field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
    field_expr->set_field(Field(table, field));
    return rc;
  };
  rc = condition->visit_field_expr(visitor, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("visit field expr error %s", strrc(rc));
    return rc;
  }

  auto arith_visitor = [](Expression *expr) {
    ComparisonExpr *comp = static_cast<ComparisonExpr *>(expr);
    AttrType left, right;
    left = comp->left()->value_type();
    right = comp->right()->value_type();
    if (left == UNDEFINED || right == UNDEFINED) return RC::SUCCESS;
    Value lv(left);
    Value rv(right);
    bool t;
    return lv.compare_op(rv, comp->comp() ,t);
  };

  if (condition) {
    rc = condition->visit_arith_expr(arith_visitor);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  stmt = new FilterStmt();
  stmt->condition_ = condition;
  return rc;
}

RC get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}