/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include "sql/operator/logical_operator.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/groupby_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/order_by_logical_operator.h"
#include "sql/operator/order_by_physical_operator.h"

#include "sql/stmt/stmt.h"
#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "storage/table/table.h"

using namespace std;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);
      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
      rc = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);
      rc = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);
      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);
      rc = create_plan(explain_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
      rc = create_plan(update_stmt, logical_operator);
    } break;

    default: {
      rc = RC::UNIMPLENMENT;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> table_oper(nullptr);

  const std::vector<Table *> &tables = select_stmt->tables();
  const std::vector<Expression *> &all_exprs = select_stmt->query_exprs();

  for (Table *table : tables) {
    std::vector<Field> fields;
    if (all_exprs[0]->type() == ExprType::STAR) {
      std::vector<Field> &all_fields = static_cast<StarExpr *>(all_exprs[0])->field();
      for (const Field &field : all_fields) {
        if (0 == strcmp(field.table_name(), table->name())) {
          fields.push_back(field);
        }
      }
    } else {
      for (Expression *expr : all_exprs) {
        std::vector<FieldExpr *> field_exprs;
        expr->get_field_expr(field_exprs, false);
        for (FieldExpr *field_expr : field_exprs) {
          if (0 == strcmp(field_expr->table_name(), table->name())) {
            fields.push_back(field_expr->field());
          }
        }
      }
    }

    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, true/*readonly*/));
    if (table_oper == nullptr) {
      table_oper = std::move(table_get_oper);
    } else {
      if (tables[0] != tables[1]) {
        JoinLogicalOperator *join_oper = new JoinLogicalOperator;
        join_oper->add_child(std::move(table_oper));
        join_oper->add_child(std::move(table_get_oper));
        table_oper = unique_ptr<LogicalOperator>(join_oper);
      }
    }
  }

  unique_ptr<LogicalOperator> predicate_oper;
  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<LogicalOperator> order_by_oper;
  if (select_stmt->has_order_by()) {
    // 创建临时表操作符
    std::vector<Field> orderByColumns = select_stmt->order_fields();
    std::vector<bool> sort_info = select_stmt->order_infos();
    order_by_oper.reset(new OrderByLogicalOperator(orderByColumns,sort_info,!(tables.size()==1)));
    order_by_oper->add_child(std::move(table_oper));
  }

  
  if (select_stmt->has_group_by()) {
    unique_ptr<LogicalOperator> group_by_oper;

    std::vector<Field> orderByColumns;
    for (Expression * expr : select_stmt->groupby()) {
      orderByColumns.push_back(static_cast<FieldExpr *>(expr)->field());
    }

    std::vector<bool> sort_info(select_stmt->groupby().size(), true);

    order_by_oper.reset(new OrderByLogicalOperator(orderByColumns, sort_info, !(tables.size() == 1)));
    order_by_oper->add_child(std::move(table_oper));

    group_by_oper.reset(new GroupByLogicalOperator(select_stmt->groupby(), select_stmt->having(), all_exprs));
    if (predicate_oper) {
      predicate_oper->add_child(std::move(order_by_oper));
      group_by_oper->add_child(std::move(predicate_oper));
    } else {
      group_by_oper->add_child(std::move(order_by_oper));
    }
    logical_operator.swap(group_by_oper);
  } else {
    unique_ptr<LogicalOperator> project_oper(new ProjectLogicalOperator(all_exprs));
    if (predicate_oper) {
      if (order_by_oper) {
        predicate_oper->add_child(std::move(order_by_oper));
      } else if (table_oper) {
        predicate_oper->add_child(std::move(table_oper));
      }

      project_oper->add_child(std::move(predicate_oper));
    } else {
      if (order_by_oper) {
        project_oper->add_child(std::move(order_by_oper));
      } else if (table_oper) {
        project_oper->add_child(std::move(table_oper));
      }
    }
    logical_operator.swap(project_oper);
  }

  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (filter_stmt && filter_stmt->condition()) {
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::unique_ptr<Expression>(filter_stmt->condition())));
  }

  logical_operator = std::move(predicate_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = insert_stmt->table();

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, insert_stmt->values_list());
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(
    DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table *table = delete_stmt->table();
  FilterStmt *filter_stmt = delete_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false/*readonly*/));

  unique_ptr<LogicalOperator> predicate_oper;
  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(
    ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt *child_stmt = explain_stmt->child();
  unique_ptr<LogicalOperator> child_oper;
  RC rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}

RC LogicalPlanGenerator::create_plan(
    UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  std::vector<Field> fields;
  for (const FieldMeta *field_meta : update_stmt->field_metas_) {
    fields.push_back(Field(update_stmt->table_, field_meta));
  }

  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(update_stmt->table_, fields, false));

  unique_ptr<LogicalOperator> predicate_oper;
  rc = create_plan(update_stmt->filter_stmt_, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(update_stmt->table_, update_stmt->values_, update_stmt->field_metas_));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    update_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(update_oper);
  return rc;
}
