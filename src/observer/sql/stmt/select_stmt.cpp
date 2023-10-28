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
// Created by Wangyunlai on 2022/6/6.
//
#include <map>
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/log/log.h"
#include "common/enum.h"
#include "common/lang/string.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
}

RC add_table(
  Db *db,
  std::vector<Table *> &tables, 
  std::unordered_map<std::string, Table *> &table_map, 
  const char *table_name,
  bool norepeat
) {
  if (nullptr == table_name) {
    LOG_WARN("invalid argument. relation name is null");
    return RC::INVALID_ARGUMENT;
  }

  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  if (!norepeat || (norepeat && table_map.find(table_name) == table_map.end())) {
    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  RC rc;
  // collect tables in `from` statement
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if ((rc = add_table(db, tables, table_map, table_name, false)) != RC::SUCCESS) 
      return rc;
  }

  // collect tables and conditions in 'join' statement
  std::vector<Expression *> conditions;
  conditions.push_back(select_sql.condition);
  for (const JoinNode &jnode : select_sql.joins) {
    const char *table_name = jnode.relation_name.c_str();
    if ((rc = add_table(db, tables, table_map, table_name, false)) != RC::SUCCESS) 
      return rc;
    conditions.push_back(jnode.condition);
  }

  Expression *utimate_condition = nullptr;
  if (conditions.size() == 0) {
    utimate_condition = nullptr;
  } else if (conditions.size() == 1) {
    utimate_condition = conditions[0];
  } else {
    ConjunctionExpr *conj = new ConjunctionExpr();
    conj->init(conditions);
    utimate_condition = conj;
  }

  if (select_sql.attributes.size() == 0) {
    LOG_WARN("select attribute size is zero");
    return RC::INVALID_ARGUMENT;
  }

  int has_agg = 0, has_common = 0;
  for (const SelectAttr &select_attr : select_sql.attributes) {
    if (select_attr.agg_type != AGG_UNDEFINED) has_agg = 1;
    if (select_attr.agg_type == AGG_UNDEFINED) has_common = 1;
    if (has_agg && has_common) {
      LOG_WARN("invalid argument.");
      return RC::INVALID_ARGUMENT;
    }
    if (select_attr.expr_nodes.size() != 1) {
      LOG_WARN("invalid argument.");
      return RC::INVALID_ARGUMENT;
    }
  }
  // select id, name
  // select min(id), max(*)
  std::vector<AggregationFunc *> aggregation_funcs;
  // collect query fields in `select` statement
  std::vector<Expression *> query_exprs;

  for (const SelectAttr &select_attr : select_sql.attributes) {
    Expression *select_expr = select_attr.expr_nodes.front();
    query_exprs.push_back(select_expr);
    
    if (select_expr->type() == ExprType::STAR) {
      StarExpr *star_expr = static_cast<StarExpr *>(select_expr);

      if (select_attr.agg_type != AGG_UNDEFINED) {
        if (select_attr.agg_type == AGG_MIN || 
          select_attr.agg_type == AGG_MAX || 
          select_attr.agg_type == AGG_SUM || 
          select_attr.agg_type == AGG_AVG) {
          LOG_WARN("no %s(*) in syntax", AGG_TYPE_NAME[select_attr.agg_type]);
          return RC::INVALID_ARGUMENT;
        }
        aggregation_funcs.push_back(new AggregationFunc(select_attr.agg_type, true, new Field(), tables.size() > 1));
      }

      for (Table *table : tables) {
        const TableMeta &table_meta = table->table_meta();
        const int field_num = table_meta.field_num();
        for (int i = table_meta.sys_field_num(); i < field_num; i++) {
          star_expr->add_field(Field(table, table_meta.field(i)));
        }
      }
      break;
    }

    auto visitor = [&select_attr, &aggregation_funcs, &tables, &table_map, &db](std::unique_ptr<Expression> &expr) {
      assert(expr->type() == ExprType::FIELD);
      FieldExpr *field_expr = static_cast<FieldExpr *>(expr.get());
      RelAttrSqlNode relation_attr = field_expr->rel_attr();
      const char *table_name = relation_attr.relation_name.c_str();
      const char *field_name = relation_attr.attribute_name.c_str();

      Table *table = nullptr; 
      if (!common::is_blank(relation_attr.relation_name.c_str())) {
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        table = iter->second;
      } else {
        if (tables.size() != 1) {
          LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }
        table = tables[0];
      }
      const FieldMeta *field_meta = table->table_meta().field(field_name);
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
        return RC::SCHEMA_FIELD_MISSING;
      }
      if (select_attr.agg_type != AGG_UNDEFINED) {
        if ((field_meta->type() == DATES) &&
            (select_attr.agg_type == AGG_AVG || select_attr.agg_type == AGG_SUM)) 
        {
          LOG_WARN("avg and sum can not be used on chars and dates");
          return RC::INVALID_ARGUMENT;
        }
        aggregation_funcs.push_back(new AggregationFunc(select_attr.agg_type, false, new Field(table, field_meta), tables.size() > 1));
      }
      field_expr->set_field(Field(table, field_meta));
      return RC::SUCCESS;
    };
    rc = select_expr->visit_field_expr(visitor, true);
    if (rc != RC::SUCCESS) return rc;
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  rc = FilterStmt::create(db,
      default_table,
      &table_map,
      utimate_condition,
      filter_stmt);

  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  std::vector<Field> order_fields_;
  std::vector<bool> order_info_;

  for(SortNode s_node: select_sql.sort)
  {
    order_info_.push_back(s_node.order == ASCEND);
    
    if(s_node.field.relation_name.empty()){
      int f=0;
      for (size_t i = 0; i < select_sql.relations.size(); i++){
        const char *table_name = select_sql.relations[i].c_str();
        Table *table1 = db->find_table(table_name);
        const FieldMeta *F_m = table1->table_meta().field(s_node.field.attribute_name.c_str());
        if(F_m){
          std::string str(table_name);
          s_node.field.relation_name = str;
          f =1;
        }
      }
      if(!f){
        return RC::NOTFOUND;
      }
    }

    Table *table = db->find_table(s_node.field.relation_name.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), s_node.field.relation_name.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    order_fields_.push_back(Field(table,table->table_meta().field(s_node.field.attribute_name.c_str())));
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  // TODO add expression copy
  select_stmt->tables_.swap(tables);
  select_stmt->query_exprs_.swap(query_exprs);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->aggregation_funcs_.swap(aggregation_funcs);
  select_stmt->order_fields_ = order_fields_;
  select_stmt->order_info = order_info_;
  select_stmt->order_by_ = !(select_sql.sort.size() == 0);
  stmt = select_stmt;
  return RC::SUCCESS;
}
