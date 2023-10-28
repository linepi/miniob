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

#include "common/log/log.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

UpdateStmt::~UpdateStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  UpdateStmt *update_stmt = new UpdateStmt();

  RC rc = RC::SUCCESS;
  const char *table_name = update_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument: db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  for (const std::pair<std::string, Expression *> &p : update_sql.av) {
    const std::string &attribute_name = p.first;
    Value value_impl;
    rc = p.second->try_get_value(value_impl);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in try get value %s", strrc(rc));
      return rc;
    }
    FieldMeta *field_meta = const_cast<FieldMeta *>(table->table_meta().field(attribute_name.c_str()));

    if (nullptr == field_meta) {
      LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), attribute_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }

    update_stmt->field_metas_.push_back(field_meta);
    update_stmt->values_.push_back(value_impl);
  }


  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(table_name, table));
  rc = FilterStmt::create(db,
      table,
      &table_map,
      update_sql.condition,
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  update_stmt->table_ = table;
  update_stmt->filter_stmt_ = filter_stmt;

  stmt = update_stmt;
  return rc;
}
