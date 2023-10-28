/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/expr/expression.h"

InsertStmt::InsertStmt(Table *table,  std::vector<std::vector<Value>> * values_list)
    : table_(table), values_list_(values_list)
{}

RC InsertStmt::create(Db *db, const InsertSqlNode &inserts, Stmt *&stmt)
{
  RC rc = RC::SUCCESS;
  const char *table_name = inserts.relation_name.c_str();

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  std::vector<std::vector<Value>> * values_list_impl = new std::vector<std::vector<Value>>;
  for (size_t i = 0; i < inserts.values_list->size(); i++) {
    std::vector<Value> values_impl;
    std::vector<Expression *> exprs = (*inserts.values_list)[i];
    if (exprs.empty()) {
      LOG_WARN("insert exprs num is zero");
      return RC::INVALID_ARGUMENT;
    }
    const int value_num = static_cast<int>(exprs.size());
    const TableMeta &table_meta = table->table_meta();
    const int field_num = table_meta.field_num() - table_meta.sys_field_num();
    if (field_num != value_num) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }

    // check fields type
    const int sys_field_num = table_meta.sys_field_num();
    for (int j = 0; j < value_num; j++) {
      Expression *expr = exprs[j];
      Value value_impl;
      rc = expr->try_get_value(value_impl);
      if (rc != RC::SUCCESS) {
        LOG_WARN("try get value error");
        return rc;
      }

      FieldMeta *field_meta = const_cast<FieldMeta *>(table_meta.field(j + sys_field_num));
      bool match = field_meta->match(value_impl);
      if (!match) {
        LOG_WARN("field does not match value(%s and %s)", 
          attr_type_to_string(field_meta->type()), 
          attr_type_to_string(value_impl.attr_type()));
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      values_impl.emplace_back(value_impl);
    }
    values_list_impl->emplace_back(values_impl);
  }
  // everything alright
  stmt = new InsertStmt(table, values_list_impl);
  return RC::SUCCESS;
}
