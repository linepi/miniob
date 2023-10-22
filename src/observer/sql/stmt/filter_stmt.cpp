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
  for (FilterUnit *unit : filter_units_) {
    delete unit;
  }
  filter_units_.clear();
}

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;

  FilterStmt *tmp_stmt = new FilterStmt();
  for (int i = 0; i < condition_num; i++) {
    FilterUnit *filter_unit = nullptr;
    rc = create_filter_unit(db, default_table, tables, conditions[i], filter_unit);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create filter unit. condition index=%d", i);
      return rc;
    }
    tmp_stmt->filter_units_.push_back(filter_unit);
  }

  stmt = tmp_stmt;
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

RC FilterStmt::create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode &condition, FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;

  CompOp comp = condition.comp;
  if (comp < EQUAL_TO || comp >= NO_OP) {
    LOG_WARN("invalid compare operator : %d", comp);
    return RC::INVALID_ARGUMENT;
  }

  filter_unit = new FilterUnit;

  FilterObj filter_obj_left;
  if (condition.left_type == CON_ATTR) {
    Table *table = nullptr;
    const FieldMeta *field = nullptr;
    rc = get_table_and_field(db, default_table, tables, condition.left_attr, table, field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
    filter_obj_left.init_attr(Field(table, field));
  } else if (condition.left_type == CON_VALUE) {
    if (condition.left_values != nullptr) filter_obj_left.init_values(std::move(*condition.left_values)); // maybe something in (1,2,3)
    else filter_obj_left.init_value(condition.left_value);
  } else if (condition.left_type == CON_SUB_SELECT) {
    if (condition.left_values != nullptr) 
      filter_obj_left.init_values(*condition.left_values);
    else
      filter_obj_left.init_value(condition.left_value);
  } else if (condition.left_type == CON_UNDEFINED) { // exists op left
    filter_obj_left.init_value(Value(NULL_TYPE, nullptr, 0));
  } else {
    assert(0);
  }
  filter_unit->set_left(filter_obj_left);

  FilterObj filter_obj_right;
  if (condition.right_type == CON_ATTR) {
    Table *table = nullptr;
    const FieldMeta *field = nullptr;
    rc = get_table_and_field(db, default_table, tables, condition.right_attr, table, field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
    filter_obj_right.init_attr(Field(table, field));
  } else if (condition.right_type == CON_VALUE) {
    if (condition.right_values != nullptr) filter_obj_right.init_values(std::move(*condition.right_values)); // maybe something in (1,2,3)
    else filter_obj_right.init_value(condition.right_value);
  } else if (condition.right_type == CON_SUB_SELECT) {
    if (condition.right_values != nullptr) 
      filter_obj_right.init_values(*condition.right_values);
    else
      filter_obj_right.init_value(condition.right_value);
  } else if (condition.right_type == CON_UNDEFINED) { // exists op left
    filter_obj_right.init_value(Value(NULL_TYPE, nullptr, 0));
  } else {
    assert(0);
  }
  filter_unit->set_right(filter_obj_right);

  filter_unit->set_comp(comp);

  // 检查两个类型是否能够比较
  AttrType left, right;
  if (filter_unit->left().is_attr) left = filter_unit->left().field.attr_type();
  else {
    if (filter_unit->left().values.size() == 0) left = NULL_TYPE; // empty select
    else left = filter_unit->left().values[0].attr_type();
  }

  if (filter_unit->right().is_attr) right = filter_unit->right().field.attr_type();
  else {
    if (filter_unit->right().values.size() == 0) right = NULL_TYPE;
    else right = filter_unit->right().values[0].attr_type();
  }

  if (left != right) {
    char buf[4] = {'.','.','.','.'};
    Value left_value(left, buf, 4);
    Value right_value(right, buf, 4);
    bool result;
    RC ret = left_value.compare_op(right_value, comp, result);
    if (ret != RC::SUCCESS) {
      LOG_INFO("type mismatch: %s and %s", attr_type_to_string(left), attr_type_to_string(right));
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  if (comp == LIKE_OP || comp == NOT_LIKE_OP) {
    if (!(filter_unit->left().is_attr && !(filter_unit->right().is_attr))) {
      LOG_INFO("only `column name` like `pattern`");
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  return rc;
}
