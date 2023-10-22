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
  FilterObj filter_obj_right;
  CompOp filter_comp = comp;
  const ValueWrapper &rv = condition.right_value;
  const ValueWrapper &lv = condition.left_value;

  if (comp == EXISTS || comp == NOT_EXISTS) {
    filter_obj_left.init_value(Value(NULL_TYPE)); 
    filter_obj_right.init_value(Value(NULL_TYPE)); 
    if ((condition.comp == EXISTS && rv.values->size() > 0) ||
        (condition.comp == NOT_EXISTS && rv.values->size() == 0))
      filter_comp = IS;
    else 
      filter_comp = IS_NOT;
    filter_unit->set_left(filter_obj_left);
    filter_unit->set_right(filter_obj_right);
    filter_unit->set_comp(filter_comp); 
    return rc;
  } 

  Table *left_table = nullptr;
  Table *right_table = nullptr;
  const FieldMeta *left_field = nullptr;
  const FieldMeta *right_field = nullptr;
  if (condition.left_type == CON_ATTR) {
    rc = get_table_and_field(db, default_table, tables, condition.left_attr, left_table, left_field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
  } 
  if (condition.right_type == CON_ATTR) {
    rc = get_table_and_field(db, default_table, tables, condition.right_attr, right_table, right_field);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot find attr");
      return rc;
    }
  } 

  if (comp == IN || comp == NOT_IN) {
    if (!rv.values) {
      LOG_WARN("in op's right value can only be sub query");
      return RC::SUB_QUERY_OP_IN;
    }
    if (condition.left_type == CON_ATTR) {
      filter_obj_left.init_attr(Field(left_table, left_field));
    } else {
      filter_obj_left.init_value(lv.value);
    }
    filter_obj_right.init_values(*rv.values);
    filter_unit->set_left(filter_obj_left);
    filter_unit->set_right(filter_obj_right);
    filter_unit->set_comp(filter_comp); 
    return rc;
  } 

  if (rv.values && rv.values->size() > 1) {
    LOG_WARN("invalid values size: %d", rv.values->size());
    return RC::SUB_QUERY_MULTI_VALUE;
  }
  if (lv.values && lv.values->size() > 1) {
    LOG_WARN("invalid values size: %d", lv.values->size());
    return RC::SUB_QUERY_MULTI_VALUE;
  }

  // 检查两个类型是否能够比较
  AttrType left, right;

  if (condition.left_type == CON_ATTR) {
    left = left_field->type();
    filter_obj_left.init_attr(Field(left_table, left_field));
  } else {
    Value lv_impl;
    if (lv.values) {
      if (lv.values->size() == 0) lv_impl.set_null();
      else lv_impl = (*lv.values)[0];
    } else {
      lv_impl = lv.value;
    }
    left = lv_impl.attr_type();
    filter_obj_left.init_value(lv_impl);
  }

  if (condition.right_type == CON_ATTR) {
    right = right_field->type();
    filter_obj_right.init_attr(Field(right_table, right_field));
  } else {
    Value rv_impl;
    if (rv.values) {
      if (rv.values->size() == 0) rv_impl.set_null();
      else rv_impl = (*rv.values)[0];
    } else {
      rv_impl = rv.value;
    }
    right = rv_impl.attr_type();
    filter_obj_right.init_value(rv_impl);
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

  filter_unit->set_left(filter_obj_left);
  filter_unit->set_right(filter_obj_right);
  filter_unit->set_comp(filter_comp);

  return rc;
}
