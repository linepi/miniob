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

#pragma once

#include <vector>
#include <unordered_map>
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "sql/expr/expression.h"

class Db;
class Table;
class FieldMeta;

/*
  values包含一个元素的情况有两类，一类是(select stmt)退化而来，另一类是基本的value
  values包含多个元素由子查询得出
*/

struct FilterObj 
{
  bool is_attr;
  Field field;
  std::vector<Value> values;

  bool to_be_select = false;
  ValueWrapper value;

  void init_attr(const Field field)
  {
    is_attr = true;
    this->field = field;
  }

  void init_value(const Value &value)
  {
    is_attr = false;
    this->values.push_back(value);
  }

  void init_value(ValueWrapper &value) {
    is_attr = false;
    if (value.values) {
      if (value.values->size() == 0) {
        value.values->push_back(Value(EMPTY_TYPE));
      }
      this->values.swap(*value.values);

      delete value.values;
      value.values = nullptr;  
    } else if (value.select) {
      to_be_select = true;
      this->value = value;
    } else {
      this->values.push_back(value.value);
    }
  }
};

class FilterUnit 
{
public:
  FilterUnit() = default;
  ~FilterUnit()
  {}

  void set_comp(CompOp comp)
  {
    comp_ = comp;
  }

  CompOp comp() const
  {
    return comp_;
  }

  void set_left(const FilterObj &obj)
  {
    left_ = obj;
  }
  void set_right(const FilterObj &obj)
  {
    right_ = obj;
  }

  const FilterObj &left() const
  {
    return left_;
  }
  const FilterObj &right() const
  {
    return right_;
  }

  const ConjuctType right_op() const {
    return right_op_;
  }

  void set_right_op(ConjuctType right_op) {
    right_op_ = right_op;
  }

private:
  CompOp comp_ = NO_OP;
  FilterObj left_;
  FilterObj right_;
  ConjuctType right_op_;
};

/**
 * @brief Filter/谓词/过滤语句
 * @ingroup Statement
 */
class FilterStmt 
{
public:
  FilterStmt() = default;
  virtual ~FilterStmt();

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      Expression *condition, FilterStmt *&stmt);
  Expression *condition() const { return condition_; }

private:
  Expression *condition_;
};
