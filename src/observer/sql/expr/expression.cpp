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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "event/sql_event.h"

using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type)
    : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr()
{}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }

  switch (cast_type_) {
    case BOOLEANS: {
      bool val = value.get_boolean();
      cast_value.set_boolean(val);
    } break;
    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported convert from type %d to %d", child_->value_type(), cast_type_);
    }
  }
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &cell) const
{
  RC rc = child_->get_value(tuple, cell);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(cell, cell);
}

RC CastExpr::try_get_value(Value &value) const
{
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, value);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right, ConjuctType right_op)
    : comp_(comp), left_(std::move(left)), right_(std::move(right)), right_op_(right_op)
{}

ComparisonExpr::~ComparisonExpr()
{}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  return left.compare_op(right, comp_, result);
}

static RC is_in(CompOp op, Value &lv, const std::vector<Value> &rvs, bool &result) {
  RC rc = RC::SUCCESS;
  if (op == CompOp::IN && rvs[0].attr_type() == EMPTY_TYPE) {
    result = false;
    return rc;
  } 
  if (op == CompOp::NOT_IN && rvs[0].attr_type() == EMPTY_TYPE) {
    result = true;
    return rc;
  } 
  if (op == CompOp::IN) {
    for (const Value &v : rvs) {
      bool tmp_res;
      rc = lv.compare_op(v, CompOp::EQUAL_TO, tmp_res); 
      if (rc != RC::SUCCESS) 
        break;
      if (tmp_res) {
        result = true;
        break;
      }
    }
  } else {
    result = true;
    for (const Value &v : rvs) {
      bool tmp_res;
      rc = lv.compare_op(v, CompOp::EQUAL_TO, tmp_res); 
      if (v.attr_type() == NULL_TYPE || tmp_res) {
        result = false;
        break;
      }
    }
  }
  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() != ExprType::VALUE || right_->type() != ExprType::VALUE) {
    return RC::INVALID_ARGUMENT; 
  }
  ValueExpr *left_value_expr = static_cast<ValueExpr *>(left_.get());
  ValueExpr *right_value_expr = static_cast<ValueExpr *>(right_.get());
  // if (left_value_expr->to_be_select() || right_value_expr->to_be_select()) {
  //   return RC::INVALID_ARGUMENT;
  // }

  bool result;
  RC rc = RC::SUCCESS;

  Value lv = left_value_expr->get_value();
  Value rv = right_value_expr->get_value();
  const std::vector<Value> &lvs = left_value_expr->get_values();
  const std::vector<Value> &rvs = right_value_expr->get_values();

  if (comp_ == EXISTS || comp_ == NOT_EXISTS) {
    cell.set_boolean(
      (comp_ == NOT_EXISTS && rvs[0].attr_type() == EMPTY_TYPE) ||
      (comp_ == EXISTS && rvs[0].attr_type() != EMPTY_TYPE));
    return rc;
  }
   
  if (comp_ == IN || comp_ == NOT_IN) {
    Value lv = left_value_expr->get_value();

    rc = is_in(comp_, lv, rvs, result);
    if (rc != RC::SUCCESS) {
      LOG_WARN("op `in` error %s", strrc(rc));
      return rc;
    }

    cell.set_boolean(result);
    return rc;
  }

  if (lvs.size() > 1 && comp_ != IS && comp_ != IS_NOT) {
    return RC::INVALID_ARGUMENT;
  }
  if (rvs.size() > 1) {
    return RC::INVALID_ARGUMENT;
  }

  rc = lv.compare_op(rv, comp_, result);
  if (rc != RC::SUCCESS) {
    LOG_WARN("compare op error %s", strrc(rc));
  }
  cell.set_boolean(result);
  return rc;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  bool result = false;

  if (comp_ == EXISTS || comp_ == NOT_EXISTS) {
    const std::vector<Value> &rvs = static_cast<ValueExpr *>(right_.get())->get_values(tuple, rc);
    if (rc != RC::SUCCESS) {
      LOG_WARN("get values from ValueExpr error");
      return rc;
    }
    assert(rvs.size() > 0);
    if (rvs[0].attr_type() == EMPTY_TYPE) 
      value.set_boolean(comp_ == NOT_EXISTS);
    else
      value.set_boolean(comp_ == EXISTS);
    return rc;
  }

  if (comp_ == IN || comp_ == NOT_IN) {
    const std::vector<Value> &rvs = static_cast<ValueExpr *>(right_.get())->get_values(tuple, rc);
    if (rc != RC::SUCCESS) {
      LOG_WARN("get values from ValueExpr error");
      return rc;
    }
    Value lv;
    if (left_->type() == ExprType::FIELD) 
      left_->get_value(tuple, lv);
    else {
      assert(left_->type() == ExprType::VALUE);
      const std::vector<Value> &lvs = static_cast<ValueExpr *>(left_.get())->get_values(tuple, rc); 
      if (rc != RC::SUCCESS) {
        LOG_WARN("get values from ValueExpr error");
        return rc;
      }
      if (lvs.size() > 1) {
        LOG_WARN("multi value found at `in` clause left");
        return RC::SUB_QUERY_OP_IN;
      }
      assert(lvs.size() > 0);
      lv = lvs[0];
    }
    rc = is_in(comp_, lv, rvs, result);
    if (rc != RC::SUCCESS) {
      LOG_WARN("op `in` error %s", strrc(rc));
      return rc;
    }

    value.set_boolean(result);
    return rc;
  }

  Value lv;
  Value rv;

  if (left_->type() == ExprType::FIELD)
    left_->get_value(tuple, lv);
  else {
    assert(left_->type() == ExprType::VALUE);
    const std::vector<Value> &lvs = static_cast<ValueExpr *>(left_.get())->get_values(tuple, rc); 
    if (rc != RC::SUCCESS) {
      LOG_WARN("get values from ValueExpr error");
      return rc;
    }
    if (lvs.size() != 1 && comp_ != IS && comp_ != IS_NOT) {
      LOG_WARN("multi value error in common op");
      return RC::SUB_QUERY_MULTI_VALUE;
    }
    assert(lvs.size() > 0);
    lv = lvs[0];
    if (lv.attr_type() == EMPTY_TYPE) 
      lv.set_null();
  }

  if (right_->type() == ExprType::FIELD)
    right_->get_value(tuple, rv);
  else {
    assert(right_->type() == ExprType::VALUE);
    const std::vector<Value> &rvs = static_cast<ValueExpr *>(right_.get())->get_values(tuple, rc); 
    if (rc != RC::SUCCESS) {
      LOG_WARN("get values from ValueExpr error");
      return rc;
    }
    if (rvs.size() != 1 && comp_ != IS && comp_ != IS_NOT) {
      LOG_WARN("multi value error in common op");
      return RC::SUB_QUERY_MULTI_VALUE;
    }
    assert(rvs.size() > 0);
    rv = rvs[0];
    if (rv.attr_type() == EMPTY_TYPE) 
      rv.set_null();
  }

  rc = lv.compare_op(rv, comp_, result);
  
  if (rc != RC::SUCCESS) {
    LOG_WARN("compare op error %s", strrc(rc));
  }
  value.set_boolean(result);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(ConjuctType type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == CONJ_AND && !bool_value) || (conjunction_type_ == CONJ_OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == CONJ_AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS &&
      right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }
  
  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();

  switch (arithmetic_type_) {
    case Type::ADD: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() + right_value.get_int());
      } else {
        value.set_float(left_value.get_float() + right_value.get_float());
      }
    } break;

    case Type::SUB: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() - right_value.get_int());
      } else {
        value.set_float(left_value.get_float() - right_value.get_float());
      }
    } break;

    case Type::MUL: {
      if (target_type == AttrType::INTS) {
        value.set_int(left_value.get_int() * right_value.get_int());
      } else {
        value.set_float(left_value.get_float() * right_value.get_float());
      }
    } break;

    case Type::DIV: {
      if (target_type == AttrType::INTS) {
        if (right_value.get_int() == 0) {
          // NOTE: 设置为整数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为整数最大值。
          value.set_int(numeric_limits<int>::max());
        } else {
          value.set_int(left_value.get_int() / right_value.get_int());
        }
      } else {
        if (right_value.get_float() > -EPSILON && right_value.get_float() < EPSILON) {
          // NOTE: 设置为浮点数最大值是不正确的。通常的做法是设置为NULL，但是当前的miniob没有NULL概念，所以这里设置为浮点数最大值。
          value.set_float(numeric_limits<float>::max());
        } else {
          value.set_float(left_value.get_float() / right_value.get_float());
        }
      }
    } break;

    case Type::NEGATIVE: {
      if (target_type == AttrType::INTS) {
        value.set_int(-left_value.get_int());
      } else {
        value.set_float(-left_value.get_float());
      }
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}