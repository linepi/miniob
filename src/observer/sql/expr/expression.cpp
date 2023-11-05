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

#include "sql/expr/common_function.h"
#include "sql/expr/aggregation_func.h"
#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "event/sql_event.h"
#include <unordered_set>
#include <cmath>
#include <stack>
#include <unordered_map>
#include <algorithm>
#include <string>

using namespace std;

Expression::~Expression() {
  for (ExprFunc *func : funcs_) {
    delete func;
    func = nullptr;
  }
}

void Expression::add_func(AggType agg_type) { 
  funcs_.push_back(new AggregationFunc(agg_type)); 
}

void Expression::add_func(FunctionType func_type) { 
  funcs_.push_back(new CommonFunction(func_type)); 
}

void Expression::add_func(FunctionType func_type, std::string param) { 
  funcs_.push_back(new CommonFunction(func_type, param)); 
}

RC Expression::is_aggregate(bool &result) {
  result = false;
  auto visitor = [&result](Expression *expr) {
    std::vector<ExprFunc *> &funcs = expr->funcs();
    for (ExprFunc *func : funcs) {
      if (func->type() == ExprFunc::AGG) {
        result = true;
        break;
      }
    }
    return RC::SUCCESS;
  };
  RC rc = this->visit(visitor);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error while visit aggregate");
    return rc;
  }
  return RC::SUCCESS;
}

void Expression::toggle_aggregate(bool on) {
  auto visitor = [&](Expression *e) {
    e->agg_on_ = on;
    return RC::SUCCESS;
  };
  this->visit(visitor);
}

void Expression::reset_aggregate() {
  auto visitor = [&](Expression *expr) {
    std::vector<ExprFunc *> &funcs = expr->funcs();
    for (ExprFunc *func : funcs) {
      if (func->type() == ExprFunc::AGG) {
        static_cast<AggregationFunc *>(func)->reset();
      }
    }
    return RC::SUCCESS;
  };
  this->visit(visitor);
}

// TODO: only get the first type
RC Expression::get_aggregate(AggType &result) {
  result = AGG_UNDEFINED;
  auto visitor = [&result](Expression *expr) {
    std::vector<ExprFunc *> &funcs = expr->funcs();
    for (ExprFunc *func : funcs) {
      if (func->type() == ExprFunc::AGG) {
        result = static_cast<AggregationFunc *>(func)->agg_type_;
        break;
      }
    }
    return RC::SUCCESS;
  };
  RC rc = this->visit(visitor);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error while visit aggregate");
    return rc;
  }
  return RC::SUCCESS;
}

RC Expression::get_field_relations(std::unordered_set<std::string> &relations) {
  auto visitor = [&relations](std::unique_ptr<Expression> &expr) {
    assert(expr->type() == ExprType::FIELD);
    FieldExpr *field_expr = static_cast<FieldExpr *>(expr.get());
    if (!field_expr->rel_attr().relation_name.empty()) {
      relations.insert(field_expr->rel_attr().relation_name);
    }
    return RC::SUCCESS;
  };
  return visit_field_expr(visitor, true);
}

RC Expression::get_field_expr(std::vector<FieldExpr *> &field_exprs, bool deepinto) {
  auto visitor = [&field_exprs](std::unique_ptr<Expression> &expr) {
    assert(expr->type() == ExprType::FIELD);
    FieldExpr *field_expr = static_cast<FieldExpr *>(expr.get());
    field_exprs.push_back(field_expr);
    return RC::SUCCESS;
  };
  return visit_field_expr(visitor, deepinto);
}

RC Expression::func_impl(Value &value) const {
  Expression *non_const_this = const_cast<Expression *>(this);
  Value v = value;
  for (ExprFunc *func : non_const_this->funcs_) {
    RC rc = func->iterate(v, agg_on_);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while get function value %s", strrc(rc));
      return rc;
    }
  }
  value = v;
  return RC::SUCCESS;
}

// get all field expressions in a expression, not deep into sub query expression
RC Expression::visit_field_expr(std::function<RC (std::unique_ptr<Expression> &)> visitor, bool deepinto) {
  RC rc = RC::SUCCESS;
  if (this->type() == ExprType::VALUE) return rc;
  if (this->type() == ExprType::STAR) return rc;
  if (this->type() == ExprType::FIELD) {
    std::unique_ptr<Expression> field_expr;
    field_expr.reset(static_cast<FieldExpr *>(this));
    rc = visitor(field_expr);
    field_expr.release();
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit field_expr: %s", strrc(rc));
      return rc;
    }
    return rc;
  }

  if (this->type() == ExprType::SUB_QUERY) {
    if (!deepinto) return rc;
    SelectSqlNode *sub_query = static_cast<SubQueryExpr *>(this)->select();
    std::vector<Expression *> exprs;
    exprs.push_back(sub_query->condition);
    for (const auto &join : sub_query->joins) {
      exprs.push_back(join.condition);
    }

    for (SelectAttr &attr : sub_query->attributes) {
      exprs.push_back(attr.expr_nodes[0]);
    }

    for (Expression *expr : exprs) {
      if (!expr) continue;
      rc = expr->visit_field_expr(visitor, deepinto);
      if (rc != RC::SUCCESS) {
        LOG_WARN("error while visit field_expr: %s", strrc(rc));
        return rc;
      }
    }
    return rc;
  }

  std::unique_ptr<Expression> *left = nullptr, *right = nullptr;
  if (type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(this);
    left = &expr_->left();
    right = &expr_->right();
  }
  else if (type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(this);
    left = &expr_->left();
    right = &expr_->right();
  }
  else if (type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(this);
    left = &expr_->left();
    right = &expr_->right();
  }
  else {
    assert(0);
  }

  if (left->get()) { 
    if ((*left)->type() == ExprType::FIELD) {
      rc = visitor(*left);
      if (rc != RC::SUCCESS) {
        LOG_WARN("error while visit field_expr: %s", strrc(rc));
        return rc;
      }
    } else {
      rc = (*left)->visit_field_expr(visitor, deepinto);
      if (rc != RC::SUCCESS) {
        LOG_WARN("error while visit field_expr: %s", strrc(rc));
        return rc;
      }
    }
  }

  if (right->get()) {
    if ((*right)->type() == ExprType::FIELD) {
      rc = visitor(*right);
      if (rc != RC::SUCCESS) {
        LOG_WARN("error while visit field_expr: %s", strrc(rc));
        return rc;
      }
    } else {
      rc = (*right)->visit_field_expr(visitor, deepinto);
      if (rc != RC::SUCCESS) {
        LOG_WARN("error while visit field_expr: %s", strrc(rc));
        return rc;
      }
    }
  }

  return rc;
}

// not deep into
RC Expression::visit_comp_expr(std::function<RC (Expression *)> visitor) {
  RC rc = RC::SUCCESS;
  if (this->type() == ExprType::VALUE) return rc;
  if (this->type() == ExprType::STAR) return rc;
  if (this->type() == ExprType::FIELD) return rc;
  if (this->type() == ExprType::SUB_QUERY) return rc;  

  Expression *left, *right;
  if (type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(this);
    rc = visitor(expr_);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit comparison expr %s", strrc(rc));
      return rc;
    }
    return rc;
  }
  else if (type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else {
    assert(0);
  }

  rc = left->visit_comp_expr(visitor);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error while visit arith_expr: %s", strrc(rc));
    return rc;
  }
  rc = right->visit_comp_expr(visitor);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error while visit arith_expr: %s", strrc(rc));
    return rc;
  }

  return rc;
}

RC Expression::visit(std::function<RC (Expression *)> visitor) {
  RC rc = visitor(this);
  if (this->type() == ExprType::VALUE) return rc;
  if (this->type() == ExprType::STAR) return rc;
  if (this->type() == ExprType::FIELD) return rc;
  if (this->type() == ExprType::SUB_QUERY) return rc;

  Expression *left, *right;
  if (type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else {
    assert(0);
  }

  if (left) {
    rc = left->visit(visitor);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit arith_expr: %s", strrc(rc));
      return rc;
    }
  }
  if (right) {
    rc = right->visit(visitor);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit arith_expr: %s", strrc(rc));
      return rc;
    }
  }

  return rc;
}

RC Expression::get_subquery_expr(std::vector<SubQueryExpr *> &result) {
  RC rc = RC::SUCCESS;
  if (this->type() == ExprType::VALUE) return rc;
  if (this->type() == ExprType::STAR) return rc;
  if (this->type() == ExprType::FIELD) return rc;

  if (this->type() == ExprType::SUB_QUERY) {
    result.push_back(static_cast<SubQueryExpr *>(this));
    return rc;
  }

  Expression *left = nullptr, *right = nullptr;
  if (type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else {
    assert(0); 
  } 

  if (left) {
    rc = left->get_subquery_expr(result);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit field_expr: %s", strrc(rc));
      return rc;
    }
  }

  if (right) {
    rc = right->get_subquery_expr(result);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error while visit field_expr: %s", strrc(rc));
      return rc;
    }
  }

  return rc;
}

bool Expression::is_condition() const {
  if (type() == ExprType::CONJUNCTION || type() == ExprType::COMPARISON) 
    return true;
  return false;
}

std::string Expression::dump_tree(int indent) {
  std::string out;
  // 打印缩进
  for (int i = 0; i < indent; i++) {
      out += "  ";
  }

  // 打印类型
  switch (type()) {
    case ExprType::FIELD: {
      FieldExpr *field_expr = static_cast<FieldExpr*>(this);
      out += "FieldExpr(" + field_expr->name() + " : " + field_expr->alias() + "): ";
      if (field_expr->rel_attr().relation_name.empty()) {
        out += field_expr->rel_attr().attribute_name;
      } else {
        out += field_expr->rel_attr().relation_name + "." + field_expr->rel_attr().attribute_name;
      }
      break;
    }
    case ExprType::STAR: {
      out += "StarExpr(" + this->name() + ")";
      break;
    }
    case ExprType::VALUE: {
      ValueExpr *value_expr = static_cast<ValueExpr*>(this);
      out += "ValueExpr(" + value_expr->name() + " : " + this->alias() + "): ";
      Value v = value_expr->get_value();
      out += v.beauty_string();
      break;
    }
    case ExprType::SUB_QUERY: {
      out += "SubQuery(" + this->name() + " : " + this->alias() + ")";
      SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(this);
      Value v;
      sub_query_expr->try_get_value(v);
      out += ": " + v.beauty_string();
      break;
    }
    case ExprType::COMPARISON: {
      int comp = static_cast<int>(static_cast<ComparisonExpr*>(this)->comp());
      out += "ComparisonExpr(" + this->name() + " : " + this->alias() + "): ";
      out += COMPOP_NAME[comp];
      break;
    }
    case ExprType::CONJUNCTION: {
      ConjuctType conj = static_cast<ConjunctionExpr*>(this)->conjunction_type();
      out += "ConjunctionExpr(" + this->name() + " : " + this->alias() + "): ";
      if (conj == CONJ_AND) out += "AND";
      if (conj == CONJ_OR) out += "OR";
      break;
    }
    case ExprType::ARITHMETIC: {
      int type = static_cast<int>(static_cast<ArithmeticExpr*>(this)->arithmetic_type());
      out += "ArithmeticExpr(" + this->name() + " : " + this->alias() + "): ";
      out += ARITHMATIC_NAME[type];
      break;
    }
    default:
      std::cout << "UnknownExpr";
  }

  out += "\n";

  Expression *left = nullptr, *right = nullptr;
  if (type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
    if (left)
      out += left->dump_tree(indent + 1);
    if (right)
      out += right->dump_tree(indent + 1);
  }
  else if (type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(this);
    if (expr_->left()) {
      left = expr_->left().get();
      out += left->dump_tree(indent + 1);
    }
    right = expr_->right().get();
    out += right->dump_tree(indent + 1);
  }
  else if (type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(this);
    left = expr_->left().get();
    right = expr_->right().get();
    out += left->dump_tree(indent + 1);
    out += right->dump_tree(indent + 1);
  }

  return out;
}

/////////////////////////////////////////////////////////////////////////////////
RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error in find_cell: %s", strrc(rc));
    return rc;
  }
  return func_impl(value);
}

/////////////////////////////////////////////////////////////////////////////////
RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return func_impl(value);
}

/////////////////////////////////////////////////////////////////////////////////
RC StarExpr::get_value(int index, const Tuple &tuple, Value &value) const
{
  TupleCellSpec cellspec(fields_[index].table_name(), fields_[index].field_name());
  RC rc = tuple.find_cell(cellspec, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("starexpr get_value error");
    return rc;
  }
  return func_impl(value);
}

// 只发生在count(*)有的表达式中
RC StarExpr::get_value(const Tuple &tuple, Value &value) const {
  return func_impl(value);
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

  rc = cast(cell, cell);
  if (rc != RC::SUCCESS)
    return rc;
  return func_impl(cell);
}

RC CastExpr::try_get_value(Value &value) const
{
  RC rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = cast(value, value);
  if (rc != RC::SUCCESS)
    return rc;
  return func_impl(value);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, Expression *left, Expression *right)
    : comp_(comp), left_(left), right_(right)
{}

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::~ComparisonExpr()
{}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  return left.compare_op(right, comp_, result);
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  Value lv, rv;
  RC rc = RC::SUCCESS;
  rc = left_->try_get_value(lv);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  rc = right_->try_get_value(rv);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  bool result = false;

  rc = lv.compare_op(rv, comp_, result);
  if (rc != RC::SUCCESS) {
    LOG_WARN("compare op error %s", strrc(rc));
    return rc;
  }
  cell.set_boolean(result);
  return func_impl(cell);
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  bool result = false;

  Value lv, rv;
  rc = left_.get()->get_value(tuple, lv);
  if (rc != RC::SUCCESS) {
    LOG_WARN("get values error %s", strrc(rc));
    return rc;
  }

  rc = right_.get()->get_value(tuple, rv);
  if (rc != RC::SUCCESS) {
    LOG_WARN("get values error %s", strrc(rc));
    return rc;
  }

  rc = lv.compare_op(rv, comp_, result);
  if (rc != RC::SUCCESS) {
    LOG_WARN("compare op error %s", strrc(rc));
    return rc;
  }
  value.set_boolean(result);
  return func_impl(value);
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(ConjuctType type, Expression *left, Expression *right)
    : conjunction_type_(type), left_(left), right_(right)
{}

ConjunctionExpr::ConjunctionExpr(ConjuctType type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : conjunction_type_(type), left_(std::move(left)), right_(std::move(right))
{}

static Expression* init_(std::vector<Expression *> &comparisons, size_t idx) {
  if (comparisons.size() == idx + 2) {
    return new ConjunctionExpr(CONJ_AND, comparisons[comparisons.size() - 2], comparisons[comparisons.size() - 1]);
  }
  ConjunctionExpr *conj = new ConjunctionExpr(CONJ_AND, comparisons[idx], init_(comparisons, idx + 1));
  return conj;
}

RC ConjunctionExpr::init(std::vector<Expression *> &comparisons) {
  conjunction_type_ = CONJ_AND;
  if (comparisons.size() == 2) {
    left_.reset(comparisons[0]);
    right_.reset(comparisons[1]);
    return RC::SUCCESS;
  }
  left_.reset(comparisons[0]);
  right_.reset(init_(comparisons, 1));
  return RC::SUCCESS;
}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value lv, rv;
  rc = left_->get_value(tuple, lv);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, rv);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
    return rc;
  }
  bool lb = lv.get_boolean(), rb = rv.get_boolean();

  if (conjunction_type_ == CONJ_AND)
    value.set_boolean(lb && rb);
  else
    value.set_boolean(lb || rb);
  
  return func_impl(value);
}

RC ConjunctionExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value lv, rv;
  rc = left_->try_get_value(lv);
  if (rc != RC::SUCCESS) {
    LOG_INFO("try get value failed. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->try_get_value(rv);
  if (rc != RC::SUCCESS) {
    LOG_INFO("try get value failed. rc=%s", strrc(rc));
    return rc;
  }
  bool lb = lv.get_boolean(), rb = rv.get_boolean();

  if (conjunction_type_ == CONJ_AND)
    value.set_boolean(lb && rb);
  else
    value.set_boolean(lb || rb);
  
  return func_impl(value);
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithType type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithType type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::INTS &&
      right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != ARITH_DIV) {
    return AttrType::INTS;
  }
  
  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  switch (arithmetic_type_) {
    case ARITH_ADD: {
      value = left_value + right_value;
    } break;

    case ARITH_SUB: {
      value = left_value - right_value;
    } break;

    case ARITH_MUL: {
      value = left_value * right_value;
    } break;

    case ARITH_DIV: {
      value = left_value / right_value;
    } break;

    case ARITH_NEG: {
      value = Value(0) - left_value;
    } break;

    default: {
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
      return RC::INTERNAL;
    } break;
  }
  return func_impl(value);
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  if (left_.get()) {
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  if (right_.get()) {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
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
    LOG_INFO("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_INFO("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

const char *ARITHMATIC_NAME[] = {
  [(int)ARITH_ADD] = "ADD",
  [(int)ARITH_SUB] = "SUB",
  [(int)ARITH_MUL] = "MUL",
  [(int)ARITH_DIV] = "DIV",
  [(int)ARITH_NEG] = "NEGATIVE",
};

