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

#pragma once

#include <string.h>
#include <memory>
#include <string>
#include <stack>

#include "sql/parser/value.h"
#include "common/log/log.h"
#include "storage/field/field.h"
#include "sql/parser/value_wrapper.h"
#include <unordered_set>

class Tuple;
class SubQueryExpr;
class FieldExpr;

/**
 * @defgroup Expression
 * @brief 表达式
 */

/**
 * @brief 表达式的抽象描述
 * @ingroup Expression
 * @details 在SQL的元素中，任何需要得出值的元素都可以使用表达式来描述
 * 比如获取某个字段的值、比较运算、类型转换
 * 当然还有一些当前没有实现的表达式，比如算术运算。
 *
 * 通常表达式的值，是在真实的算子运算过程中，拿到具体的tuple后
 * 才能计算出来真实的值。但是有些表达式可能就表示某一个固定的
 * 值，比如ValueExpr。
 */
class Expression 
{
public:
  Expression() = default;
  virtual ~Expression() = default;

  /**
   * @brief 根据具体的tuple，来计算当前表达式的值。tuple有可能是一个具体某个表的行数据
   */
  virtual RC get_value(const Tuple &tuple, Value &value) const = 0;

  /**
   * @brief 在没有实际运行的情况下，也就是无法获取tuple的情况下，尝试获取表达式的值
   * @details 有些表达式的值是固定的，比如ValueExpr，这种情况下可以直接获取值
   */
  virtual RC try_get_value(Value &value) const
  {
    return RC::UNIMPLENMENT;
  }

  /**
   * @brief 表达式的类型
   * 可以根据表达式类型来转换为具体的子类
   */
  virtual ExprType type() const = 0;

  /**
   * @brief 表达式值的类型
   * @details 一个表达式运算出结果后，只有一个值
   */
  virtual AttrType value_type() const = 0;

  /**
   * @brief 表达式的名字，比如是字段名称，或者用户在执行SQL语句时输入的内容
   */
  virtual std::string name() const { return name_; }
  virtual void set_name(std::string name) { name_ = name; }

  void add_func(AggType agg_type);
  void add_func(FunctionType func_type);

  RC func_impl(Value &value) const;
  std::vector<ExprFunc *> &funcs() { return funcs_; }

  // tool functions
  // get all field expressions in a expression, not deep into sub query expression
  bool is_condition() const;
  RC is_aggregate(bool &result) ;
  RC visit_field_expr(std::function<RC (std::unique_ptr<Expression> &)> visitor, bool deepinto);
  RC visit_comp_expr(std::function<RC (Expression *)> visitor);
  RC visit(std::function<RC (Expression *)> visitor);
  RC get_field_expr(std::vector<FieldExpr *> &field_exprs, bool deepinto);
  RC get_subquery_expr(std::vector<SubQueryExpr *> &result);
  RC get_relations(std::unordered_set<std::string> &relations);
  std::string dump_tree(int indent = 0);

private:
  std::string  name_;
  std::vector<ExprFunc *> funcs_;
};

/**
 * @brief 字段表达式
 * @ingroup Expression
 */
class FieldExpr : public Expression 
{
public:
  FieldExpr() = default;
  FieldExpr(const Table *table, const FieldMeta *field) : field_(table, field)
  {}
  FieldExpr(const Field &field) : field_(field)
  {}
  FieldExpr(RelAttrSqlNode &rel_attr) : rel_attr_(rel_attr)
  {}

  virtual ~FieldExpr() = default;

  ExprType type() const override { return ExprType::FIELD; }
  AttrType value_type() const override { return field_.attr_type(); }

  Field &field() { return field_; }
  void set_field(const Field &field) { field_ = field; }

  const Field &field() const { return field_; }
  const RelAttrSqlNode &rel_attr() const { return rel_attr_; }

  const char *table_name() const { return field_.table_name(); }

  const char *field_name() const { return field_.field_name(); }

  RC get_value(const Tuple &tuple, Value &value) const override;

private:
  Field field_;
  RelAttrSqlNode rel_attr_;
};

/**
 * @brief 字段表达式
 * @ingroup Expression
 */
class StarExpr : public Expression 
{
public:
  StarExpr() = default;
  virtual ~StarExpr() = default;

  ExprType type() const override { return ExprType::STAR; }

  std::vector<Field> &field() { return fields_; }
  void add_field(Field field) { fields_.push_back(field); }

  AttrType value_type() const override { return UNDEFINED; }

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC get_value(int index, const Tuple &tuple, Value &value) const;

private:
  std::vector<Field> fields_;
};

/**
 * @brief 常量值表达式
 * @ingroup Expression
 */
class ValueExpr : public Expression 
{
public:
  ValueExpr() = default;
  explicit ValueExpr(const Value &value) 
  { 
    value_ = value;
  }

  virtual ~ValueExpr() = default;

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC try_get_value(Value &value) const override { 
    value = value_;
    return func_impl(value);
  }

  ExprType type() const override { return ExprType::VALUE; }

  AttrType value_type() const override { 
      return value_.attr_type(); }

  void get_value(Value &value) const { 
    value = value_;
  }

  const Value &get_value() const { 
    return value_;
  }

private:
  Value value_;
};

/**
 * @brief 子查询表达式
 * @ingroup Expression
 */
class SubQueryExpr : public Expression 
{
public:
  SubQueryExpr() { value_.set_type(UNDEFINED); }
  explicit SubQueryExpr(SelectSqlNode *select) { select_ = select; value_.set_type(UNDEFINED); }
  virtual ~SubQueryExpr() = default;

  RC get_value(const Tuple &tuple, Value &value) const override;
  void set_value(const Value &value) { value_ = value; }

  RC try_get_value(Value &value) const override { 
    if (value_.attr_type() == UNDEFINED)
      return RC::SUB_QUERY_TO_BE_SELECT;
    else 
      value = value_; 
    return RC::SUCCESS;
  }

  ExprType type() const override { return ExprType::SUB_QUERY; }

  AttrType value_type() const override { 
    return value_.attr_type(); 
  }

  bool correlated() const { return correlated_; }
  void set_correlated(bool value) {
    correlated_ = value;
  }

  SelectSqlNode* select() const {
    return select_;
  }
  void set_select(SelectSqlNode* select) {
    select_ = select;
  }

  SessionStage* ss() const {
    return ss_;
  }
  void set_ss(SessionStage* ss) {
    ss_ = ss;
  }

  SQLStageEvent* sql_event() const {
    return sql_event_;
  }
  void set_sql_event(SQLStageEvent* sql_event) {
    sql_event_ = sql_event;
  }


private:
  bool correlated_ = false;
  SelectSqlNode *select_ = nullptr;
  SessionStage *ss_ = nullptr;
  SQLStageEvent *sql_event_ = nullptr;
  Value value_;
};

/**
 * @brief 类型转换表达式
 * @ingroup Expression
 */
class CastExpr : public Expression 
{
public:
  CastExpr(std::unique_ptr<Expression> child, AttrType cast_type);
  virtual ~CastExpr();

  ExprType type() const override
  {
    return ExprType::CAST;
  }
  RC get_value(const Tuple &tuple, Value &value) const override;

  RC try_get_value(Value &value) const override;

  AttrType value_type() const override { return cast_type_; }

  std::unique_ptr<Expression> &child() { return child_; }

private:
  RC cast(const Value &value, Value &cast_value) const;

private:
  std::unique_ptr<Expression> child_;  ///< 从这个表达式转换
  AttrType cast_type_;  ///< 想要转换成这个类型
};

/**
 * @brief 比较表达式
 * @ingroup Expression
 */
class ComparisonExpr : public Expression 
{
public:
  ComparisonExpr(CompOp comp, Expression *left, Expression *right);
  ComparisonExpr(CompOp comp, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  virtual ~ComparisonExpr();

  ExprType type() const override { return ExprType::COMPARISON; }

  RC get_value(const Tuple &tuple, Value &value) const override;

  AttrType value_type() const override { return BOOLEANS; }

  CompOp comp() const { return comp_; }

  std::unique_ptr<Expression> &left()  { return left_;  }
  std::unique_ptr<Expression> &right() { return right_; }

  /**
   * 尝试在没有tuple的情况下获取当前表达式的值
   * 在优化的时候，可能会使用到
   */
  RC try_get_value(Value &value) const override;

  /**
   * compare the two tuple cells
   * @param value the result of comparison
   */
  RC compare_value(const Value &left, const Value &right, bool &value) const;

private:
  CompOp comp_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief 联结表达式
 * @ingroup Expression
 * 多个表达式使用同一种关系(AND或OR)来联结
 * 当前miniob仅有AND操作
 */
class ConjunctionExpr : public Expression 
{
public:
  ConjunctionExpr() = default;
  ConjunctionExpr(ConjuctType type, Expression *left, Expression *right);
  ConjunctionExpr(ConjuctType type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  virtual ~ConjunctionExpr() = default;

  RC init(std::vector<Expression *> &comparisons);

  ExprType type() const override { return ExprType::CONJUNCTION; }

  AttrType value_type() const override { return BOOLEANS; }

  RC get_value(const Tuple &tuple, Value &value) const override;
  RC try_get_value(Value &value) const override;

  ConjuctType conjunction_type() const { return conjunction_type_; }

  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

private:
  ConjuctType conjunction_type_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief 算术表达式
 * @ingroup Expression
 */
extern const char *ARITHMATIC_NAME[];

class ArithmeticExpr : public Expression 
{
public:
  ArithmeticExpr(ArithType type, Expression *left, Expression *right);
  ArithmeticExpr(ArithType type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  virtual ~ArithmeticExpr() = default;

  ExprType type() const override { return ExprType::ARITHMETIC; }

  AttrType value_type() const override;

  RC get_value(const Tuple &tuple, Value &value) const override;
  RC try_get_value(Value &value) const override;

  ArithType arithmetic_type() const { return arithmetic_type_; }

  std::unique_ptr<Expression> &left() { return left_; }
  std::unique_ptr<Expression> &right() { return right_; }

private:
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;
  
private:
  ArithType arithmetic_type_;
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};