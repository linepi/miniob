#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "event/sql_event.h"

RC sub_query_extract(SelectSqlNode *select, SessionStage *ss, SQLStageEvent *sql_event, std::string &std_out, std::vector<AttrInfoSqlNode> *attr_infos = nullptr);
RC value_from_sql_stdout(std::string &std_out, Value &value);

RC check_correlated_query(SubQueryExpr *expr, std::vector<std::string> *father_tables, bool &result) {
  RC rc = RC::SUCCESS;
  if (!father_tables) {
    result = false;
    return rc;
  }

  SubQueryExpr *sub_query_expr = expr;
  if (sub_query_expr->correlated()) {
    result = true;
    return rc;
  }

  SelectSqlNode *sub_query = sub_query_expr->select();

  std::vector<Expression *> conditions;
  conditions.push_back(sub_query->condition);
  for (const auto &join : sub_query->joins) {
    conditions.push_back(join.condition);
  }

  for (Expression *con : conditions) {
    if (!con) continue;
    if(!con->is_condition()) {
      LOG_WARN("not a valid condition");
      return RC::INVALID_ARGUMENT;
    }

    std::vector<FieldExpr *> field_exprs;
    auto visitor = [&field_exprs](std::unique_ptr<Expression> &expr) {
      assert(expr->type() == ExprType::FIELD);
      field_exprs.push_back(static_cast<FieldExpr *>(expr.get()));
      return RC::SUCCESS;
    };
    rc = con->visit_field_expr(visitor, false);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in visit_field_expr: %s", strrc(rc));
      return rc;
    }

    for (FieldExpr *field_expr : field_exprs) {
      const std::string *relation_name = &field_expr->rel_attr().relation_name;
      if (relation_name->empty()) {
        if (sub_query->relations.size() > 1) {
          rc = RC::SCHEMA_FIELD_MISSING_TABLE;
          return rc;
        } else {
          relation_name = &sub_query->relations[0];
        }
      } else {
        bool in_subquery_relations = std::find(
          sub_query->relations.begin(), 
          sub_query->relations.end(),
          *relation_name) != sub_query->relations.end();
        bool in_father_relations = std::find(
          father_tables->begin(),
          father_tables->end(),
          *relation_name) != father_tables->end();
        if (!in_father_relations && !in_subquery_relations) {
          rc = RC::SUB_QUERY_CORRELATED_TABLE_NOT_FOUND;
          return rc;
        }
        if (!in_subquery_relations && in_father_relations) {
          result = true;
          return rc;
        }
      }
    }

    std::vector<SubQueryExpr *> sub_querys;
    rc = con->get_subquery_expr(sub_querys);
    if (rc != RC::SUCCESS) {
      LOG_WARN("get_subquery_expr failed: %s", strrc(rc));
      return rc;
    }
    for (SubQueryExpr * sub_query : sub_querys) {
      rc = check_correlated_query(sub_query, father_tables, result);
      if (rc != RC::SUCCESS) {
        LOG_WARN("check_correlated_query failed");
        return rc;
      }
    }
  }
  return rc;
}

static RC fill_value_for_correlated_query(
  SelectSqlNode *select, const std::vector<std::string> &father_relations, 
  const Tuple &tuple, std::unordered_map<std::unique_ptr<Expression> *, FieldExpr *> &saved_fieldexpr) {

  RC rc = RC::SUCCESS;
  if (!select) return rc;

  std::vector<Expression *> conditions;
  conditions.push_back(select->condition);
  for (const auto &join : select->joins) {
    conditions.push_back(join.condition);
  }

  for (Expression *con : conditions) {
    if (!con) continue;
    if(!con->is_condition()) {
      LOG_WARN("not a valid condition");
      return RC::INVALID_ARGUMENT;
    }

    auto visitor = [&father_relations, &tuple, &saved_fieldexpr](std::unique_ptr<Expression> &expr) {
      assert(expr->type() == ExprType::FIELD);
      FieldExpr *field_expr = static_cast<FieldExpr *>(expr.get());
      Value v;
      RC rc = tuple.find_cell(
        TupleCellSpec(field_expr->rel_attr().relation_name.c_str(), 
                      field_expr->rel_attr().attribute_name.c_str()), v);
      if (rc != RC::SUCCESS) 
        return RC::SUCCESS;
      
      expr.release();
      saved_fieldexpr.insert(std::pair<std::unique_ptr<Expression> *, FieldExpr *>(&expr, field_expr));
      ValueExpr *value_expr = new ValueExpr(v);
      expr.reset(value_expr);
      return RC::SUCCESS;
    };
    rc = con->visit_field_expr(visitor, true);
    if (rc != RC::SUCCESS) {
      LOG_WARN("visit_field_expr failed: %s", strrc(rc));
      return rc;
    }

    std::vector<SubQueryExpr *> sub_querys;
    rc = con->get_subquery_expr(sub_querys);
    if (rc != RC::SUCCESS) {
      LOG_WARN("get_subquery_expr failed: %s", strrc(rc));
      return rc;
    }

    for (SubQueryExpr * sub_query : sub_querys) {
      rc = fill_value_for_correlated_query(sub_query->select(), father_relations, tuple, saved_fieldexpr);
      if (rc != RC::SUCCESS) {
        LOG_WARN("check_correlated_query failed");
        return rc;
      }
    }
  }
  return rc;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const {
  RC rc = RC::SUCCESS;
  if (!correlated_) {
    value = value_;
    return rc;
  } 

  // 开始子查询
  std::unordered_map<std::unique_ptr<Expression> *, FieldExpr *> saved_fieldexpr;

  rc = fill_value_for_correlated_query(select_, select_->relations, tuple, saved_fieldexpr);
  if (rc != RC::SUCCESS) {
    LOG_WARN("fill_value_for_correlated_query error");
  }

  std::string std_out;
  sql_event_->set_correlated_query(true);
  rc = sub_query_extract(select_, ss_, sql_event_, std_out);
  sql_event_->set_correlated_query(false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error in sub_query_extract %s", strrc(rc));
    return rc;
  }
  rc = value_from_sql_stdout(std_out, value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error in value_from_sql_stdout %s", strrc(rc));
    return rc;
  }

  for (auto node : saved_fieldexpr) {
    node.first->reset(node.second);
  }

  SubQueryExpr *nonconstthis = const_cast<SubQueryExpr *>(this);
  nonconstthis->value_ = value;
  return rc;
}