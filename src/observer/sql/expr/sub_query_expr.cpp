#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "event/sql_event.h"

RC value_extract(ValueWrapper &value, SessionStage *ss, SQLStageEvent *sql_event, std::vector<std::string> *father_tables);

static RC fill_value_for_correlated_query(
  SelectSqlNode *select, const Tuple &tuple, std::unordered_map<ConditionSqlNode *, ConditionSqlNode> &saved_con) {

  RC rc = RC::SUCCESS;
  if (!select) return rc;

  std::vector<ConditionSqlNode *> conditions;
  for (ConditionSqlNode &con : select->conditions) {
    conditions.push_back(&con);
  }
  for (JoinNode &jn : select->joins) {
    for (ConditionSqlNode &con : jn.on) {
      conditions.push_back(&con);
    }
  }

  Value v;
  for (ConditionSqlNode *con : conditions) {
    if (con->left_type == CON_ATTR) {
      if (std::find(select->relations.begin(), select->relations.end(), con->left_attr.relation_name) == select->relations.end()) {
        rc = tuple.find_cell(TupleCellSpec(con->left_attr.relation_name.c_str(), con->left_attr.attribute_name.c_str()), v);
        if (rc != RC::SUCCESS) return rc;
        saved_con.insert(std::pair<ConditionSqlNode *, ConditionSqlNode>(con, *con));
        con->left_type = CON_VALUE;
        con->left_value.value = v;
      }
    } else if (con->left_type == CON_VALUE && con->left_value.select) {
      rc = fill_value_for_correlated_query(con->left_value.select, tuple, saved_con);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    if (con->right_type == CON_ATTR) {
      if (std::find(select->relations.begin(), select->relations.end(), con->right_attr.relation_name) == select->relations.end()) {
        rc = tuple.find_cell(TupleCellSpec(con->right_attr.relation_name.c_str(), con->right_attr.attribute_name.c_str()), v);
        if (rc != RC::SUCCESS) return rc;
        saved_con.insert(std::pair<ConditionSqlNode *, ConditionSqlNode>(con, *con));
        con->right_type = CON_VALUE;
        con->right_value.value = v;
      }
    } else if (con->right_type == CON_VALUE && con->right_value.select) {
      rc = fill_value_for_correlated_query(con->right_value.select, tuple, saved_con);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
  }
  return rc;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const {
	return RC::SUCCESS;
}

RC SubQueryExpr::extract_value() {
  if (!to_be_select_) {
    return RC::SUCCESS;
  } 
}

RC SubQueryExpr::extract_value(const Tuple &tuple) {
	RC rc = RC::SUCCESS;
  if (!to_be_select_) {
    return RC::SUCCESS;
  } 

  // 开始子查询
  std::unordered_map<ConditionSqlNode *, ConditionSqlNode> saved_con;

  rc = fill_value_for_correlated_query(select_, tuple, saved_con);
  if (rc != RC::SUCCESS) {
    LOG_WARN("fill_value_for_correlated_query error");
  }

  sql_event_->set_correlated_query(true);
  rc = value_extract(value_, value_.ss, value_.sql_event, nullptr);

  for (auto &node : saved_con) {
    *node.first = node.second;
  }

  if (rc != RC::SUCCESS) {
    LOG_WARN("correlated sub query value extract error %s\n",  strrc(rc));
    return values_;
  }

  if (value_.values->size() == 0) {
    value_.values->push_back(Value(EMPTY_TYPE));
  } 
  values_.swap(*value_.values);
  delete value_.values;
  value_.values = nullptr;

  return values_;
}