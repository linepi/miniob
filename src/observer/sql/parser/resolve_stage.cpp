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
// Created by Longda on 2021/4/13.
//

#include <string.h>
#include <string>
#include <algorithm>
#include <unordered_set>
#include <regex>

#include "resolve_stage.h"
#include "storage/db/db.h"
#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "session/session_stage.h"
#include "sql/stmt/stmt.h"
#include "net/writer.h"
#include "net/communicator.h"
#include "net/silent_writer.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"

using namespace common;

RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);
RC sub_query_extract(SelectSqlNode *select, SessionStage *ss, SQLStageEvent *sql_event, std::string &std_out, std::vector<AttrInfoSqlNode> *attr_infos = nullptr);
RC check_correlated_query(SubQueryExpr *expr, std::vector<std::string> *father_tables, bool &result); 

void get_relation_from_select(SelectSqlNode *select, std::unordered_set<std::string> &relations);
void get_relation_from_update(UpdateSqlNode *update, std::unordered_set<std::string> &relations);
void show_relations(std::unordered_set<std::string> &relations, SessionStage *ss, SQLStageEvent *sql_event);
void show_expressions(ParsedSqlNode *node);

RC value_from_sql_stdout(std::string &std_out, Value &value)
{
  while (std_out.back() == '\0') {
    std_out = std_out.substr(0, std_out.size() - 1);
  }

  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);
  std::vector<Value> *values = new std::vector<Value>;
  for (size_t i = 0; i < lines.size(); i++) {
    std::string &line = lines[i];
    if (line.find("FALIURE", 0) != std::string::npos) {
      return RC::SUB_QUERY_FAILURE;
    }
    if (line.find(" | ", 0) != std::string::npos) {  // 多于一列
      return RC::SUB_QUERY_MULTI_COLUMN;
    }
    if (i != 0) {
      Value v;
      int rc = v.from_string(line);
      if (rc == 0) { // 是否为空白
        values->push_back(v);
      }
    }
  }

  if (values->size() == 0) {
    value.set_empty();
    delete values;
  } else {
    value.set_list(values);
  }
  return RC::SUCCESS;
}

// for string delimiter
std::vector<std::string> split_str(std::string s, std::string delimiter) {
  size_t pos_start = 0, pos_end, delim_len = delimiter.length();
  std::string token;
  std::vector<std::string> res;

  while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
      token = s.substr (pos_start, pos_end - pos_start);
      pos_start = pos_end + delim_len;
      res.push_back (token);
  }

  res.push_back (s.substr (pos_start));
  return res;
}

RC create_select_extract(
    CreateTableSqlNode &cts, SessionStage *ss, SQLStageEvent *sql_event)
{
  if (cts.select == nullptr)
    return RC::SUCCESS;
  RC rc = RC::SUCCESS;

  std::string std_out;
  cts.select_attr_infos = new std::vector<AttrInfoSqlNode>;
  rc = sub_query_extract(cts.select, ss, sql_event, std_out, cts.select_attr_infos);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error in create select extract's sub query extrace");
    delete cts.select_attr_infos;
    return rc;
  }
  cts.values_list     = new std::vector<std::vector<Value>>;
  cts.names = new std::vector<std::string>;

  while (std_out.back() == '\0') {
    std_out = std_out.substr(0, std_out.size() - 1);
  }

  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);
  for (size_t i = 0; i < lines.size(); i++) {
    std::string &line = lines[i];
    if (i == 0 && line.find("FALIURE", 0) != std::string::npos) {
      return RC::SUB_QUERY_FAILURE;
    }
    if (line.empty()) continue;
    std::vector<std::string> tokens = split_str(line, " | ");
    if (i != 0) {
      std::vector<Value> values;
      for (std::string &token : tokens) {
        Value v;
        int rc = v.from_string(token);
        assert(rc == 0);
        values.push_back(std::move(v));
      }
      if (tokens.size() > 0)
        cts.values_list->push_back(std::move(values));
    } else {
      for (std::string &token : tokens) {
        std::regex rx("^[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$"); 

        std::ptrdiff_t number_of_matches = std::distance(  // Count the number of matches inside the iterator
        std::sregex_iterator(token.begin(), token.end(), rx),
        std::sregex_iterator());

        std::string to_add;
        if (number_of_matches == 0) {
          to_add = token;
        } else {
          size_t dotindex = token.find('.', 0);
          to_add = token.substr(dotindex + 1);
        }
        if (std::find(cts.names->begin(), cts.names->end(), to_add) != cts.names->end()) {
          return RC::INVALID_ARGUMENT;
        } else {
          cts.names->push_back(to_add);
        }
      }
    }
  }
  return rc;
}

RC sub_query_extract(SelectSqlNode *select, SessionStage *ss, SQLStageEvent *sql_event, std::string &std_out, 
  std::vector<AttrInfoSqlNode> *attr_infos)
{
  if (select == nullptr) 
    return RC::SUCCESS;
  RC rc = RC::SUCCESS;

  ParsedSqlNode *node = new ParsedSqlNode();
  node->flag          = SCF_SELECT;
  node->selection     = *select;
  SQLStageEvent stack_sql_event(*sql_event, false);
  stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));

  rc = handle_sql(ss, &stack_sql_event, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  Writer       *thesw        = new SilentWriter();
  if (attr_infos) {
    static_cast<SilentWriter *>(thesw)->create_table_ = true;
  }
  Communicator *communicator = stack_sql_event.session_event()->get_communicator();
  Writer       *writer_bak   = communicator->writer();
  communicator->set_writer(thesw);

  bool need_disconnect = false;
  rc                   = communicator->write_result(stack_sql_event.session_event(), need_disconnect);
  LOG_INFO("sub query write result return %s", strrc(rc));
  communicator->set_writer(writer_bak);

  if ((rc = stack_sql_event.session_event()->sql_result()->return_code()) != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  SilentWriter *sw = static_cast<SilentWriter *>(thesw);
  if (rc != RC::SUCCESS) {
    LOG_WARN("values_from_sql_stdout gives %s", strrc(rc));
    delete sw;
    return rc;
  }
  std_out = sw->content();
  if (attr_infos)
    *attr_infos = sw->attr_infos();
  delete sw;
  return rc;
}

RC expression_sub_query_extract(Expression *expr, SessionStage *ss, SQLStageEvent *sql_event, std::vector<std::string> *father_tables) {
  RC rc = RC::SUCCESS;
  if (!expr) return rc;
  if (expr->type() == ExprType::FIELD) return rc;
  if (expr->type() == ExprType::STAR) return rc;
  if (expr->type() == ExprType::VALUE) return rc;
  if (expr->type() == ExprType::SUB_QUERY) {
    SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(expr);
    if (sub_query_expr->correlated()) return rc;

    bool is_correlated = false;
    rc = check_correlated_query(sub_query_expr, father_tables, is_correlated);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in check_correlated_query %s", strrc(rc));
      return rc;
    }
    if (is_correlated) {
      sub_query_expr->set_ss(ss);
      sub_query_expr->set_sql_event(sql_event);
      sub_query_expr->set_correlated(true);
      return rc;
    }

    std::string std_out;
    rc = sub_query_extract(sub_query_expr->select(), ss, sql_event, std_out);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in sub_query_extract %s", strrc(rc));
      return rc;
    }
    Value value;
    rc = value_from_sql_stdout(std_out, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in value_from_sql_stdout %s", strrc(rc));
      return rc;
    }
    sub_query_expr->set_value(value);

    return rc;
  }

  Expression *left = nullptr, *right = nullptr;
  if (expr->type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *expr_ = static_cast<ArithmeticExpr *>(expr);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else if (expr->type() == ExprType::COMPARISON) {
    ComparisonExpr *expr_ = static_cast<ComparisonExpr *>(expr);
    if (expr_->left())
      left = expr_->left().get();
    if (expr_->right())
      right = expr_->right().get();
  }
  else if (expr->type() == ExprType::CONJUNCTION) {
    ConjunctionExpr *expr_ = static_cast<ConjunctionExpr *>(expr);
    left = expr_->left().get();
    right = expr_->right().get();
  }
  else {
    assert(0); 
  }
  rc = expression_sub_query_extract(left, ss, sql_event, father_tables); 
  if (rc != RC::SUCCESS) {
    LOG_WARN("expression sub query extract error for %s type", EXPR_NAME[(int)left->type()]);
    return rc;
  }
  rc = expression_sub_query_extract(right, ss, sql_event, father_tables); 
  if (rc != RC::SUCCESS) {
    LOG_WARN("expression sub query extract error for %s type", EXPR_NAME[(int)right->type()]);
    return rc;
  }
  return rc;
}

RC ResolveStage::extract_values(std::unique_ptr<ParsedSqlNode> &node_, SessionStage *ss, SQLStageEvent *sql_event)
{
  ParsedSqlNode       *node = node_.get();
  RC                   rc   = RC::SUCCESS;
  std::vector<std::string> *father_tables = new std::vector<std::string>;
  Db                  *db = sql_event->session_event()->session()->get_current_db();
  Table               *t  = nullptr;
  assert(db);

  switch (node->flag) {
    case SCF_INSERT: {
      t = db->find_table(node->insertion.relation_name.c_str());
      if (!t) {
        LOG_WARN("no such table");
        rc = RC::SCHEMA_TABLE_NOT_EXIST;
        goto deal_rc;
      }
      father_tables->push_back(node->insertion.relation_name);

      for (std::vector<Expression *> &exprs : *(node->insertion.values_list)) {
        for (Expression *expr : exprs) {
          rc = expression_sub_query_extract(expr, ss, sql_event, father_tables);
          if (rc != RC::SUCCESS) {
            LOG_WARN("insert value extract error");
            goto deal_rc;
          }
        }
      }
      break;
    }
    case SCF_UPDATE: {
      t = db->find_table(node->update.relation_name.c_str());
      if (!t) {
        LOG_WARN("no such table");
        rc = RC::SCHEMA_TABLE_NOT_EXIST;
        goto deal_rc;
      }
      father_tables->push_back(node->update.relation_name);

      for (std::pair<std::string, Expression *> &av_ : node->update.av) {
        rc = expression_sub_query_extract(av_.second, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.av value extract error");
          goto deal_rc;
        }
      }
      rc = expression_sub_query_extract(node->update.condition, ss, sql_event, father_tables);
      if (rc != RC::SUCCESS) {
        LOG_WARN("update.condition value extract error");
        goto deal_rc;
      }
      break;
    }
    case SCF_SELECT: {
      for (std::string &relation_name : node->selection.relations) {
        t = db->find_table(relation_name.c_str());
        if (!t) {
          LOG_WARN("no such table");
          rc = RC::SCHEMA_TABLE_NOT_EXIST;
          goto deal_rc;
        }
        father_tables->push_back(relation_name);
      }
      for (SelectAttr &attr : node->selection.attributes) {
        if (attr.expr_nodes.size() == 0) continue;
        rc = expression_sub_query_extract(attr.expr_nodes[0], ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("selection.condition value extract error");
          goto deal_rc;
        }
      }

      rc = expression_sub_query_extract(node->selection.condition, ss, sql_event, father_tables);
      if (rc != RC::SUCCESS) {
        LOG_WARN("selection.condition value extract error");
        goto deal_rc;
      }
      for (JoinNode &jn : node->selection.joins) {
        rc = expression_sub_query_extract(jn.condition, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("JoinNode.condition value extract error");
          goto deal_rc;
        }
      }
      break;
    }
    case SCF_DELETE: {
      t = db->find_table(node->deletion.relation_name.c_str());
      if (!t) {
        LOG_WARN("no such table");
        rc = RC::SCHEMA_TABLE_NOT_EXIST;
        goto deal_rc;
      }
      father_tables->push_back(node->deletion.relation_name);

      rc = expression_sub_query_extract(node->deletion.condition, ss, sql_event, father_tables);
      if (rc != RC::SUCCESS) {
        LOG_WARN("selection.condition value extract error");
        goto deal_rc;
      }
      break;
    }
    case SCF_CREATE_TABLE: {
      rc = create_select_extract(node->create_table, ss, sql_event);
      if (rc != RC::SUCCESS) {
        LOG_WARN("create_table value extract error");
        goto deal_rc;
      }
      break;
    }
    case SCF_CALC: {
      for (Expression *expr : node->calc.expressions) {
        rc = expression_sub_query_extract(expr, ss, sql_event, nullptr);
        if (rc != RC::SUCCESS) {
          LOG_WARN("calc.expression value extract error");
          goto deal_rc;
        }
      }
    } 
    default: {
      break;
    }
  }
deal_rc:
  if (father_tables) {
    delete father_tables;
    father_tables = nullptr;
  }
  return rc;
}

RC ResolveStage::handle_request(SessionStage *ss, SQLStageEvent *sql_event, bool main_query)
{
  // for debug
  if (main_query) {
    std::unordered_set<std::string> relations;
    if (sql_event->sql_node().get()->flag == SCF_SELECT) {
      get_relation_from_select(&(sql_event->sql_node().get()->selection), relations); 
    } else if (sql_event->sql_node().get()->flag == SCF_UPDATE) {
      get_relation_from_update(&(sql_event->sql_node().get()->update), relations); 
    } else if (sql_event->sql_node().get()->flag == SCF_CREATE_TABLE) {
      if (sql_event->sql_node().get()->create_table.select)
        get_relation_from_select(sql_event->sql_node().get()->create_table.select, relations);
    }
    // show_relations(relations, ss, sql_event);
  }
  void test_main();
  // test_main();
  // end for debug

  RC            rc            = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult    *sql_result    = session_event->sql_result();

  Db *db = session_event->session()->get_current_db();
  if (nullptr == db) {
    LOG_ERROR("cannot find current db");
    rc = RC::SCHEMA_DB_NOT_EXIST;
    sql_result->set_return_code(rc);
    sql_result->set_state_string("no db selected");
    return rc;
  }

  rc = extract_values(const_cast<std::unique_ptr<ParsedSqlNode> &>(sql_event->sql_node()), ss, sql_event);
  if (rc != RC::SUCCESS) {
    LOG_WARN("extract_values error %s", strrc(rc));
    sql_result->set_return_code(rc);
    return rc;
  }

  // if (main_query)
  //   show_expressions(sql_event->sql_node().get());

  Stmt *stmt = nullptr;
  rc         = Stmt::create_stmt(db, *(sql_event->sql_node().get()), stmt);
  if (rc != RC::SUCCESS && rc != RC::UNIMPLENMENT) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
    sql_result->set_return_code(rc);
    return rc;
  }

  sql_event->set_stmt(stmt);

  return rc;
}
