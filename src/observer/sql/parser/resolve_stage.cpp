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

#include <string>
#include <algorithm>
#include <unordered_set>
#include <regex>

#include "resolve_stage.h"
#include "storage/db/db.h"
#include "storage/table/view.h"
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
RC convert_view_to_physical(SelectSqlNode &select_node, std::unordered_set<std::string> &view2physical);

void get_relation_from_select(SelectSqlNode *select, std::unordered_set<std::string> &relations);
void get_relation_from_update(UpdateSqlNode *update, std::unordered_set<std::string> &relations);
void show_relations(std::unordered_set<std::string> &relations, SessionStage *ss, SQLStageEvent *sql_event);
void show_expressions(ParsedSqlNode *node);

std::regex rx("^[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$"); 

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


RC create_view_select_extract(
    CreateViewSqlNode &cvs, SessionStage *ss, SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;

  std::string std_out;
  cvs.select_attr_infos = new std::vector<AttrInfoSqlNode>;
  rc = sub_query_extract(cvs.select, ss, sql_event, std_out, cvs.select_attr_infos);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error in create select extract's sub query extrace");
    delete cvs.select_attr_infos;
    return rc;
  }
  cvs.select_attr_names = new std::vector<std::string>;

  while (std_out.back() == '\0') {
    std_out = std_out.substr(0, std_out.size() - 1);
  }

  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);

  std::string &line = lines[0];
  if (line.find("FALIURE", 0) != std::string::npos) {
    return RC::SUB_QUERY_FAILURE;
  }
  std::vector<std::string> tokens = split_str(line, " | ");
  for (std::string &token : tokens) {
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
    if (std::find(cvs.select_attr_names->begin(), cvs.select_attr_names->end(), to_add) != cvs.select_attr_names->end()) {
      return RC::INVALID_ARGUMENT;
    } else {
      cvs.select_attr_names->push_back(to_add);
    }
  }

  return rc;
}

RC create_table_select_extract(
    CreateTableSqlNode &cts, SessionStage *ss, SQLStageEvent *sql_event)
{
  if (cts.select == nullptr)
    return RC::SUCCESS;
  RC rc = RC::SUCCESS;

  std::string std_out;
  if (!cts.select_attr_infos)
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
          LOG_WARN("get same table attr names while create table with select!");
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
  if (attr_infos && attr_infos->size() == 0) {
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
  if (attr_infos && attr_infos->size() == 0)
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
          LOG_WARN("no such table %s", relation_name.c_str());
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
        LOG_WARN("no such table %s", node->deletion.relation_name.c_str());
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
      rc = create_table_select_extract(node->create_table, ss, sql_event);
      if (rc != RC::SUCCESS) {
        LOG_WARN("create_table value extract error");
        goto deal_rc;
      }
      break;
    }
    case SCF_CREATE_VIEW: {
      rc = create_view_select_extract(node->create_view, ss, sql_event);
      if (rc != RC::SUCCESS) {
        LOG_WARN("create_view attr extract error");
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

RC ResolveStage::handle_view_select(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  RC rc = RC::SUCCESS;
  Db *db = sql_event->session_event()->session()->get_current_db();
  SelectSqlNode &select_node = sql_event->sql_node()->selection;

  std::vector<View *> views;
  for (std::string &rel : select_node.relations) {
    Table *table = db->find_table(rel.c_str());
    if (table && table->type() == Table::VIEW) {
      views.push_back(static_cast<View *>(table));
      break;
    }
  }
  if (views.size() == 0) return RC::SUCCESS;

  // build tables
  std::unordered_set<std::string> view2physical;
  for (View *v : views) {
    CreateTableSqlNode cnode;
    cnode.select = v->table_meta().select_;
    rc = alias_pre_process(v->table_meta().select_);
    if (rc != RC::SUCCESS) {
      LOG_WARN("view select alias preprocess failed. rc=%s", strrc(rc));
      return rc;
    }
    cnode.relation_name = v->name() + std::string("--phyview");
    view2physical.insert(v->name());
    cnode.select_attr_infos = new std::vector<AttrInfoSqlNode>;
    cnode.build_for_view = true;
    for (const FieldMeta &meta : *(v->table_meta().field_metas())) {
      cnode.select_attr_infos->push_back(AttrInfoSqlNode{meta.type(), meta.name(), (unsigned long)meta.len(), true});
    }

    ParsedSqlNode *node = new ParsedSqlNode();
    node->flag          = SCF_CREATE_TABLE;
    node->create_table  = cnode;
    SQLStageEvent stack_sql_event(*sql_event, false);
    stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));
    rc = handle_sql(ss, &stack_sql_event, false);
    if (rc != RC::SUCCESS) {
      LOG_WARN("sub create table failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  rc = convert_view_to_physical(sql_event->sql_node()->selection, view2physical);
  if (rc != RC::SUCCESS) {
    LOG_WARN("error while convert view to physical %s", strrc(rc));
    return rc;
  }
  return rc;
}

RC ResolveStage::handle_view_insert(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  RC rc = RC::SUCCESS;
  Db *db = sql_event->session_event()->session()->get_current_db();
  Table *table = nullptr;

  InsertSqlNode &node = sql_event->sql_node()->insertion;
  table = db->find_table(node.relation_name.c_str());
  if (!table || table->type() != Table::VIEW) return RC::SUCCESS;
  View *view = static_cast<View *>(table);

  SelectSqlNode *view_select = view->table_meta().select_;
  assert(view_select);

  if (node.values_list->at(0).size() != view->table_meta().field_num() - view->table_meta().sys_field_num()) {
    LOG_WARN("size of insert value and attr mismatch");
    sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_ARGUMENT);
    return RC::INVALID_ARGUMENT;
  } 
  for (SelectAttr &attr : view_select->attributes) {
    if (!attr.expr_nodes[0]) return RC::INVALID_VIEW_DML;
    bool agg;
    attr.expr_nodes[0]->is_aggregate(agg);
    if (agg) 
      return RC::INVALID_VIEW_DML;
    // 假设不要求支持指定字段的插入，那么所有的必须都为非表达式
    if (attr.expr_nodes[0]->type() != ExprType::FIELD &&
        attr.expr_nodes[0]->type() != ExprType::STAR  &&
        attr.expr_nodes[0]->type() != ExprType::VALUE) {
      LOG_WARN("view's special attribute is not allowed to insert");
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
      return RC::INVALID_VIEW_DML;
    }
  }

  if (view_select->groupby.size() != 0 || view_select->relations.size() > 1 ||
      view_select->joins.size() != 0) {
    LOG_WARN("groupby and join view not allowed to insert");
    sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_ARGUMENT);
    return RC::INVALID_ARGUMENT;
  }

  std::unordered_map<int, int> site_map;
  Table *real_table = db->find_table(view_select->relations[0].c_str());
  for (int j = 0; j < real_table->table_meta().field_num() - real_table->table_meta().sys_field_num(); j++) {
    std::string field_name = real_table->table_meta().field_metas()->at(real_table->table_meta().sys_field_num() + j).name();
    size_t i;
    for (i = 0; i < view_select->attributes.size(); i++) {
      std::string &attr_name = static_cast<FieldExpr *>(view_select->attributes[i].expr_nodes[0])->rel_attr().attribute_name;
      if (attr_name == field_name) {
        site_map[j] = i;
        break;
      }
    }
    if (i == view_select->attributes.size()) {
      site_map[j] = -1;
    }
  }
  
  std::vector<std::vector<Expression *>> values_list;
  if (view_select->attributes[0].expr_nodes[0]->type() == ExprType::STAR) {
    node.relation_name = view_select->relations[0];
    return rc;
  }

  for (std::vector<Expression *> &values : *node.values_list) {
    size_t idx = 0;
    std::vector<Expression *> new_values;
    for (int j = 0; j < real_table->table_meta().field_num() - real_table->table_meta().sys_field_num(); j++) {
      if (site_map[j] != -1) {
        new_values.push_back(values[idx++]);
      } else {
        new_values.push_back(new ValueExpr(Value(NULL_TYPE)));
      }
    }
    values_list.push_back(new_values);
  }
  node.values_list->swap(values_list);      
  node.relation_name = view_select->relations[0];
  return rc;
}

RC ResolveStage::handle_view_update(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  RC rc = RC::SUCCESS;
  Db *db = sql_event->session_event()->session()->get_current_db();
  Table *table = nullptr;

  UpdateSqlNode &node = sql_event->sql_node()->update;
  table = db->find_table(node.relation_name.c_str());
  if (!table || table->type() != Table::VIEW) return RC::SUCCESS;
  View *view = static_cast<View *>(table);

  SelectSqlNode *view_select = view->table_meta().select_;
  assert(view_select);

  for (SelectAttr &attr : view_select->attributes) {
    if (!attr.expr_nodes[0]) return RC::INVALID_VIEW_DML;
    bool agg;
    attr.expr_nodes[0]->is_aggregate(agg);
    if (agg) {
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
      return RC::INVALID_VIEW_DML;
    }
  }

  if (view_select->groupby.size() != 0) {
    LOG_WARN("groupby view not allowed to update");
    sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
    return RC::INVALID_VIEW_DML;
  }

  std::vector<Table *> tables;
  for (std::string &n : view_select->relations) {
    Table *t;
    if ((t = (db->find_table(n.c_str()))) != nullptr) {
      tables.push_back(t);
    } else {
      LOG_WARN("-");
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
      return RC::INVALID_VIEW_DML;
    }
  }
  for (JoinNode &j : view_select->joins) {
    Table *t;
    if ((t = db->find_table(j.relation_name.c_str())) != nullptr) {
      tables.push_back(t);
    } else {
      LOG_WARN("-");
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
      return RC::INVALID_VIEW_DML;
    }
  }

  // 首先获取表达式映射
  std::unordered_map<std::string, Expression *> expr_map; 
  for (size_t i = 0; i < view->table_meta().field_metas()->size(); i++) {
    const FieldMeta &meta = view->table_meta().field_metas()->at(i);
    if (view_select->attributes[0].expr_nodes[0]->type() == ExprType::STAR) {
      StarExpr *star_expr = static_cast<StarExpr *>(view_select->attributes[0].expr_nodes[0]);
      Table *view_select_table = db->find_table(view_select->relations[0].c_str());
      assert(view_select_table);
      const FieldMeta &field = view_select_table->table_meta().field_metas()->at(i);
      RelAttrSqlNode attr("", field.name(), "");
      FieldExpr *field_expr = new FieldExpr(attr);
      expr_map[meta.name()] = field_expr;
    } else {
      expr_map[meta.name()] = view_select->attributes[i].expr_nodes[0];
    }
  }

  // 排除对表达式的更新
  for (std::pair<std::string, Expression *> &av_elem : node.av) {
    std::string &view_attr_name = av_elem.first;
    if (expr_map.find(view_attr_name) != expr_map.end()) {
      Expression *expr = expr_map[view_attr_name];
      if (expr->type() != ExprType::FIELD) {
        LOG_WARN("view's special attribute is not allowed to update");
        sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
        return RC::INVALID_VIEW_DML;
      }
    } else {
      LOG_WARN("-");
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_ARGUMENT);
      return RC::INVALID_ARGUMENT;
    }
  }

  /*
    对于update的set，假设set了表A的字段
    update condition涉及表:
      // 1.A；直接对A进行更新，条件就是这个条件。
      // 3.B表：直接对这个表进行更新，无更新条件。
      // 2.A, B都有：
    view select condition:
      // 1.A：直接拿进来
      // 3.B：直接不要
      // 2.两个表都有：用另一个表的属性的值填充表达式中那个表的字段，每填充一次，生成一个新的表达式，用or连接，最终生成一个表达式。
  */
  if (tables.size() == 1) {
    for (std::pair<std::string, Expression *> &av_elem : node.av) {
      if (expr_map.find(av_elem.first) != expr_map.end()) {
        Expression *expr = expr_map[av_elem.first];
        assert(expr->type() == ExprType::FIELD);
        FieldExpr *field_expr = static_cast<FieldExpr *>(expr);
        av_elem.first = field_expr->rel_attr().attribute_name;
      } else {
        LOG_WARN("-");
        sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_ARGUMENT);
        return RC::INVALID_ARGUMENT;
      }
    }

    auto substitutor = [&](std::unique_ptr<Expression> &e) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(e.get());
      if (expr_map.find(field_expr->rel_attr().attribute_name) != expr_map.end()) {
        e.reset(expr_map[field_expr->rel_attr().attribute_name]);
      } else {
        return RC::INVALID_ARGUMENT;
      }
      return RC::SUCCESS;
    };
    rc = node.condition->visit_field_expr(substitutor, false);
    if (rc != RC::SUCCESS) {
      LOG_WARN("-");
      sql_event->session_event()->sql_result()->set_return_code(rc);
      return rc;
    }

    if (node.condition && view_select->condition) {
      ConjunctionExpr *new_expr = new ConjunctionExpr(CONJ_AND, node.condition, view_select->condition);
      node.condition = new_expr;
    } else {
      if (view_select->condition) {
        node.condition = view_select->condition;
      }
    }
    node.relation_name = view_select->relations[0];
  } else {
    LOG_WARN("do not support join view update");
    sql_event->session_event()->sql_result()->set_return_code(RC::UNIMPLENMENT);
    return RC::UNIMPLENMENT;
  }

  return rc;
}

RC ResolveStage::handle_view_delete(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  RC rc = RC::SUCCESS;
  Db *db = sql_event->session_event()->session()->get_current_db();
  Table *table = nullptr;

  DeleteSqlNode &node = sql_event->sql_node()->deletion;
  table = db->find_table(node.relation_name.c_str());
  if (!table || table->type() != Table::VIEW) return RC::SUCCESS;
  View *view = static_cast<View *>(table);

  SelectSqlNode *view_select = view->table_meta().select_;
  assert(view_select);

  for (SelectAttr &attr : view_select->attributes) {
    if (!attr.expr_nodes[0]) return RC::INVALID_VIEW_DML;
    bool agg;
    attr.expr_nodes[0]->is_aggregate(agg);
    if (agg) {
      sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_VIEW_DML);
      return RC::INVALID_VIEW_DML;
    }
  }
  if (view_select->groupby.size() != 0 || view_select->relations.size() > 1 ||
      view_select->joins.size() != 0) {
    LOG_WARN("groupby and join view not allowed to delete");
    sql_event->session_event()->sql_result()->set_return_code(RC::INVALID_ARGUMENT);
    return RC::INVALID_ARGUMENT;
  }

  // 生成表达式map
  std::unordered_map<std::string, Expression *> expr_map; 
  for (size_t i = 0; i < view->table_meta().field_metas()->size(); i++) {
    const FieldMeta &meta = view->table_meta().field_metas()->at(i);
    if (view_select->attributes[0].expr_nodes[0]->type() == ExprType::STAR) {
      Table *view_select_table = db->find_table(view_select->relations[0].c_str());
      assert(view_select_table);
      const FieldMeta &field = view_select_table->table_meta().field_metas()->at(i);
      RelAttrSqlNode attr("", field.name(), "");
      FieldExpr *field_expr = new FieldExpr(attr);
      expr_map[meta.name()] = field_expr;
    } else {
      expr_map[meta.name()] = view_select->attributes[i].expr_nodes[0];
    }
  }
  // 将delete中的条件表达式替换为对原表的表达式
  auto substitutor = [&](std::unique_ptr<Expression> &e) {
    FieldExpr *field_expr = static_cast<FieldExpr *>(e.get());
    if (expr_map.find(field_expr->rel_attr().attribute_name) != expr_map.end()) {
      e.reset(expr_map[field_expr->rel_attr().attribute_name]);
    } else {
      return RC::INVALID_ARGUMENT;
    }
    return RC::SUCCESS;
  };
  rc = node.condition->visit_field_expr(substitutor, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("-");
    sql_event->session_event()->sql_result()->set_return_code(rc);
    return rc;
  }
  // 然后将此表达式与原表进行与运算生成一个新的where
  if (node.condition && view_select->condition) {
    ConjunctionExpr *new_expr = new ConjunctionExpr(CONJ_AND, node.condition, view_select->condition);
    node.condition = new_expr;
  } else {
    if (view_select->condition) {
      node.condition = view_select->condition;
    }
  }
  node.relation_name = view_select->relations[0];
  return rc;
}

RC ResolveStage::handle_view(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  switch (sql_event->sql_node()->flag) {
    case SCF_INSERT: {
      return handle_view_insert(ss, sql_event, main_query);
    } break;
    case SCF_DELETE: {
      return handle_view_delete(ss, sql_event, main_query);
    } break;
    case SCF_UPDATE: {
      return handle_view_update(ss, sql_event, main_query);
    } break;
    case SCF_SELECT: {
      return handle_view_select(ss, sql_event, main_query);
    } break;
    default: {
      return RC::SUCCESS;
    } break;
  }
}

RC ResolveStage::alias_pre_process(SelectSqlNode *select_sql)
{
  if (!select_sql) return RC::SUCCESS;

  if (select_sql->attributes.size() == 0) {
    LOG_WARN("select attribute size is zero");
    return RC::INVALID_ARGUMENT;
  }
  for (const SelectAttr &select_attr : select_sql->attributes) {
    if (select_attr.expr_nodes.size() == 0) {
      LOG_WARN("select attribute expr is empty");
      return RC::INVALID_ARGUMENT;
    }
  }

  RC rc = RC::SUCCESS; 
  std::map<std::string, std::string> table2alias_map_tmp; ///< alias-->table_name
  std::map<std::string, int> alias_exist_tmp;  

  for (size_t i= 0; i< select_sql->relations.size(); i++)
  {
    if (select_sql->table_alias[i].empty()){
      if (!alias_exist_tmp[select_sql->relations[i]]){
        if (!alias_exis[select_sql->relations[i]]) continue;
      }
      if (alias_exist_tmp[select_sql->relations[i]]){
        select_sql->relations[i] = table2alias_map_tmp[select_sql->relations[i]]; 
        continue;
      }
      if (alias_exis[select_sql->relations[i]]){
        select_sql->relations[i] = table2alias_mp[select_sql->relations[i]]; 
        continue;
      }
    }
    if (alias_exist_tmp[select_sql->table_alias[i]]){
      return RC::SAME_ALIAS;
    }
    table2alias_map_tmp[select_sql->table_alias[i]] = select_sql->relations[i];
    alias_exist_tmp[select_sql->table_alias[i]] = 1;

    if (alias_exis[select_sql->table_alias[i]])continue;
    
    table2alias_mp[select_sql->table_alias[i]] = select_sql->relations[i];
    alias_exis[select_sql->table_alias[i]] = 1;
  }

  for (JoinNode &node : select_sql->joins)
  {
    if (node.table_alias.empty()){
      if (!alias_exist_tmp[node.relation_name] && !alias_exis[node.relation_name]){
        continue;
      }
      if (alias_exist_tmp[node.relation_name]){
        node.relation_name = table2alias_map_tmp[node.relation_name]; 
        continue;
      }
      if (alias_exis[node.relation_name]){
        node.relation_name = table2alias_mp[node.relation_name]; 
        continue;
      }
    }
    if (alias_exist_tmp[node.table_alias]){
      return RC::SAME_ALIAS;
    }
    table2alias_map_tmp[node.table_alias] = node.relation_name;
    alias_exist_tmp[node.table_alias] = 1;

    if (alias_exis[node.table_alias])continue;
    
    table2alias_mp[node.table_alias] = node.relation_name;
    alias_exis[node.table_alias] = 1;
  }

  for (SelectAttr &select_node : select_sql->attributes)
  {
    if(select_node.expr_nodes.empty())continue;
    Expression *attr_expr = select_node.expr_nodes[0];

    if (attr_expr->type() != ExprType::FIELD && attr_expr->type() != ExprType::STAR){
      auto field_visitor = [&](std::unique_ptr<Expression> &f) {
        FieldExpr *field_expr = static_cast<FieldExpr *>(f.get());
        RelAttrSqlNode &node = field_expr->rel_attr();
        if (alias_exist_tmp[node.relation_name]){
          node.relation_name = table2alias_map_tmp[node.relation_name];
        }
        else if (alias_exis[node.relation_name]){
          node.relation_name = table2alias_mp[node.relation_name];
        }
        if (field_exis[node.attribute_name]){
          node.attribute_name = field2alias_mp[node.attribute_name];
        }
        return RC::SUCCESS;
      };
      rc = attr_expr->visit_field_expr(field_visitor, true);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    } else if (attr_expr->type() == ExprType::FIELD) {
      bool is_agg;
      RC rc = attr_expr->is_aggregate(is_agg);
      if(!is_agg)
      {
        FieldExpr *field_expr = static_cast<FieldExpr *>(attr_expr);         
        RelAttrSqlNode &node = field_expr->rel_attr();
        if (!field_exis[node.alias] && !node.alias.empty() && node.attribute_name != "*"){
          field2alias_mp[node.alias] = node.attribute_name;
          field_exis[node.alias] = 1;
        }
        if (alias_exist_tmp[node.relation_name]){
          node.relation_name = table2alias_map_tmp[node.relation_name];
          continue;
        }
        if (alias_exis[node.relation_name]){
          node.relation_name = table2alias_mp[node.relation_name];
        }
      }
      else
      {
        FieldExpr *field_expr = static_cast<FieldExpr *>(attr_expr); 
        RelAttrSqlNode &node = field_expr->rel_attr();
        node.alias = "";
        if (alias_exist_tmp[node.relation_name]){
          node.relation_name = table2alias_map_tmp[node.relation_name]; 
          continue;
        }
        if (alias_exis[node.relation_name]){
          node.relation_name = table2alias_mp[node.relation_name];
        }
      }
    } else {
      StarExpr *star_expr = static_cast<StarExpr *>(attr_expr);
      bool is_agg;
      RC rc = star_expr->is_aggregate(is_agg);
      if(!star_expr->alias().empty() && is_agg == false)
        return RC::SAME_ALIAS;
      else {
        if (!alias_exist_tmp[star_expr->relation()]){
          if (!alias_exis[star_expr->relation()]) continue;
        }
        if (alias_exist_tmp[star_expr->relation()]){
          star_expr->set_relation(table2alias_map_tmp[star_expr->relation()]); 
          continue;
        }
        if (alias_exis[star_expr->relation()]){
          star_expr->set_relation(table2alias_mp[star_expr->relation()]); 
          continue;
        }
      }  
    }
  }

  for (SortNode &sort_node : select_sql->sort)
  {
    RelAttrSqlNode &node = sort_node.field;
    if (!field_exis[node.alias]){
      field2alias_mp[node.alias] = node.attribute_name;
      field_exis[node.alias] = 1;
    }
    if (alias_exist_tmp[node.relation_name]){
      node.relation_name = table2alias_map_tmp[node.relation_name];
    }
    if (alias_exis[node.relation_name]){
      node.relation_name = table2alias_mp[node.relation_name];
    }
  }

  std::vector<Expression *> conditions;
  if (select_sql->condition)
    conditions.push_back(select_sql->condition);
  for (const JoinNode &jnode : select_sql->joins) {
    const char *table_name = jnode.relation_name.c_str();
    if (jnode.condition)
      conditions.push_back(jnode.condition);
  }

  for (Expression *con : conditions)
  {
    if (!con) continue;
    if(!con->is_condition()) {
      LOG_WARN("not a valid condition");
      return RC::INVALID_ARGUMENT;
    }

    auto visitor = [&](std::unique_ptr<Expression> &f) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(f.get());
      RelAttrSqlNode &node = field_expr->rel_attr();
      if (field_exis[node.attribute_name] && node.relation_name.empty()){
        return RC::SAME_ALIAS;
      }
      if (alias_exist_tmp[node.relation_name]){
        node.relation_name = table2alias_map_tmp[node.relation_name];
      }
      else if (alias_exis[node.relation_name]){
        node.relation_name = table2alias_mp[node.relation_name];
      }
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
      rc = alias_pre_process(sub_query->select());
      if (rc != RC::SUCCESS) {
        LOG_WARN("check_correlated_query failed");
        return rc;
      }
    }
  }
  return RC::SUCCESS;
}

RC ResolveStage::handle_alias(SessionStage *ss, SQLStageEvent *sql_event, bool main_query) {
  RC rc = RC::SUCCESS;
  if (main_query) {
    field2alias_mp.clear();
    field_exis.clear();
    alias_exis.clear();
    table2alias_mp.clear();
  }
  if (sql_event->sql_node() && main_query) { 
    SessionEvent *session_event = sql_event->session_event();
    SqlResult    *sql_result    = session_event->sql_result();
    SelectSqlNode* select_sql = nullptr;
    
    if (sql_event->sql_node()->flag == SqlCommandFlag::SCF_SELECT)
      select_sql = &sql_event->sql_node()->selection;
    else if (sql_event->sql_node()->flag == SqlCommandFlag::SCF_CREATE_TABLE)
      select_sql = sql_event->sql_node()->create_table.select;
    else if (sql_event->sql_node()->flag == SqlCommandFlag::SCF_CREATE_VIEW)
      select_sql = sql_event->sql_node()->create_view.select;

    rc = alias_pre_process(select_sql);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to do select pre-process. rc=%s", strrc(rc));
      sql_result->set_return_code(rc);
      return rc;
    }
  }
  return rc;
}

RC convert_view_to_physical(SelectSqlNode &select_node, std::unordered_set<std::string> &view2physical) {
  RC rc = RC::SUCCESS;
  // every thing related to table name convert to view tables
  std::vector<std::string *> relations;
  std::vector<Expression *> exprs;
  for (std::string &rel : select_node.relations) 
    relations.push_back(&rel);
  for (JoinNode &j : select_node.joins) {
    exprs.push_back(j.condition);
    relations.push_back(&j.relation_name);
  }
  exprs.push_back(select_node.condition);
  for (SortNode &s : select_node.sort) {
    if (!s.field.relation_name.empty()) 
      relations.push_back(&s.field.relation_name);
  }
  for (Expression *e : select_node.groupby) 
    exprs.push_back(e);
  for (SelectAttr &attr : select_node.attributes)
    exprs.push_back(attr.expr_nodes[0]);


  for (std::string *rel : relations) {
    if (view2physical.find(*rel) != view2physical.end()) {
      *rel += "--phyview";
    }
  }
  for (Expression *e : exprs) {
    if (!e) continue;
    std::vector<SubQueryExpr *> sub_query_exprs;
    e->get_subquery_expr(sub_query_exprs);
    auto visitor = [&](std::unique_ptr<Expression> &f) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(f.get());
      if (view2physical.find(field_expr->rel_attr().relation_name) != view2physical.end()) {
        field_expr->rel_attr().relation_name += "--phyview";
      }
      return RC::SUCCESS;
    };
    rc = e->visit_field_expr(visitor, false);
    if (rc != RC::SUCCESS) 
      assert(0);
    for (SubQueryExpr *sub_query_expr : sub_query_exprs) {
      rc = convert_view_to_physical(*sub_query_expr->select(), view2physical);
      if (rc != RC::SUCCESS) 
        assert(0);
    }
  }
  return RC::SUCCESS;
}