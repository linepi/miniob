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
void select_to_string(SelectSqlNode *select, std::string &out);
void select_extract_relation(SelectSqlNode *select, std::unordered_set<std::string> &relations);
void update_extract_relation(UpdateSqlNode *update, std::unordered_set<std::string> &relations);
void show_relations(std::unordered_set<std::string> &relations, SessionStage *ss, SQLStageEvent *sql_event);

RC values_from_sql_stdout(std::string &std_out, std::vector<Value> &values)
{
  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);
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
        if (!(v.attr_type() == CHARS && v.to_string().size() == 0))
          values.push_back(v);
      }
    }
  }
  if (values.size() == 0) {
    values.push_back(Value(EMPTY_TYPE));
  }
  return RC::SUCCESS;
}

static RC check_correlated_query(SelectSqlNode *sub_query, std::vector<std::string> *father_tables, bool &result)
{
  // 初始假设它是非关联的子查询
  result = false;
  std::vector<const ConditionSqlNode *> conditions;
  RC rc = RC::SUCCESS;

  for (const auto &condition : sub_query->conditions) {
    conditions.push_back(&condition);
  }

  for (const auto &join : sub_query->joins) {
    for (const auto &condition : join.on) {
      conditions.push_back(&condition);
    }
  }

  for (const auto con : conditions) {
    const ConditionSqlNode &condition = *con;
    // 检查左边的属性
    if (condition.left_type == CON_ATTR) {
      const std::string *relation_name = &condition.left_attr.relation_name;
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

    if (condition.right_type == CON_ATTR) {
      const std::string *relation_name = &condition.right_attr.relation_name;
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

    // 如果值包含子查询，递归检查
    if (condition.left_value.select != nullptr) {
      rc = check_correlated_query(condition.left_value.select, father_tables, result);
      if (rc != RC::SUCCESS) return rc;
      if (result)
        return rc;
    }
    if (condition.right_value.select != nullptr) {
      check_correlated_query(condition.right_value.select, father_tables, result);
      if (rc != RC::SUCCESS) return rc;
      if (result)
        return rc;
    }
  }

  return rc;
}


RC value_extract(
    ValueWrapper &value, SessionStage *ss, SQLStageEvent *sql_event, std::vector<std::string> *father_tables)
{
  if (value.select == nullptr)
    return RC::SUCCESS;
  // 检测关联查询
  RC   rc = RC::SUCCESS;
  if (father_tables) {
    bool is_correlated_query;
    rc = check_correlated_query(value.select, father_tables, is_correlated_query);
    if (rc != RC::SUCCESS) {
      LOG_WARN("error in correlated query check %s", strrc(rc));
      return rc;
    }
    if (is_correlated_query) {
      value.sql_event = sql_event;
      value.ss = ss;
      return RC::SUCCESS;
    }
  }

  ParsedSqlNode *node = new ParsedSqlNode();
  node->flag          = SCF_SELECT;
  node->selection     = *value.select;
  SQLStageEvent stack_sql_event(*sql_event, false);
  stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));

  // for debug
  std::string select_string;
  select_string += "\33[1;33m[sub-query]\33[0m: [";
  select_to_string(value.select, select_string);
  select_string += "]";
  // end for debug

  rc = handle_sql(ss, &stack_sql_event, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  Writer       *thesw        = new SilentWriter();
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
  value.values     = new std::vector<Value>;
  rc               = values_from_sql_stdout(sw->content(), *value.values);

  // for debug
  select_string += " = [";
  for (Value &v : *value.values) {
    select_string += v.to_string() + "(" + attr_type_to_string(v.attr_type()) + ")";
    if (&v != &((*value.values).back())) {
      select_string += ", ";
    }
  }
  select_string += "];";
  sql_debug(select_string.c_str());
  // end for debug

  delete sw;

  if (value.values->size() == 0) {
    value.values->push_back(Value(NULL_TYPE));
  }  

  if (!sql_event->correlated_query()) {
    delete value.select;
    value.select = nullptr;
  }

  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query parse values from std out failed. rc=%s", strrc(rc));
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

      for (std::vector<ValueWrapper> &values : *(node->insertion.values_list)) {
        for (ValueWrapper &value : values) {
          rc = value_extract(value, ss, sql_event, father_tables);
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

      for (std::pair<std::string, ValueWrapper> &av_ : node->update.av) {
        rc = value_extract(av_.second, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.av value extract error");
          goto deal_rc;
        }
      }
      for (ConditionSqlNode &con : node->update.conditions) {
        rc = value_extract(con.left_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.conditions.left value extract error");
          goto deal_rc;
        }
        rc = value_extract(con.right_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.conditions.right value extract error");
          goto deal_rc;
        }
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

      for (ConditionSqlNode &con : node->selection.conditions) {
        rc = value_extract(con.left_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("select.conditions.left value extract error");
          goto deal_rc;
        }
        rc = value_extract(con.right_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("select.conditions.right value extract error");
          goto deal_rc;
        }
      }
      for (JoinNode &jn : node->selection.joins) {
        for (ConditionSqlNode &con : jn.on) {
          rc = value_extract(con.left_value, ss, sql_event, father_tables);
          if (rc != RC::SUCCESS) {
            LOG_WARN("select.join.on.left value extract error");
            goto deal_rc;
          }
          rc = value_extract(con.right_value, ss, sql_event, father_tables);
          if (rc != RC::SUCCESS) {
            LOG_WARN("select.join.on.right value extract error");
            goto deal_rc;
          }
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

      for (ConditionSqlNode &con : node->deletion.conditions) {
        rc = value_extract(con.left_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("delete.condition.left value extract error");
          goto deal_rc;
        }
        if (rc != RC::SUCCESS)
          goto deal_rc;
        rc = value_extract(con.right_value, ss, sql_event, father_tables);
        if (rc != RC::SUCCESS) {
          LOG_WARN("delete.condition.right value extract error");
          goto deal_rc;
        }
      }
      break;
    }
    default: {
      break;
    }
  }
deal_rc:
  delete father_tables;
  return rc;
}

RC ResolveStage::handle_request(SessionStage *ss, SQLStageEvent *sql_event, bool main_query)
{
  // for debug
  if (main_query) {
    if (sql_event->sql_node().get()->flag == SCF_SELECT) {
      std::unordered_set<std::string> relations;
      select_extract_relation(&(sql_event->sql_node().get()->selection), relations); 
      show_relations(relations, ss, sql_event);
    } else if (sql_event->sql_node().get()->flag == SCF_UPDATE) {
      std::unordered_set<std::string> relations;
      update_extract_relation(&(sql_event->sql_node().get()->update), relations); 
      show_relations(relations, ss, sql_event);
    }
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
