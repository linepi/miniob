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

#include "session_stage.h"

#include <string.h>
#include <string>

#include "common/conf/ini.h"
#include "common/log/log.h"
#include "common/lang/mutex.h"
#include "common/lang/string.h"
#include "common/seda/callback.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "net/server.h"
#include "net/communicator.h"
#include "session/session.h"
#include <map>
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "common/enum.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/parse_defs.h"

using namespace common;

// Constructor
SessionStage::SessionStage(const char *tag) : Stage(tag)
{}

SessionStage::SessionStage(SessionStage &other) {

}

// Destructor
SessionStage::~SessionStage()
{}

// Parse properties, instantiate a stage object
Stage *SessionStage::make_stage(const std::string &tag)
{
  SessionStage *stage = new (std::nothrow) SessionStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecutorStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

// Set properties for this object set in stage specific properties
bool SessionStage::set_properties()
{
  return true;
}

// Initialize stage params and validate outputs
bool SessionStage::initialize()
{
  return true;
}

// Cleanup after disconnection
void SessionStage::cleanup()
{

}

void SessionStage::handle_event(StageEvent *event)
{
  // right now, we just support only one event.
  handle_request(event);

  event->done_immediate();
  return;
}

RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);

static void clean_garbage(SessionStage *ss, SQLStageEvent *sql_event) {
  ParsedSqlNode *node = sql_event->sql_node().get();
  if (!node) return;

  // clean expr
  std::vector<Expression *> all_expr;
  switch (node->flag) {
    case SCF_INSERT: {
      for (std::vector<Expression *> &exprs : *(node->insertion.values_list)) {
        for (Expression *expr : exprs) {
          all_expr.push_back(expr);
        }
      }
      break;
    }
    case SCF_UPDATE: {
      for (std::pair<std::string, Expression *> &av_ : node->update.av) {
        all_expr.push_back(av_.second);
      }
      break;
    }
    case SCF_SELECT: {
      for (SelectAttr &attr : node->selection.attributes) {
        if (attr.expr_nodes.size() == 0) continue;
        all_expr.push_back(attr.expr_nodes[0]);
      }
      break;
    }
    case SCF_CALC: {
      for (Expression *expr : node->calc.expressions) {
        all_expr.push_back(expr);
      }
    } 
    default: {
      break;
    }
  }

  for (Expression *expr : all_expr) {
    if (!expr) continue;
    delete expr;
    expr = nullptr;
  }

  Db *db = sql_event->session_event()->session()->get_current_db();
  // clean tmp table
  std::vector<std::string> all_table;
  db->all_tables(all_table);
  for (std::string &t_name : all_table) {
    Table *t = db->find_table(t_name.c_str());
    if (!t) continue;
    if (t->type() == Table::VIEW) {
      std::string physical_tmp_table = t_name + "--phyview";
      if (db->find_table(physical_tmp_table.c_str())) {
        DropTableSqlNode dnode;
        dnode.relation_name = physical_tmp_table;
        ParsedSqlNode *node = new ParsedSqlNode();
        node->flag          = SCF_DROP_TABLE;
        node->drop_table    = dnode;
        SQLStageEvent stack_sql_event(*sql_event, false);
        stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));
        assert(handle_sql(ss, &stack_sql_event, false) == RC::SUCCESS);
      }

      std::string select_sql = t->table_meta().select_->select_string;
      ParsedSqlResult parsed_sql_result;
      parse(select_sql.c_str(), &parsed_sql_result);
      delete t->table_meta().select_;
      const_cast<TableMeta &>(t->table_meta()).select_ = new SelectSqlNode(parsed_sql_result.sql_nodes()[0]->selection);
    }
  }
}


void SessionStage::handle_request(StageEvent *event)
{
  SessionEvent *sev = dynamic_cast<SessionEvent *>(event);
  if (nullptr == sev) {
    LOG_ERROR("Cannot cat event to sessionEvent");
    return;
  }

  std::string query_str = sev->query();
  if (common::is_blank(query_str.c_str())) {
    return;
  }

  Session::set_current_session(sev->session());
  sev->session()->set_current_request(sev);

  std::vector<std::string> sqls;
  common::split_string(query_str, ";", sqls);

  for (std::string &sql : sqls) {
    SQLStageEvent sql_event(sev, sql);
    
    (void)handle_sql(this, &sql_event, true);
    Communicator *communicator = sev->get_communicator();
    communicator->session()->set_sql_debug(true);

    bool need_disconnect = false;
    RC rc = communicator->write_result(sev, need_disconnect);
    LOG_INFO("write result return %s", strrc(rc));
    clean_garbage(this, &sql_event);

    if (need_disconnect) {
      Server::close_connection(communicator);
    }
    if (sev->sql_result()->return_code() != RC::SUCCESS) 
      break;
  }
  sev->session()->set_current_request(nullptr);
  Session::set_current_session(nullptr);
}


/**
 * 处理一个SQL语句经历这几个阶段。
 * 虽然看起来流程比较多，但是对于大多数SQL来说，更多的可以关注parse和executor阶段。
 * 通常只有select、delete等带有查询条件的语句才需要进入optimize。
 * 对于DDL语句，比如create table、create index等，没有对应的查询计划，可以直接搜索
 * create_table_executor、create_index_executor来看具体的执行代码。
 * select、delete等DML语句，会产生一些执行计划，如果感觉繁琐，可以跳过optimize直接看
 * execute_stage中的执行，通过explain语句看需要哪些operator，然后找对应的operator来
 * 调试或者看代码执行过程即可。
 */


RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query)
{
  RC rc = RC::SUCCESS;
  rc = ss->query_cache_stage_.handle_request(sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  rc = ss->parse_stage_.handle_request(sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  rc = ss->resolve_stage_.handle_alias(ss, sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to handle alias. rc=%s", strrc(rc));
    return rc;
  }

  rc = ss->resolve_stage_.handle_view(ss, sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to handle view. rc=%s", strrc(rc));
    return rc;
  }

  rc = ss->resolve_stage_.handle_request(ss, sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }
  
  rc = ss->optimize_stage_.handle_request(sql_event, main_query);
  if (rc != RC::UNIMPLENMENT && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }
  
  rc = ss->execute_stage_.handle_request(sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  return rc;
}
