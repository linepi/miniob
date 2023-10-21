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
#include "net/writer.h"
#include "net/silent_writer.h"

#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"

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
  //  std::string stageNameStr(stage_name_);
  //  std::map<std::string, std::string> section = g_properties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

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
    bool need_disconnect = false;
    RC rc = communicator->write_result(sev, need_disconnect);
    LOG_INFO("write result return %s", strrc(rc));
    if (need_disconnect) {
      Server::close_connection(communicator);
    }

    if (sev->sql_result()->return_code() != RC::SUCCESS) break;
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
static std::vector<Value> * values_from_sql_stdout(std::string &std_out);

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

  ParsedSqlNode *sql_node = sql_event->sql_node().get();
  Writer *thesw = new SilentWriter();
  if (sql_node->flag == SCF_DELETE || sql_node->flag == SCF_UPDATE || sql_node->flag == SCF_SELECT) {
    std::vector<ConditionSqlNode> *conditions;
    if (sql_node->flag == SCF_DELETE)
      conditions = &(sql_node->deletion.conditions);
    else if (sql_node->flag == SCF_UPDATE)
      conditions = &(sql_node->update.conditions);
    else
      conditions = &(sql_node->selection.conditions);

    for (ConditionSqlNode &condition : *conditions) {
      ParsedSqlNode node;
      node.flag = SCF_SELECT;
      if (condition.left_type == CON_SUB_SELECT) {
        SQLStageEvent stack_sql_event(*sql_event);
        SessionStage stack_ss(*ss);

        node.selection = *condition.left_select;
        stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(&node));

        handle_sql(&stack_ss, &stack_sql_event, false);

        Communicator *communicator = stack_sql_event.session_event()->get_communicator();
        Writer *writer_bak = communicator->writer();

        communicator->set_writer(thesw); 
        bool need_disconnect = false;
        rc = communicator->write_result(stack_sql_event.session_event(), need_disconnect);
        SilentWriter *sw = static_cast<SilentWriter *>(communicator->writer());
        condition.left_values = values_from_sql_stdout(sw->content());

        communicator->set_writer(writer_bak);

        condition.left_type = CON_VALUE;
        condition.left_value = (*condition.left_values)[0];
      } 
    }
  }
  delete thesw;

  rc = ss->resolve_stage_.handle_request(sql_event, main_query);
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

static std::vector<Value> * values_from_sql_stdout(std::string &std_out) {
  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);
  std::vector<Value> * ret = new std::vector<Value>;
  for (size_t i = 0; i < lines.size(); i++) {
    std::string &line = lines[i];
    if (line.find(" | ", 0) != std::string::npos) {
      delete ret;
      return nullptr;
    }
    if (i != 0) {
      Value v;
      int rc = v.from_string(line);
      if (rc == 0)
        ret->push_back(v);
    }
  }
  return ret;
}