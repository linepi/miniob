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

#include "alias.h"
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


std::map<std::string, std::string> field2alias_mp;
std::map<std::string, int> field_exis;

std::map<std::string, std::string> table2alias_mp; ///< alias-->table_name
std::map<std::string, int> alias_exis;          /// if alias exis (1 or 0)

RC select_pre_process(SelectSqlNode *select_sql)
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
      auto field_visitor = [&alias_exist_tmp, &table2alias_map_tmp](unique_ptr<Expression> &f) {
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

    auto visitor = [&alias_exist_tmp, &table2alias_map_tmp](std::unique_ptr<Expression> &f) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(f.get());
      RelAttrSqlNode &node = field_expr->rel_attr();
      if (field_exis[node.attribute_name]){
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
      rc = select_pre_process(sub_query->select());
      if (rc != RC::SUCCESS) {
        LOG_WARN("check_correlated_query failed");
        return rc;
      }
    }
  }
  return RC::SUCCESS;
}

RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);

static void clean_garbage(ParsedSqlNode *node) {
  if (!node) return;
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
    if (need_disconnect) {
      Server::close_connection(communicator);
    }
    clean_garbage(sql_event.sql_node().get());

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

  //判断sql_event是否为selectsqlnode，若是的话，调用RC select_pre_process(SelectSqlNode *select_sql)对其select_sql进行预处理
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

    rc = select_pre_process(select_sql);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to do select pre-process. rc=%s", strrc(rc));
      sql_result->set_return_code(rc);
      return rc;
    }
  }

  rc = ss->resolve_stage_.handle_request(ss, sql_event, main_query);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
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
