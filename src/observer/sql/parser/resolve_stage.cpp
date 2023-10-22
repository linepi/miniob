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

#include "resolve_stage.h"

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

static RC values_from_sql_stdout(std::string &std_out, std::vector<Value> &values) {
  std::vector<std::string> lines;
  common::split_string(std_out, "\n", lines);
  for (size_t i = 0; i < lines.size(); i++) {
    std::string &line = lines[i];
    if (line.find("FALIURE", 0) != std::string::npos) {
      return RC::SUB_QUERY_FAILURE;
    }
    if (line.find(" | ", 0) != std::string::npos) { // 多于一列
      return RC::SUB_QUERY_MULTI_COLUMN;
    }
    if (i != 0) {
      Value v;
      int rc = v.from_string(line);
      if (rc == 0) // 是否为空白
        values.push_back(v);
    }
  }
  return RC::SUCCESS;
}

static RC value_extract(ValueWrapper &value, SessionStage *ss, SQLStageEvent *sql_event) {
  if (value.select == nullptr) return RC::SUCCESS;

  ParsedSqlNode *node = new ParsedSqlNode();
  node->flag = SCF_SELECT;
  node->selection = *value.select;
  SQLStageEvent stack_sql_event(*sql_event, false);
  stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));

  RC rc = handle_sql(ss, &stack_sql_event, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  Writer *thesw = new SilentWriter();
  Communicator *communicator = stack_sql_event.session_event()->get_communicator();
  Writer *writer_bak = communicator->writer();
  communicator->set_writer(thesw); 

  bool need_disconnect = false;
  rc = communicator->write_result(stack_sql_event.session_event(), need_disconnect);
  LOG_INFO("sub query write result return %s", strrc(rc));
  communicator->set_writer(writer_bak);

  if ((rc = stack_sql_event.session_event()->sql_result()->return_code()) != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  SilentWriter *sw = static_cast<SilentWriter *>(thesw);
  value.values = new std::vector<Value>;
  rc = values_from_sql_stdout(sw->content(), *value.values);

  delete sw;
  delete value.select;
  value.select = nullptr;

  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query parse values from std out failed. rc=%s", strrc(rc));
  }
  return rc;
} 

RC ResolveStage::extract_values(std::unique_ptr<ParsedSqlNode> &node_, SessionStage *ss, SQLStageEvent *sql_event) {
  ParsedSqlNode *node = node_.get();
  RC rc = RC::SUCCESS;
  switch (node->flag) {
    case SCF_INSERT: {
      for (std::vector<ValueWrapper> &values : *(node->insertion.values_list)) {
        for (ValueWrapper &value : values) {
          rc = value_extract(value, ss, sql_event);
          if (rc != RC::SUCCESS) {
            LOG_WARN("insert value extract error");
            goto deal_rc;
          }
        }
      }
      break;
    }
    case SCF_UPDATE: {
      for (std::pair<std::string, ValueWrapper> &av_ : node->update.av) {
        rc = value_extract(av_.second, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.av value extract error");
          goto deal_rc;
        }
      }
      for (ConditionSqlNode &con : node->update.conditions) {
        rc = value_extract(con.left_value, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.conditions.left value extract error");
          goto deal_rc;
        }
        rc = value_extract(con.right_value, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("update.conditions.right value extract error");
          goto deal_rc;
        }
      }
      break;
    }
    case SCF_SELECT: {
      for (ConditionSqlNode &con : node->selection.conditions) {
        rc = value_extract(con.left_value, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("select.conditions.left value extract error");
          goto deal_rc;
        }
        rc = value_extract(con.right_value, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("select.conditions.right value extract error");
          goto deal_rc;
        }
      }
      for (JoinNode &jn : node->selection.joins) {
        for (ConditionSqlNode &con : jn.on) {
          rc = value_extract(con.left_value, ss, sql_event);
          if (rc != RC::SUCCESS) {
            LOG_WARN("select.join.on.left value extract error");
            goto deal_rc;
          }
          rc = value_extract(con.right_value, ss, sql_event);
          if (rc != RC::SUCCESS) {
            LOG_WARN("select.join.on.right value extract error");
            goto deal_rc;
          }
        }
      }
      break;
    }
    case SCF_DELETE: {
      for (ConditionSqlNode &con : node->deletion.conditions) {
        rc = value_extract(con.left_value, ss, sql_event);
        if (rc != RC::SUCCESS) {
          LOG_WARN("delete.condition.left value extract error");
          goto deal_rc;
        }
        if (rc != RC::SUCCESS) goto deal_rc;
        rc = value_extract(con.right_value, ss, sql_event);
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
  return rc;
}

RC ResolveStage::handle_request(SessionStage *ss, SQLStageEvent *sql_event, bool main_query)
{
  RC rc = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult *sql_result = session_event->sql_result();

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
  rc = Stmt::create_stmt(db, *(sql_event->sql_node().get()), stmt);
  if (rc != RC::SUCCESS && rc != RC::UNIMPLENMENT) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
    sql_result->set_return_code(rc);
    return rc;
  }

  sql_event->set_stmt(stmt);

  return rc;
}
