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

#pragma once

#include "common/rc.h"
#include <memory>
#include <map>

class SQLStageEvent;
class SessionStage;
class SelectSqlNode;
class ParsedSqlNode;
/**
 * @brief 执行Resolve，将解析后的SQL语句，转换成各种Stmt(Statement), 同时会做错误检查
 * @ingroup SQLStage
 */
class ResolveStage
{
public:
  RC handle_request(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_view(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_view_select(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_view_update(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_view_delete(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_view_insert(SessionStage *ss, SQLStageEvent *sql_event, bool);
  RC handle_alias(SessionStage *ss, SQLStageEvent *sql_event, bool);
private:
  RC extract_values(std::unique_ptr<ParsedSqlNode> &node_, SessionStage *ss, SQLStageEvent *sql_event);
  RC alias_pre_process(SelectSqlNode *select_sql);
  std::map<std::string, std::string> field2alias_mp;
  std::map<std::string, int> field_exis;
  std::map<std::string, std::string> table2alias_mp; 
  std::map<std::string, int> alias_exis;          
};
