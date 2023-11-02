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
// Created by Longda on 2021/4/14.
//

#include "event/sql_event.h"

#include <cstddef>

#include "event/session_event.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"
#include "net/communicator.h"

SQLStageEvent::SQLStageEvent(SessionEvent *event, const std::string &sql) : session_event_(event), sql_(sql)
{}

std::stack<PhysicalOperator *> physical_baks;
std::stack<bool> main_query_baks;

SQLStageEvent::SQLStageEvent(SQLStageEvent &other, bool main_query) {
  if (!main_query) {
    physical_baks.push(other.session_event()->sql_result()->get_operator().get());
    other.session_event()->sql_result()->get_operator().release();
  }

  main_query_baks.push(other.session_event()->main_query_);

  session_event_ = other.session_event();
  other.session_event()->main_query_ = main_query;

  main_query_ = main_query;
  correlated_query_ = other.correlated_query_;
  session_event_->sql_result()->correlated_query_ = other.correlated_query_;
}

SQLStageEvent::~SQLStageEvent() noexcept
{
  if (session_event_ != nullptr && main_query_) {
    session_event_ = nullptr;
  }

  if (stmt_ != nullptr) {
    delete stmt_;
    stmt_ = nullptr;
  }

  if (!main_query_) {
    session_event_->sql_result()->get_operator().release();
    session_event_->sql_result()->get_operator().reset(physical_baks.top());
    physical_baks.pop();
    session_event_->main_query_ = main_query_baks.top();
    main_query_baks.pop();
  }
}
