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

SQLStageEvent::SQLStageEvent(SQLStageEvent &other, bool main_query) {
  session_event_ = new SessionEvent(other.session_event()->get_communicator());
  main_query_ = main_query;
  correlated_query_ = other.correlated_query_;
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
}
