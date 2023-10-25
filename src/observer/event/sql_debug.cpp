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
// Created by Wangyunlai on 2023/6/29.
//

#include <stdarg.h>

#include "event/sql_debug.h"
#include "session/session.h"
#include "event/session_event.h"
#include "common/lang/string.h"

using namespace std;

std::string gsdb;

void SqlDebug::add_debug_info(const std::string &debug_info)
{
  debug_infos_.push_back(debug_info);
}

void SqlDebug::clear_debug_info()
{
  debug_infos_.clear();
}

const list<string> &SqlDebug::get_debug_infos() const
{
  return debug_infos_;
}

static std::string insertNewlineAt250(const std::string& s) {
  if (s.length() <= 250) {
      return s;
  }
  return s.substr(0, 250) + "\n    " + insertNewlineAt250(s.substr(250));
}

void sql_debug_log(const char *fmt, ...)
{
  Session *session  = Session::current_session();
  if (nullptr == session) {
    return;
  }

  SessionEvent *request = session->current_request();
  if (nullptr == request) {
    return ;
  }

  SqlDebug &sql_debug = request->sql_debug();
  if (sql_debug.trace_depth == 0) return;

  const int buffer_size = 4096;
  char *str = new char[buffer_size];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(str, buffer_size, fmt, ap);
  va_end(ap);

  std::string fmt_str = insertNewlineAt250(str);  
  std::vector<std::string> lines;
  common::split_string(fmt_str, "\n", lines);

  for (std::string line : lines)
    sql_debug.add_debug_info(line.c_str());

  delete[] str;
  sql_debug.trace_depth -= 1;
}


void sql_debug(const char *fmt, ...)
{
  Session *session  = Session::current_session();
  if (nullptr == session) {
    return;
  }

  SessionEvent *request = session->current_request();
  if (nullptr == request) {
    return ;
  }

  SqlDebug &sql_debug = request->sql_debug();

  const int buffer_size = 4096;
  char *str = new char[buffer_size];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(str, buffer_size, fmt, ap);
  va_end(ap);

  std::string fmt_str = insertNewlineAt250(str);  
  std::vector<std::string> lines;
  common::split_string(fmt_str, "\n", lines);

  for (std::string line : lines)
    sql_debug.add_debug_info(line.c_str());

  delete[] str;
}
