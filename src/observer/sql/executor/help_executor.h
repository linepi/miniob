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
// Created by Wangyunlai on 2023/6/14.
//

#pragma once

#include "common/rc.h"
#include "common/color.h"
#include "sql/operator/string_list_physical_operator.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/executor/sql_result.h"
#include "session/session.h"

/**
 * @brief Help语句执行器
 * @ingroup Executor
 */
class HelpExecutor
{
public:
  HelpExecutor() = default;
  virtual ~HelpExecutor() = default;

  RC execute(SQLStageEvent *sql_event)
  {
    const char *strings[] = {
        ANSI_FMT("  show    ",   ANSI_FG_GREEN) " tables;",
        ANSI_FMT("  desc    ",   ANSI_FG_GREEN) " `table name`;",
        ANSI_FMT("  create  ", ANSI_FG_GREEN) " table `table name` (`column name` `column type`, ...);",
        ANSI_FMT("          ", ANSI_FG_GREEN) " index `index name` on `table` (`column`);",
        ANSI_FMT("  drop    ",   ANSI_FG_GREEN) " table `table name`;",
        ANSI_FMT("  insert  ", ANSI_FG_GREEN) " into `table` values(`value1`,`value2`);",
        ANSI_FMT("  update  ", ANSI_FG_GREEN) " `table` set column=value [where `column`=`value`];",
        ANSI_FMT("  delete  ", ANSI_FG_GREEN) " from `table` [where `column`=`value`];",
        ANSI_FMT("  select  ", ANSI_FG_GREEN) " [ * | `columns` ] from `table`;",
        "", 
      };

    auto oper = new StringListPhysicalOperator();
    for (size_t i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
      oper->append(strings[i]);
    }

    SqlResult *sql_result = sql_event->session_event()->sql_result();

    TupleSchema schema;
    schema.append_cell("\nCommands:");

    sql_result->set_tuple_schema(schema);
    sql_result->set_operator(std::unique_ptr<PhysicalOperator>(oper));

    return RC::SUCCESS;
  }
};