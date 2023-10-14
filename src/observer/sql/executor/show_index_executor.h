#pragma once

#include "common/rc.h"
#include "sql/operator/string_list_physical_operator.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/show_index_stmt.h"
#include "session/session.h"
#include "storage/db/db.h"

/**
 * @brief 显示所有表的执行器
 * @ingroup Executor
 * @note 与CreateIndex类似，不处理并发
 */
class ShowIndexExecutor
{
public:
  ShowIndexExecutor() = default;
  virtual ~ShowIndexExecutor() = default;

  RC execute(SQLStageEvent *sql_event)
  {
    Stmt *stmt = sql_event->stmt();
    Session *session = sql_event->session_event()->session();
    ASSERT(stmt->type() == StmtType::SHOW_INDEX, 
          "show table index executor can not run this command: %d", static_cast<int>(stmt->type()));
    ShowIndexStmt *index_table_stmt = static_cast<ShowIndexStmt *>(stmt);
    const char *table_name = index_table_stmt->table_name().c_str();
    RC rc = session->get_current_db()->show_index(table_name);
    return rc;
  }
};