#pragma once

#include "common/rc.h"
#include "sql/operator/string_list_physical_operator.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/drop_table_stmt.h"
#include "session/session.h"

/**
 * @brief Drop语句执行器
 * @ingroup Executor
 */
class DropExecutor
{
public:
  DropExecutor() = default;
  virtual ~DropExecutor() = default;

  RC execute(SQLStageEvent *sql_event)
  {
    Stmt *stmt = sql_event->stmt();
    Session *session = sql_event->session_event()->session();
    ASSERT(stmt->type() == StmtType::DROP_TABLE, 
          "drop table executor can not run this command: %d", static_cast<int>(stmt->type()));
    DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);
    const char *table_name = drop_table_stmt->table_name().c_str();
    RC rc = session->get_current_db()->drop_table(table_name);
    if (rc == RC::SCHEMA_TABLE_NOT_EXIST) 
      rc = RC::SUCCESS;
    return rc;
  }
};