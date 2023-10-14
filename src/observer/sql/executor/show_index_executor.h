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
    SqlResult *sql_result = sql_event->session_event()->sql_result();

    ASSERT(stmt->type() == StmtType::SHOW_INDEX, 
          "show table index executor can not run this command: %d", static_cast<int>(stmt->type()));
    ShowIndexStmt *index_table_stmt = static_cast<ShowIndexStmt *>(stmt);
    const char *table_name = index_table_stmt->table_name().c_str();


    TableMeta table_meta;
    std::vector<std::string>index_name;
    std::vector<std::string>index_column;
    std::vector<std::string>index_id;
    RC rc = session->get_current_db()->show_index(table_name,table_meta);

    const int index_num = table_meta.index_num();
    for (int i = 0; i < index_num; i++) {
      const IndexMeta *index_meta = table_meta.index(i);
      index_name.push_back(index_meta->name());
      index_column.push_back(index_meta->field());
      index_id.push_back(std::to_string(i+1));
    }

    auto oper = new StringListPhysicalOperator;

    TupleSchema schema;
    schema.append_cell("Table | Non_unique | Key_name | Seq_in_index | Column_name");
    sql_result->set_tuple_schema(schema);

    for (int i=0; i < index_num; i++)
    {
      std::string ss = std::string(table_name) + std::string(" | 1 | ") + index_name[i] + std::string(" | ") + index_id[i] + std::string(" | ")  + index_column[i];
      oper->append(ss);
    }

    sql_result->set_operator(std::unique_ptr<PhysicalOperator>(oper));
    return rc;
  }
};