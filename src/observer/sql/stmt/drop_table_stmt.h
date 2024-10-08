#pragma once

#include <string>
#include <vector>

#include "sql/stmt/stmt.h"
#include "event/sql_debug.h"

/**
 * @brief Drop 语句
 * @ingroup Statement
 */
class DropTableStmt : public Stmt
{
public:
  DropTableStmt(const std::string &table_name) : table_name_(table_name)
  {}
  virtual ~DropTableStmt() = default;

  StmtType type() const override { return StmtType::DROP_TABLE; }
  const std::string &table_name() const { return table_name_; }

  static RC create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt)
  {
    stmt = new DropTableStmt(drop_table.relation_name);
    return RC::SUCCESS;
  }
private:
  std::string table_name_;
};