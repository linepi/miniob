#pragma once
#include <sql/parser/value.h>

struct SelectSqlNode;
class SessionStage; 
class SQLStageEvent;

/*
  case 1, 单纯值value
  case 2, select不为空，需要extract values
  case 3, select为空，values不为空，里面是非关联子查询的结果
  case 4, select, ss, sql_event不为空，意味着关联子查询，还未得出值
*/

struct ValueWrapper
{
	ValueWrapper(Value &v, SelectSqlNode *s) : value(v), select(s) {}
	ValueWrapper() = default;
  ~ValueWrapper() {}
  Value value;
  std::vector<Value> *values = nullptr;
  SelectSqlNode *select = nullptr;
  SessionStage *ss = nullptr;
  SQLStageEvent *sql_event = nullptr;
};