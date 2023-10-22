#pragma once
#include <sql/parser/value.h>

struct SelectSqlNode;
struct ValueWrapper
{
	ValueWrapper(Value &v, SelectSqlNode *s) : value(v), select(s) {}
	ValueWrapper() = default;
  Value value;
  std::vector<Value> *values = nullptr;
  SelectSqlNode *select = nullptr;
};