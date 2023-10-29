#pragma once

/**
 * @brief 聚合函数类型
 */
enum AggType {
  AGG_UNDEFINED,
  AGG_MIN,         
  AGG_MAX,         
  AGG_AVG,         
  AGG_SUM,         
  AGG_COUNT,       
};

#include <sql/parser/value.h>
#include <storage/field/field.h>

extern const char *AGG_TYPE_NAME[];

class Field;
class Expression;

class AggregationFunc {
public:
	AggregationFunc(AggType agg_type, bool star, Expression *expr, bool multi_table); 
	AggregationFunc(); 
	~AggregationFunc(); 

	RC aggregate(Value *value);
	Value result();

private:
	void min(Value *value) ;

	void max(Value *value) ;

	void avg(Value *value) ;

	void count(Value *value) ;

	void sum(Value *value);

public:
	AggType agg_type_ = AGG_UNDEFINED;
	bool star_ = false;
	Expression *expr_ = nullptr;
	bool multi_table_ = false;
	Value result_;
	Value sum_;
	int cnt_ = 0;
};