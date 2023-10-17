#pragma once
#include <sql/parser/value.h>

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

extern const char *AGG_TYPE_NAME[];

class AggregationFunc {
public:
	AggregationFunc(AggType agg_type, bool star, std::string field_name); 
	AggregationFunc() {
		agg_type_ = AGG_UNDEFINED;
	} 
	~AggregationFunc() = default;


	RC aggregate(Value *value);

private:
	void min(Value *value) ;

	void max(Value *value) ;

	void avg(Value *value) ;

	void count(Value *value) ;

	void sum(Value *value);

public:
	AggType agg_type_;
	bool star_ = false;
	std::string field_name_;
	Value result_;
	Value sum_;
	int cnt_ = 0;
};