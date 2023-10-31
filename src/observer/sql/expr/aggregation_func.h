#pragma once

#include <sql/parser/value.h>
#include <sql/expr/exprfunc.h>

class AggregationFunc : public ExprFunc {
public:
	AggregationFunc(AggType agg_type); 
	~AggregationFunc() = default; 

	RC iterate(Value &value) override;
	RC aggregate(Value *value);
	Value result();
  int type() override { return ExprFunc::AGG; }

private:
	void min(Value *value) ;

	void max(Value *value) ;

	void avg(Value *value) ;

	void count(Value *value) ;

	void sum(Value *value);

public:
	AggType agg_type_ = AGG_UNDEFINED;
	Value result_;
	Value sum_;
	int cnt_ = 0;
};