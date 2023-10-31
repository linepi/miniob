#pragma once

#include "common/rc.h"
class Value;

class ExprFunc {
public:
	enum {
		AGG,
		COMMON,
	};
	ExprFunc() = default;
	virtual ~ExprFunc() = default;
	
	virtual RC iterate(Value &value) = 0;
	virtual int type() = 0;
};