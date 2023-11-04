#pragma once

#include <string>
#include <vector>

#include "sql/stmt/stmt.h"

class CreateViewStmt : public Stmt {
public:
	StmtType type() const override { return StmtType::CREATE_VIEW; }
	std::string              name_;
	std::vector<std::string> attr_names_;
	SelectSqlNode *select;
};