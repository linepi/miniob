#pragma once

#include <string>
#include <vector>

#include "sql/stmt/stmt.h"

class CreateViewStmt : public Stmt {
public:
	StmtType type() const override { return StmtType::CREATE_VIEW; }
	std::string                  view_name_;
	std::vector<AttrInfoSqlNode> attrs_;
	SelectSqlNode 							*select;
};