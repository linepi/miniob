/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"
#include <cassert>

RC CreateTableStmt::create(Db *db, const CreateTableSqlNode &create_table, Stmt *&stmt)
{
  if (create_table.select) {
    assert(create_table.names->size() == create_table.select_attr_infos->size());
    for (size_t i = 0; i < create_table.names->size(); i++) {
      std::string &name = create_table.names->at(i);
      AttrInfoSqlNode &attr_info = create_table.select_attr_infos->at(i);
      attr_info.name = name;
      attr_info.nullable = true;
    }
    delete create_table.names;
    delete create_table.select;
  }
  stmt = new CreateTableStmt(
    create_table.relation_name, create_table.attr_infos, 
    create_table.values_list, create_table.select_attr_infos);
  return RC::SUCCESS;
}
