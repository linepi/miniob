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

#include "sql/executor/create_table_executor.h"

#include "session/session.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "sql/stmt/create_table_stmt.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "storage/db/db.h"
#include "common/lang/string.h"

void wildcard_fields(Table *table, std::vector<Field> &field_metas);
RC add_table(
  Db *db,
  std::vector<Table *> &tables, 
  std::unordered_map<std::string, Table *> &table_map, 
  const char *table_name,
  bool norepeat
);

static RC get_fields(std::vector<Field> &query_fields, Db *db, SelectSqlNode &select_sql, std::vector<Table *> &tables) {
  RC rc;
  // collect tables in `from` statement
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if ((rc = add_table(db, tables, table_map, table_name, false)) != RC::SUCCESS) 
      return rc;
  }

  for (const SelectAttr &select_attr : select_sql.attributes) {
    const RelAttrSqlNode &relation_attr = select_attr.nodes.front();
    const char *table_name = relation_attr.relation_name.c_str();
    const char *field_name = relation_attr.attribute_name.c_str();

    if (common::is_blank(relation_attr.relation_name.c_str()) &&
        0 == strcmp(relation_attr.attribute_name.c_str(), "*")) {
      for (Table *table : tables) {
        wildcard_fields(table, query_fields);
      }
    } else {
      Table *table = nullptr; 
      if (!common::is_blank(relation_attr.relation_name.c_str())) {
        auto iter = table_map.find(table_name);
        table = iter->second;
      } else {
        table = tables[0];
      }
      const FieldMeta *field_meta = table->table_meta().field(field_name);
      query_fields.push_back(Field(table, field_meta));
    }
  }
  return RC::SUCCESS;
}

RC CreateTableExecutor::execute(SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  Stmt *stmt = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_TABLE, 
         "create table executor can not run this command: %d", static_cast<int>(stmt->type()));

  CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);
  const char *table_name = create_table_stmt->table_name().c_str();
  if (create_table_stmt->values_list()) {
    SelectSqlNode *select = create_table_stmt->select();
    assert(select); 
    Db *db = session->get_current_db();
    assert(db);
    std::vector<Field> field_metas;
    std::vector<Table *> tables;
    get_fields(field_metas, db, *select, tables);

    std::vector<AttrInfoSqlNode> attrs;
    if (select->attributes[0].nodes.front().attribute_name == "*") {
      if (select->attributes[0].agg_type == AGG_COUNT) {
        attrs.push_back(AttrInfoSqlNode{INTS, "COUNT(*)", 4, true});
      } else {
        for (Field &f : field_metas) {
          std::string name;
          if (tables.size() == 1) 
            name = f.field_name();
          else
            name = f.table_name() + std::string(".") + f.field_name();
          attrs.push_back(AttrInfoSqlNode{f.attr_type(), name.c_str(), (uint64_t)f.attr_len(), true});
        }
      }
    } else {
      for (SelectAttr &select_attr : select->attributes) {
        RelAttrSqlNode rel_attr = select_attr.nodes[0];
        AttrInfoSqlNode new_node;
        new_node.nullable = true;
        std::string name;
        if (rel_attr.relation_name.empty())
          name = rel_attr.attribute_name;
        else
          name = rel_attr.relation_name + "." + rel_attr.attribute_name;

        if (select_attr.agg_type != AGG_UNDEFINED) {
          name = AGG_TYPE_NAME[select_attr.agg_type] + std::string("(") + name + ")";
        }
        new_node.name = name;

        const FieldMeta *field_meta = nullptr;
        rel_attr.relation_name = tables[0]->name();
        for (Field &f : field_metas) {
          if (rel_attr.attribute_name == f.field_name() && rel_attr.relation_name == f.table_name())
            field_meta = f.meta();
        }
        if (!field_meta) {
          LOG_WARN("cannot find field_meta");
          return RC::SCHEMA_FIELD_MISSING;
        }
        new_node.length = field_meta->len();
        new_node.type = field_meta->type();
        attrs.push_back(new_node);
      }
    }

    if (create_table_stmt->attr_infos().size() != 0) {
      for (const AttrInfoSqlNode &set_attr : create_table_stmt->attr_infos()) {
        for (AttrInfoSqlNode &attr : attrs) {
          if (attr.name == set_attr.name) {
            attr = set_attr;
          }
        }
      }
    }

    const int attribute_count = static_cast<int>(attrs.size());
    rc = session->get_current_db()->create_table(table_name, attribute_count, attrs.data(), create_table_stmt->values_list());
  } else {
    const int attribute_count = static_cast<int>(create_table_stmt->attr_infos().size());
    rc = session->get_current_db()->create_table(table_name, attribute_count, create_table_stmt->attr_infos().data(), nullptr);
  }

  return rc;
}