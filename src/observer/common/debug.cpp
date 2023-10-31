#include <string.h>
#include <string>
#include <algorithm>
#include <regex>
#include <unordered_set>

#include "storage/db/db.h"
#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/stmt.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "session/session_stage.h"
#include "sql/stmt/stmt.h"
#include "net/writer.h"
#include "net/communicator.h"
#include "net/silent_writer.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "storage/index/index.h"

RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);
RC sub_query_extract(SelectSqlNode *select, SessionStage *ss, SQLStageEvent *sql_event, std::string &std_out);

std::string rel_attr_to_string(RelAttrSqlNode &rel) {
  if (rel.relation_name.empty()) {
    return rel.attribute_name;
  } else {
    return rel.relation_name + "." + rel.attribute_name;
  }
}

static void add_relation(std::unordered_set<std::string> &relations, std::string &relation) {
  if (!relation.empty()) {
    relations.insert(relation);
  }
}

void get_relation_from_update(UpdateSqlNode *update, std::unordered_set<std::string> &relations) {
  relations.insert(update->relation_name);
  for (auto av : update->av) {
    av.second->get_relations(relations);
  }
  if (update->condition)
    update->condition->get_relations(relations);
}

void get_relation_from_select(SelectSqlNode *select, std::unordered_set<std::string> &relations) {
  for (SelectAttr &attr : select->attributes) {
    if (attr.expr_nodes.size() > 0)
      attr.expr_nodes[0]->get_relations(relations);
  }
  for (std::string &rela : select->relations) {
    add_relation(relations, rela);
  }
  for (JoinNode &jn : select->joins) {
    if (jn.condition)
      jn.condition->get_relations(relations);
  }
  if (select->condition)
    select->condition->get_relations(relations);
}

RC stdout_of_relation(const std::string &relation, SessionStage *ss, SQLStageEvent *sql_event, std::string &content)
{
  RC rc = RC::SUCCESS;
  SelectSqlNode *select = new SelectSqlNode();

  SelectAttr attr;
  attr.expr_nodes.push_back(new StarExpr());
  attr.expr_nodes[0]->set_name("*");

  select->attributes.push_back(attr);
  select->relations.push_back(relation);

  rc = sub_query_extract(select, ss, sql_event, content);
  if (rc != RC::SUCCESS) {
    LOG_WARN("sub_query_extract error: %s", strrc(rc));
  }
  return rc;
}

RC get_relation_index_info(Table *table, std::string &out) {
  if (!table || table->indexes().size() == 0) return RC::SUCCESS;
  out += "(index: ";
  for (Index *index : table->indexes()) {
    out += "["; 
    out += index->index_meta().field();
    out += "]";
    if (index->get_index_meta_unique()) {
      out += ":unique";
    }
    if (index != table->indexes().back()) {
      out += ", ";
    }
  }
  out += ")";
  return RC::SUCCESS;
}

std::string string_to_bytes(const std::string& str) {
  std::stringstream ss;
  for (unsigned char c : str) {
      ss << std::hex << (int)c << " ";
  }
  return ss.str();
}

void show_relations(std::unordered_set<std::string> &relations, SessionStage *ss, SQLStageEvent *sql_event) {
  for (auto &relation : relations) {
    std::string out;
    std::string content;
    RC rc = stdout_of_relation(relation, ss, sql_event, content);
    if (rc != RC::SUCCESS) {
      continue;
    }

    std::string index_info;
    rc = get_relation_index_info(
      sql_event->session_event()->session()->get_current_db()->find_table(relation.c_str()), index_info);
    if (rc != RC::SUCCESS) {
      continue;
    }
    out += "\33[1;33m[Table] \33[0m" + relation + index_info + ": [";
    out += content;
    out = std::regex_replace(out, std::regex(" \\| "), ", ");
    out = std::regex_replace(out, std::regex("\n\n"), "\n");
    out = std::regex_replace(out, std::regex("\n"), "], [");
    out = out.substr(0, out.size() - 3);
    sql_debug(out.c_str());
  }
}

void show_expressions(ParsedSqlNode *node) {
  std::vector<Expression *> exprs;
  if (node->flag == SCF_SELECT) {
    exprs.push_back(node->selection.condition);
    for (const auto &join : node->selection.joins) {
      exprs.push_back(join.condition);
    }
    for (SelectAttr &attr : node->selection.attributes) {
      if (attr.expr_nodes.size() == 0) continue;
      exprs.push_back(attr.expr_nodes[0]);
    }
  } else if (node->flag == SCF_UPDATE) {
    exprs.push_back(node->update.condition);
  } else if (node->flag == SCF_CREATE_TABLE) {
    if (node->create_table.select) {
      exprs.push_back(node->create_table.select->condition);
      for (const auto &join : node->create_table.select->joins) {
        exprs.push_back(join.condition);
      }
      for (SelectAttr &attr : node->create_table.select->attributes) {
        if (attr.expr_nodes.size() == 0) continue;
        exprs.push_back(attr.expr_nodes[0]);
      }
    }
  } else if (node->flag == SCF_CALC) {
    for (Expression *expr : node->calc.expressions)
      exprs.push_back(expr);
  }

  for (Expression *expr : exprs) {
    if (expr) {
      sql_debug("\33[1;33m[expr]\33[0m");
      sql_debug("%s", expr->dump_tree().c_str());
    }
  }
}

void test_main() {
  // std::vector<Value> values;
  // values_from_sql_stdout(output, values);
  // for (Value v : values) {
  //   sql_debug("%s", v.to_string());
  // }
}