#include <string.h>
#include <string>
#include <algorithm>
#include <regex>

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
#include <unordered_set>

static void condition_extract_relation(std::vector<ConditionSqlNode> &conditions, std::unordered_set<std::string> &relations);
void select_to_string(SelectSqlNode *select, std::string &out);
RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);

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

void update_extract_relation(UpdateSqlNode *update, std::unordered_set<std::string> &relations) {
  relations.insert(update->relation_name);
  if (update->conditions.size() > 0) {
    condition_extract_relation(update->conditions, relations);
  } 
}

void select_extract_relation(SelectSqlNode *select, std::unordered_set<std::string> &relations) {
  for (SelectAttr &attr : select->attributes) {
    if (attr.nodes.size() > 0)
      add_relation(relations, attr.nodes[0].relation_name);
  }
  for (std::string &rela : select->relations) {
    add_relation(relations, rela);
  }
  for (JoinNode &jn : select->joins) {
    condition_extract_relation(jn.on, relations);
  }
  if (select->conditions.size() > 0) {
    condition_extract_relation(select->conditions, relations);
  } 
}

static void condition_extract_relation(
	std::vector<ConditionSqlNode> &conditions, std::unordered_set<std::string> &relations) {
  for (ConditionSqlNode &con : conditions) {
    if (con.left_type == CON_ATTR) {
      add_relation(relations, con.left_attr.relation_name);
    } else {
      if (con.left_value.select) {
        select_extract_relation(con.left_value.select, relations);
      } 
    }
    if (con.right_type == CON_ATTR) {
      add_relation(relations, con.right_attr.relation_name);
    } else {
      if (con.right_value.select) {
        select_extract_relation(con.right_value.select, relations);
      } 
    }
  }
}

static void condition_to_string(std::vector<ConditionSqlNode> &conditions, std::string &out) {
  for (ConditionSqlNode &con : conditions) {
    if (con.left_type == CON_ATTR) {
      out += rel_attr_to_string(con.left_attr);
    } else {
      if (con.left_value.select) {
        out += "(";
        select_to_string(con.left_value.select, out);
        out += ")";
      } else {
        out += con.left_value.value.to_string();
      } 
    }
    out += " " + std::string(COMPOP_NAME[con.comp]) + " ";
    if (con.right_type == CON_ATTR) {
      out += rel_attr_to_string(con.right_attr);
    } else {
      if (con.right_value.select) {
        out += "(";
        select_to_string(con.right_value.select, out);
        out += ")";
      } else {
        out += con.right_value.value.to_string();
      } 
    }
    if (&con != &conditions.back()) {
      out += " and ";
    }
  }
}

void select_to_string(SelectSqlNode *select, std::string &out) {
  if (select) out += "select ";
  for (SelectAttr &attr : select->attributes) {
    if (attr.agg_type != AGG_UNDEFINED) {
      out += std::string(AGG_TYPE_NAME[attr.agg_type]) + "(" + rel_attr_to_string(attr.nodes[0]) + ")";
    } else {
      out += rel_attr_to_string(attr.nodes[0]);
    }
    if (&attr != &select->attributes.back())
      out += ", ";
  }
  out += " from ";
  for (std::string &rela : select->relations) {
    out += rela;
    if (&rela != &select->relations.back())
      out += ", ";
  }
  for (JoinNode &jn : select->joins) {
    out += " join " + jn.relation_name; 
    if (jn.on.size() > 0) {
      out += " on ";
      condition_to_string(jn.on, out);
    }
  }
  if (select->conditions.size() > 0) {
    out += " where ";
    condition_to_string(select->conditions, out);
  }
}

RC extract_content_from_relation(const std::string &relation, SessionStage *ss, SQLStageEvent *sql_event, std::string &content)
{
  RC rc = RC::SUCCESS;
  SelectSqlNode select;
  select.relations.push_back(relation);
  SelectAttr attr;
  attr.agg_type = AGG_UNDEFINED;
  attr.nodes.push_back(RelAttrSqlNode{"", "*"});
  select.attributes.push_back(attr);

  ParsedSqlNode *node = new ParsedSqlNode();
  node->flag          = SCF_SELECT;
  node->selection     = select;
  SQLStageEvent stack_sql_event(*sql_event, false);
  stack_sql_event.set_sql_node(std::unique_ptr<ParsedSqlNode>(node));

  rc = handle_sql(ss, &stack_sql_event, false);
  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  Writer       *thesw        = new SilentWriter();
  Communicator *communicator = stack_sql_event.session_event()->get_communicator();
  Writer       *writer_bak   = communicator->writer();
  communicator->set_writer(thesw);

  bool need_disconnect = false;
  rc                   = communicator->write_result(stack_sql_event.session_event(), need_disconnect);
  LOG_INFO("sub query write result return %s", strrc(rc));
  communicator->set_writer(writer_bak);

  if ((rc = stack_sql_event.session_event()->sql_result()->return_code()) != RC::SUCCESS) {
    LOG_WARN("sub query failed. rc=%s", strrc(rc));
    return rc;
  }

  SilentWriter *sw = static_cast<SilentWriter *>(thesw);
  content = sw->content();

  if (rc != RC::SUCCESS) {
    LOG_WARN("sub query parse values from std out failed. rc=%s", strrc(rc));
  }
  return rc;
}

void show_relations(std::unordered_set<std::string> &relations, SessionStage *ss, SQLStageEvent *sql_event) {
  for (auto &relation : relations) {
    std::string out;
    std::string content;
    RC rc = extract_content_from_relation(relation, ss, sql_event, content);
    out += relation + ": ";
    if (rc != RC::SUCCESS) {
      continue;
    }
    out += content;
    out = std::regex_replace(out, std::regex("\n"), ";");
    sql_debug(out.c_str());
  }
}