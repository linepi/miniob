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

static void get_relation_from_condition(std::vector<ConditionSqlNode> &conditions, std::unordered_set<std::string> &relations);
void select_to_string(SelectSqlNode *select, std::string &out);
RC handle_sql(SessionStage *ss, SQLStageEvent *sql_event, bool main_query);
void get_relation_from_select(SelectSqlNode *select, std::unordered_set<std::string> &relations);

std::string rel_attr_to_string(RelAttrSqlNode &rel) {
  if (rel.relation_name.empty()) {
    return rel.attribute_name;
  } else {
    return rel.relation_name + "." + rel.attribute_name;
  }
}

std::string values_to_string(std::vector<Value> &values) {
  std::string out = "["; 
  for (Value &v : values) {
    out += v.to_string() + "(" + attr_type_to_string(v.attr_type()) + ")";
    if (&v != &values.back()) {
      out += ", ";
    }
  }
  out += "](size " + std::to_string(values.size()) + ")";;
  return out;
}

static void add_relation(std::unordered_set<std::string> &relations, std::string &relation) {
  if (!relation.empty()) {
    relations.insert(relation);
  }
}

void get_relation_from_update(UpdateSqlNode *update, std::unordered_set<std::string> &relations) {
  relations.insert(update->relation_name);
  for (auto av : update->av) {
    if (av.second.select) {
      get_relation_from_select(av.second.select, relations);
    }
  }
  if (update->conditions.size() > 0) {
    get_relation_from_condition(update->conditions, relations);
  } 
}

void get_relation_from_select(SelectSqlNode *select, std::unordered_set<std::string> &relations) {
  for (SelectAttr &attr : select->attributes) {
    if (attr.nodes.size() > 0)
      add_relation(relations, attr.nodes[0].relation_name);
  }
  for (std::string &rela : select->relations) {
    add_relation(relations, rela);
  }
  for (JoinNode &jn : select->joins) {
    get_relation_from_condition(jn.on, relations);
  }
  if (select->conditions.size() > 0) {
    get_relation_from_condition(select->conditions, relations);
  } 
}

static void get_relation_from_condition(
	std::vector<ConditionSqlNode> &conditions, std::unordered_set<std::string> &relations) {
  for (ConditionSqlNode &con : conditions) {
    if (con.left_type == CON_ATTR) {
      add_relation(relations, con.left_attr.relation_name);
    } else {
      if (con.left_value.select) {
        get_relation_from_select(con.left_value.select, relations);
      } 
    }
    if (con.right_type == CON_ATTR) {
      add_relation(relations, con.right_attr.relation_name);
    } else {
      if (con.right_value.select) {
        get_relation_from_select(con.right_value.select, relations);
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

RC stdout_of_relation(const std::string &relation, SessionStage *ss, SQLStageEvent *sql_event, std::string &content)
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

RC values_from_sql_stdout(std::string &std_out, std::vector<Value> &values);

void test_main() {
  // std::vector<Value> values;
  // values_from_sql_stdout(output, values);
  // for (Value v : values) {
  //   sql_debug("%s", v.to_string());
  // }
}