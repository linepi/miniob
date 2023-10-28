
%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "event/sql_debug.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.hpp"
#include "sql/parser/lex_sql.h"
#include "sql/expr/expression.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg)
{
  std::unique_ptr<ParsedSqlNode> error_sql_node = std::make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

ArithmeticExpr *create_arithmetic_expression(ArithmeticExpr::Type type,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

%}

%define api.pure full
%define parse.error verbose
/** 启用位置标识 **/
%locations
%lex-param { yyscan_t scanner }
/** 这些定义了在yyparse函数中的参数 **/
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }

//标识tokens
%token  SEMICOLON
        CREATE
        DROP
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        OR
        SET
        ON
        LOAD
        DATA
        INFILE
        EXPLAIN
        EQ
        LT
        GT
        LE
        GE
        NE
        MIN
        MAX
        AVG
        COUNT
        SUM
        NOT
        LIKE
        INNER 
        JOIN
        IS_TOKEN
        NULL_TOKEN
        UNIQUE
        IN_TOKEN
        EXISTS_TOKEN
        NULLABLE
        ORDER
        BY
        ASC
        AS


/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                   sql_node;
  std::vector<ParsedSqlNode *> *    sql_nodes;
  ConditionSqlNode *                condition;

  ValueWrapper *                           value;
  std::vector<ValueWrapper> *              value_list;
  std::vector<std::vector<ValueWrapper>> * values_list;

  enum SortType                     sort_type;
  enum CompOp                       comp;
  std::vector<AttrInfoSqlNode> *    attr_infos;
  AttrInfoSqlNode *                 attr_info;
  Expression *                      expression;
  std::vector<Expression *> *       expression_list;
  std::vector<ConditionSqlNode> *   condition_list;
  RelAttrSqlNode *                  alias_attr;  
  std::pair<std::string, std::string> *                  alias_id;
  std::vector<std::pair<std::string, std::string>> *     alias_id_list;
  RelAttrSqlNode *                  sort_attr;
  RelAttrSqlNode *                  rel_attr;
  std::vector<RelAttrSqlNode> *     rel_attr_list;
  SelectAttr *                      select_attr;
  std::vector<SelectAttr> *         select_attr_list;
  std::vector<std::string> *        relation_list;
  std::vector<JoinNode> *           join_list;
  JoinNode *                        join_node;
  std::vector<std::pair<std::string, ValueWrapper>> * set_list;
  SortNode *                        sort_condition;
  std::vector<SortNode> *           sort_condition_list;
  char *                            string;
  int                               number;
  float                             floats;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <string> ID
%token <string> SSS
%token <string> DATE
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <number>              type_meta
%type <number>              type_note
%type <number>              join_type
%type <number>              aggregation_func
%type <sort_condition>      sort_condition
%type <condition>           condition
%type <value>               value
%type <value_list>          value_list
%type <number>              number
%type <comp>                comp_op
%type <comp>                comp_op_single
%type <sort_type>           sort_type

%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <value_list>          insert_data
%type <values_list>         insert_data_list
%type <sort_condition_list> order_by  
%type <condition_list>      where
%type <sort_condition_list> sort_condition_list
%type <condition_list>      condition_list

%type <alias_id>            alias_id
%type <alias_attr>          alias_attr
%type <sort_attr>           sort_attr
%type <rel_attr>            rel_attr
%type <select_attr>         select_attr_impl
%type <select_attr_list>    select_attr
%type <select_attr_list>    select_attr_impl_list
%type <rel_attr_list>       select_attr_impl_piece
%type <rel_attr_list>       rel_attr_list
%type <join_list>           joins
%type <join_node>           join 

%type <alias_id_list>       alias_id_list
%type <relation_list>       id_list
%type <expression>          expression
%type <expression_list>     expression_list

%type <set_list>            set_list

%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            show_table_stmt
%type <sql_node>            show_index_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            load_data_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands
%type <sql_nodes>           command_list

%left '+' '-'
%left '*' '/'
%nonassoc UMINUS
%%

commands: command_wrapper opt_semicolon command_list //commands or sqls. parser starts here.
  {
    std::unique_ptr<ParsedSqlNode> sql_node = std::unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
    if ($3 != nullptr) {
      for (auto node : *$3) {
        std::unique_ptr<ParsedSqlNode> thenode = std::unique_ptr<ParsedSqlNode>(node);
        sql_result->add_sql_node(std::move(thenode));
      }
      delete $3;
    }
  }
  ;

command_list: 
  {
    $$ = nullptr;
  }
  | command_wrapper opt_semicolon command_list {
    if ($3 == nullptr) {
      $$ = new std::vector<ParsedSqlNode *>;
      $$->emplace_back($1);
    } else {
      $3->emplace_back($1);
      $$ = $3;
    }
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_table_stmt
  | drop_table_stmt
  | show_table_stmt
  | show_index_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | load_data_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
      free($3);
    };

show_index_stmt:
    SHOW INDEX FROM ID {
      $$ = new ParsedSqlNode(SCF_SHOW_INDEX);
      $$->show_index.relation_name = $4;
      free($4);
    }
    ;

    
show_table_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
    ;


desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      $$->desc_table.relation_name = $2;
      free($2);
    }
    ;


create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE ID id_list RBRACE
    {
        $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
        CreateIndexSqlNode &create_index = $$->create_index;
        create_index.index_name = $3;
        create_index.relation_name = $5;
        std::string out;
        out += $7;
        if ($8 != nullptr) {
          std::reverse($8->begin(), $8->end());
          for (std::string &str : *$8) {
            out += "-";
            out += str;
          }
        }
        create_index.attribute_name = out;
        create_index.unique = false;
        free($3);
        free($5);
        free($7);
        free($8);
    }
    | CREATE UNIQUE INDEX ID ON ID LBRACE ID id_list RBRACE
    {
        $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
        CreateIndexSqlNode &create_index = $$->create_index;
        create_index.index_name = $4;
        create_index.relation_name = $6;
        std::string out;
        out += $8;
        if ($9 != nullptr) {
          std::reverse($9->begin(), $9->end());
          for (std::string &str : *$9) {
            out += "-";
            out += str;
          }
        }
        create_index.attribute_name = out;
        create_index.unique = true;
        free($4);
        free($6);
        free($8);
        free($9);
    }
    ;

drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
      free($3);
      free($5);
    }
    ;
create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      free($3);

      std::vector<AttrInfoSqlNode> *src_attrs = $6;

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
      }
      create_table.attr_infos.emplace_back(*$5);
      std::reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      delete $5;
    }
    ;
attr_def_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA attr_def attr_def_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<AttrInfoSqlNode>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
    
attr_def:
    ID type_meta LBRACE number RBRACE type_note
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $4;
      $$->nullable = $6;
      free($1);
    }
    | ID type_meta type_note
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      if ($$->type == DATES) $$->length = 10;
      $$->nullable = $3;
      free($1);
    }
    ;
number:
    NUMBER {$$ = $1;}
    ;

type_note:
  { $$ = 1; }
  | NULLABLE { $$ = 1; }
  | NULL_TOKEN { $$ = 1; }
  | NOT NULL_TOKEN { $$ = 0; }

type_meta:
    INT_T      { $$=INTS; }
    | STRING_T { $$=CHARS; }
    | FLOAT_T  { $$=FLOATS; }
    | DATE_T   { $$=DATES; }
    ;

aggregation_func:
    MIN { $$ = AGG_MIN; }
    | MAX { $$ = AGG_MAX; }
    | AVG { $$ = AGG_AVG; }
    | COUNT { $$ = AGG_COUNT; }
    | SUM { $$ = AGG_SUM; }

insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES insert_data insert_data_list
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      $$->insertion.relation_name = $3;
      if ($6 != nullptr) {
        $6->emplace_back(*$5);
        std::reverse($6->begin(), $6->end());
        $$->insertion.values_list = $6;
      } else {
        $$->insertion.values_list = new std::vector<vector<ValueWrapper>>;
        $$->insertion.values_list->emplace_back(*$5);
      }
      delete $5;
      free($3);
    }
    ;

insert_data_list: 
    {
      $$ = nullptr;
    }
    | COMMA insert_data insert_data_list {
      if ($3 != nullptr) {
        $3->emplace_back(*$2);
        $$ = $3;
      } else {
        $$ = new std::vector<vector<ValueWrapper>>;
        $$->emplace_back(*$2);
      }
      delete $2;
    }
    ;

insert_data:
    LBRACE value value_list RBRACE {
      if ($3 != nullptr) {
        $3->emplace_back(*$2);
        std::reverse($3->begin(), $3->end());
        $$ = $3;
      } else {
        $$ = new std::vector<ValueWrapper>;
        $$->emplace_back(*$2);
      }   
      delete $2;
    }

value_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA value value_list  { 
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<ValueWrapper>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
value:
    NUMBER {
      $$ = new ValueWrapper;
      $$->value.set_int((int)$1);
      @$ = @1;
    }
    |FLOAT {
      $$ = new ValueWrapper;
      $$->value.set_float((float)$1);
      @$ = @1;
    }
    |DATE {
      $$ = new ValueWrapper;
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$->value.set_date(tmp);
      free(tmp);
    }
    |SSS {
      $$ = new ValueWrapper;
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$->value.set_string(tmp);
      free(tmp);
    }
    | NULL_TOKEN {
      $$ = new ValueWrapper;
      $$->value.set_null();
    }
    | LBRACE select_stmt RBRACE {
      $$ = new ValueWrapper;
      $$->select = new SelectSqlNode;
      *($$->select) = $2->selection;
      delete $2;
    }
    | insert_data {
      $$ = new ValueWrapper;
      $$->values = new std::vector<Value>;
      for (ValueWrapper &vw : *$1) {
        $$->values->emplace_back(vw.value);
      }
      delete $1;
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.conditions.swap(*$4);
        delete $4;
      }
      free($3);
    }
    ;

set_list:
  {
    $$ = nullptr;
  }
  | COMMA ID EQ value set_list
  {
    if ($5 != nullptr) {
      $$ = $5;
    } else {
      $$ = new std::vector<std::pair<std::string, ValueWrapper>>;
    }
    $$->emplace_back(std::pair<std::string, ValueWrapper>($2, *$4));
    free($2);
    delete $4;
  }

update_stmt:      /*  update 语句的语法解析树*/
    UPDATE alias_id SET ID EQ value set_list where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2->first;
      $$->update.table_alias = $2->second;
      $$->update.attribute_name = $4;

      if ($7 != nullptr) {
        $$->update.av.swap(*$7);
        delete $7;
      }
      $$->update.av.emplace_back(std::pair<std::string, ValueWrapper>($4, *$6));
      std::reverse($$->update.av.begin(), $$->update.av.end());

      if ($8 != nullptr) {
        $$->update.conditions.swap(*$8);
        delete $8;
      }
      free($2);
      free($4);
      delete $6;
    }
    ;
calc_stmt:
    CALC expression_list
    {
      $$ = new ParsedSqlNode(SCF_CALC);
      std::reverse($2->begin(), $2->end());
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;

expression_list:
    expression
    {
      $$ = new std::vector<Expression*>;
      $$->emplace_back($1);
    }
    | expression COMMA expression_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<Expression *>;
      }
      $$->emplace_back($1);
    }
    ;
expression:
    expression '+' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, $3, sql_string, &@$);
    }
    | expression '-' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::SUB, $1, $3, sql_string, &@$);
    }
    | expression '*' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::MUL, $1, $3, sql_string, &@$);
    }
    | expression '/' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::DIV, $1, $3, sql_string, &@$);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
    | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, $2, nullptr, sql_string, &@$);
    }
    | value {
      $$ = new ValueExpr($1->value);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    ;

joins:
  {
    $$ = nullptr;
  }
  | join joins {
    if ($2 != nullptr) {
      $$ = $2;
    } else {
      $$ = new std::vector<JoinNode>;
    }
    $$->emplace_back(*$1);
    free($1);
  }
  ;

join:
  join_type ID {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    free($2);
  }
  | join_type ID AS ID {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    $$->table_alias = $4;
    free($2);
    free($4);
  }
  | join_type ID ID {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    $$->table_alias = $3;
    free($2);
    free($3);
  }
  | join_type ID ON condition condition_list {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    if ($5 != nullptr) {
      $$->on.swap(*$5);
      delete $5;
    }
    $$->on.emplace_back(*$4);
    delete $4;
    free($2);
  }
  ;
join_type:
    JOIN { $$ = (JoinType)JOIN_INNER; }
  | INNER JOIN { $$ = (JoinType)JOIN_INNER; }
  ;

select_stmt:        /*  select 语句的语法解析树*/
    SELECT select_attr FROM alias_id alias_id_list joins where order_by
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        std::reverse($2->begin(), $2->end());
        $$->selection.attributes.swap(*$2);
        delete $2;
      }

      if ($5) {
        for (auto p : *$5) {
          $$->selection.relations.emplace_back(p.first);
          $$->selection.table_alias.emplace_back(p.second);
        }
      }
      $$->selection.relations.emplace_back($4->first);
      $$->selection.table_alias.emplace_back($4->second);
      
      std::reverse($$->selection.relations.begin(), $$->selection.relations.end());
      std::reverse($$->selection.table_alias.begin(), $$->selection.table_alias.end());
      delete $4;
      delete $5;

      if ($7 != nullptr) {
        $$->selection.conditions.swap(*$7);
        delete $7;
      }
      if ($8 != nullptr) {
        $$->selection.sort.swap(*$8);
        std::reverse($$->selection.sort.begin(),$$->selection.sort.end());
        delete $8;
      }
      if ($6 != nullptr) {
        $$->selection.joins.swap(*$6);
        std::reverse($$->selection.joins.begin(), $$->selection.joins.end());
        delete $6;
      }
    }
    ;

order_by:
  {
    $$ = nullptr;
  }
  | ORDER BY sort_condition sort_condition_list
  {
    if ($4 == nullptr) {
      $$ = new std::vector<SortNode>;
      $$->emplace_back(*$3);
    } else {
      $$ = $4;
      $$->emplace_back(*$3);
    }
    delete $3;
  }
  ;

sort_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      free($1);
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      free($1);
      free($3);
    }
    | ID AS ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      $$->alias = $3;
      free($1);
      free($3);      
    }
    | ID ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      $$->alias = $2;
      free($1);
      free($2);      
    }
    | ID DOT ID AS ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      $$->alias = $5;
      free($1);
      free($3);
      free($5);
    }
    | ID DOT ID ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      $$->alias = $4;
      free($1);
      free($3);
      free($4);
    }
    ;

sort_condition:
  sort_attr sort_type {
    $$ = new SortNode;
    $$->field = *$1;
    $$->order = $2;
    delete $1;
  }
  | sort_attr {
    $$ = new SortNode;
    $$->field = *$1;
    delete $1;
  }
  ;

sort_condition_list:
  {
    $$ = nullptr;
  }
  | COMMA sort_condition sort_condition_list {
    if($3 == nullptr){
      $$ = new std::vector<SortNode>;
      $$->emplace_back(*$2);
    } else {
      $$ = $3;
      $$->emplace_back(*$2);
    }
    delete $2;
  }


select_attr:
  select_attr_impl select_attr_impl_list {
    if ($2 == nullptr) {
      $$ = new std::vector<SelectAttr>;
      $$->emplace_back(*$1);
    } else {
      $$ = $2;
      $$->emplace_back(*$1);
    }
    delete $1;
  }

select_attr_impl_list: 
  {
    $$ = nullptr;
  }
  | COMMA select_attr_impl select_attr_impl_list {
    if ($3 == nullptr) {
      $$ = new std::vector<SelectAttr>;
    } else {
      $$ = $3;
    }
    $$->emplace_back(*$2);
    delete $2;
  }

select_attr_impl:
  rel_attr {
    $$ = new SelectAttr();
    $$->nodes.emplace_back(*$1);
    $$->agg_type = AGG_UNDEFINED;
    delete $1;
  }
  | aggregation_func LBRACE RBRACE {
    $$ = new SelectAttr();
    $$->agg_type = (AggType)$1;
  }
  | aggregation_func LBRACE select_attr_impl_piece RBRACE {
    $$ = new SelectAttr();
    $$->nodes = *$3;
    $$->agg_type = (AggType)$1;
    delete $3;
  }
  | alias_attr {
    $$ = new SelectAttr();
    $$->nodes.emplace_back(*$1);
    $$->agg_type = AGG_UNDEFINED;
    delete $1;
  }
  ;

alias_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      free($1);
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      free($1);
      free($3);
    }
    | ID AS ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      $$->alias = $3;
      free($1);
      free($3);      
    }
    | ID DOT ID AS ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      $$->alias = $5;
      free($1);
      free($3);
      free($5);
    }
    | ID DOT ID ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      $$->alias = $4;
      free($1);
      free($3);
      free($4);
    }
    ;

select_attr_impl_piece:
    rel_attr rel_attr_list {
      if ($2 != nullptr) {
        $$ = $2;
      } else {
        $$ = new std::vector<RelAttrSqlNode>;
      }
      $$->emplace_back(*$1);
      delete $1;
    }
    ;

rel_attr:
    '*' {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = "";
      $$->attribute_name = "*";
    }
    | ID DOT '*' {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = "*";
    }
    | ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      free($1);
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      free($1);
      free($3);
    }
    ;

rel_attr_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA rel_attr rel_attr_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<RelAttrSqlNode>;
      }

      $$->emplace_back(*$2);
      delete $2;
    }
    ;

alias_id:
    ID {
      $$ = new std::pair<std::string, std::string>;
      $$->first = $1;
      free($1);
    }
    | ID AS ID {
      $$ = new std::pair<std::string, std::string>;
      $$->first = $1;
      $$->second = $3;
      free($1);
      free($3);      
    }
    | ID ID {
      $$ = new std::pair<std::string, std::string>;
      $$->first = $1;
      $$->second = $2;
      free($1);
      free($2);      
    }
    ;

alias_id_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA alias_id alias_id_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<std::pair<std::string, std::string>>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;  

id_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA ID id_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<std::string>;
      }
      $$->push_back($2);
      free($2);
    }
    ;

where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE condition condition_list {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }
      $$->emplace_back(*$2);  
      delete $2;
    }
    ;

condition_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | condition_list AND condition {
      if ($1 != nullptr) {
        $$ = $1;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }
      $3->right_op = CONJ_AND;
      $$->emplace_back(*$3);
      delete $3;
    }
    | condition_list OR condition {
      if ($1 != nullptr) {
        $$ = $1;
      } else {
        $$ = new std::vector<ConditionSqlNode>;
      }
      $3->right_op = CONJ_OR;
      $$->emplace_back(*$3);
      delete $3;
    }
    ;

condition:
    rel_attr comp_op value
    {
      $$ = new ConditionSqlNode;
      $$->left_type = CON_ATTR;
      $$->left_attr = *$1;
      $$->right_type = CON_VALUE;
      $$->right_value = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | value comp_op value 
    {
      $$ = new ConditionSqlNode;
      $$->left_type = CON_VALUE;
      $$->left_value = *$1;
      $$->right_type = CON_VALUE;
      $$->right_value = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | rel_attr comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->left_type = CON_ATTR;
      $$->left_attr = *$1;
      $$->right_type = CON_ATTR;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | value comp_op rel_attr
    {
      $$ = new ConditionSqlNode;
      $$->left_type = CON_VALUE;
      $$->left_value = *$1;
      $$->right_type = CON_ATTR;
      $$->right_attr = *$3;
      $$->comp = $2;

      delete $1;
      delete $3;
    }
    | comp_op_single LBRACE select_stmt RBRACE
    {
      $$ = new ConditionSqlNode;
      $$->left_type = CON_UNDEFINED;
      $$->right_type = CON_VALUE;
      $$->right_value.select = new SelectSqlNode;
      (*$$->right_value.select) = $3->selection; 
      $$->comp = $1;
      delete $3;
    }
    ;

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | LIKE { $$ = LIKE_OP; }
    | NOT LIKE { $$ = NOT_LIKE_OP; }
    | IS_TOKEN { $$ = IS; }
    | IS_TOKEN NOT { $$ = IS_NOT; }
    | IN_TOKEN { $$ = IN; }
    | NOT IN_TOKEN { $$ = NOT_IN; }
    ;

comp_op_single:
  NOT EXISTS_TOKEN { $$ = NOT_EXISTS; }
  | EXISTS_TOKEN { $$ = EXISTS; }
  ;

sort_type:
      ASC { $$ = ASCEND; }
    | DESC { $$ = DECLINE; }
    ;

load_data_stmt:
    LOAD DATA INFILE SSS INTO TABLE ID 
    {
      char *tmp_file_name = common::substr($4, 1, strlen($4) - 2);
      
      $$ = new ParsedSqlNode(SCF_LOAD_DATA);
      $$->load_data.relation_name = $7;
      $$->load_data.file_name = tmp_file_name;
      free($7);
      free(tmp_file_name);
    }
    ;

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      $$->explain.sql_node = std::unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      $$->set_variable.name  = $2;
      $$->set_variable.value = $4->value;
      free($2);
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  yylex_init(&scanner);
  scan_string(s, scanner);
  int result = yyparse(s, sql_result, scanner);
  yylex_destroy(scanner);
  return result;
}
