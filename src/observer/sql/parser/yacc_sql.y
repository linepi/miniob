
%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "common/log/log.h"
#include "common/lang/string.h"
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

ArithmeticExpr *create_arithmetic_expression(ArithType type,
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
        NOT_LIKE
        INNER 
        JOIN
        IS_TOKEN
        IS_NOT_TOKEN
        NULL_TOKEN
        UNIQUE
        IN_TOKEN
        NOT_IN_TOKEN
        EXISTS_TOKEN
        NOT_EXISTS_TOKEN
        AND
        OR
        NULLABLE
        ORDER
        BY
        ASC
        AS
        ROUND
        LENGTH
        DATE_FORMAT

/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                   sql_node;
  std::vector<ParsedSqlNode *> *    sql_nodes;
  Value *                           value;
  std::vector<Value> *              value_list;
  enum SortType                     sort_type;
  enum CompOp                       comp;
  std::vector<AttrInfoSqlNode> *    attr_infos;
  AttrInfoSqlNode *                 attr_info;
  Expression *                      expression;
  std::vector<Expression *> *       expression_list;
  std::vector<std::vector<Expression *>> *expressions_list;
  RelAttrSqlNode *                  sort_attr;
  RelAttrSqlNode *                  rel_attr;
  SelectAttr *                      select_attr;
  std::vector<SelectAttr> *         select_attr_list;
  std::vector<std::string> *        relation_list;
  std::vector<JoinNode> *           join_list;
  JoinNode *                        join_node;
  std::vector<std::pair<std::string, Expression *>> * set_list;
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
%type <number>              function_type
%type <number>              join_type
%type <number>              aggregation_func
%type <number>              conjunct_type
%type <sort_condition>      sort_condition
%type <value>               value
%type <number>              number
%type <comp>                comp_op
%type <comp>                comp_op_single
%type <sort_type>           sort_type

%type <attr_infos>          attr_def_list
%type <attr_infos>          attr_def_list_for_create
%type <attr_info>           attr_def
%type <value_list>          value_list
%type <expression_list>     insert_data
%type <expressions_list>    insert_data_list
%type <sort_condition_list> order_by  
%type <sort_condition_list> sort_condition_list
%type <expression>          where

%type <sort_attr>           sort_attr
%type <rel_attr>            rel_attr
%type <select_attr>         select_attr_impl
%type <select_attr_list>    select_attr
%type <select_attr_list>    select_attr_impl_list
%type <expression_list>     select_attr_impl_piece
%type <join_list>           joins
%type <join_node>           join 

%type <relation_list>       id_list
%type <expression>          expression
%type <expression_list>     expression_list

%type <set_list>            set_list

%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            as_select_wrapper
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

%left AND OR
%left LT EQ GT LE GE NE LIKE NOT_LIKE IS_TOKEN IN_TOKEN IS_NOT_TOKEN NOT_IN_TOKEN EXISTS_TOKEN NOT_EXISTS_TOKEN
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

as_select_wrapper:
  {
    $$ = nullptr;
  }
  | AS select_stmt {
    $$ = $2;
  }
  | select_stmt {
    $$ = $1;
  }

attr_def_list_for_create:
  {
    $$ = nullptr;
  }
  | LBRACE attr_def attr_def_list RBRACE {
    if ($3 != nullptr) {
      $$ = $3;
    } else {
      $$ = new std::vector<AttrInfoSqlNode>;
    }
    $$->emplace_back(*$2);
    std::reverse($$->begin(), $$->end());
    delete $2;
  }

create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID attr_def_list_for_create as_select_wrapper
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      free($3);

      if ($4 != nullptr) {
        create_table.attr_infos.swap(*$4);
        delete $4;
      }
      if ($5 != nullptr) {
        create_table.select = new SelectSqlNode;
        *(create_table.select) = $5->selection;
        delete $5;
      }
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
        $$->insertion.values_list = new std::vector<vector<Expression *>>;
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
        $$ = new std::vector<vector<Expression *>>;
        $$->emplace_back(*$2);
      }
      delete $2;
    }
    ;

insert_data:
    LBRACE expression_list RBRACE {
      std::reverse($2->begin(), $2->end());
      $$ = $2;
    }

value:
    NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    | DATE {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp, true);
      free(tmp);
    }
    | SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      free(tmp);
    }
    | NULL_TOKEN {
      $$ = new Value(NULL_TYPE);
    }
    | LBRACE value value_list RBRACE {
      std::vector<Value> *values;
      if ($3 != nullptr) {
        values = $3;
      } else {
        values = new std::vector<Value>;
      }
      values->emplace_back(*$2);
      std::reverse(values->begin(), values->end());
      $$ = new Value(values);
      delete $2;
    }
    ;
    
value_list:
  /* empty */
  {
    $$ = nullptr;
  }
  | COMMA value value_list  { 
    if ($3 != nullptr) {
      $$ = $3;
    } else {
      $$ = new std::vector<Value>;
    }
    $$->emplace_back(*$2);
    delete $2;
  }
  ;

delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      $$->deletion.condition = $4;
      free($3);
    }
    ;

set_list:
  {
    $$ = nullptr;
  }
  | COMMA ID EQ expression set_list
  {
    if ($5 != nullptr) {
      $$ = $5;
    } else {
      $$ = new std::vector<std::pair<std::string, Expression *>>;
    }
    $$->emplace_back(std::pair<std::string, Expression *>($2, $4));
    free($2);
  }

update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ expression set_list where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2;

      if ($7 != nullptr) {
        $$->update.av.swap(*$7);
        delete $7;
      }
      $$->update.av.emplace_back(std::pair<std::string, Expression *>($4, $6));
      std::reverse($$->update.av.begin(), $$->update.av.end());

      $$->update.condition = $8;
      free($2);
      free($4);
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
    | SELECT expression_list
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
    expression conjunct_type expression {
      $$ = new ConjunctionExpr((ConjuctType)$2, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    | expression comp_op expression {
      $$ = new ComparisonExpr((CompOp)$2, $1, $3);
      $$->set_name(token_name(sql_string, &@$));
    }
    | comp_op_single expression {
      $$ = new ComparisonExpr((CompOp)$1, $2);
      $$->set_name(token_name(sql_string, &@$));
    }
    | expression '+' expression {
      $$ = create_arithmetic_expression(ARITH_ADD, $1, $3, sql_string, &@$);
    }
    | expression '-' expression {
      $$ = create_arithmetic_expression(ARITH_SUB, $1, $3, sql_string, &@$);
    }
    | expression '*' expression {
      $$ = create_arithmetic_expression(ARITH_MUL, $1, $3, sql_string, &@$);
    }
    | expression '/' expression {
      $$ = create_arithmetic_expression(ARITH_DIV, $1, $3, sql_string, &@$);
    }
    | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ARITH_NEG, $2, nullptr, sql_string, &@$);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
    | function_type LBRACE expression RBRACE {
      $$ = $3;
      $$->set_func_type((FunctionType)$1);
      $$->set_name(token_name(sql_string, &@$));
    }
    | '*' {
      $$ = new StarExpr();
      $$->set_name(token_name(sql_string, &@$));
    }
    | value {
      $$ = new ValueExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | rel_attr {
      $$ = new FieldExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | LBRACE select_stmt RBRACE {
      SelectSqlNode *select = new SelectSqlNode; 
      (*select) = $2->selection;
      $$ = new SubQueryExpr(select);
      $$->set_name(token_name(sql_string, &@$));
      delete $2;
    }
    | function_type LBRACE select_stmt RBRACE {
      SelectSqlNode *select = new SelectSqlNode; 
      (*select) = $3->selection;
      $$ = new SubQueryExpr(select);
      $$->set_name(token_name(sql_string, &@$));
      $$->set_func_type((FunctionType)$1);
      delete $3;
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

join: 
  join_type ID {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    free($2);
  }
  | join_type ID ON expression {
    $$ = new JoinNode();
    $$->type = (JoinType)$1;
    $$->relation_name = $2;
    $$->condition = $4;
    free($2);
  }

join_type:
    JOIN { $$ = (JoinType)JOIN_INNER; }
  | INNER JOIN { $$ = (JoinType)JOIN_INNER; }

conjunct_type:
  AND { $$ = CONJ_AND; }
  | OR { $$ = CONJ_OR; }

function_type:
  DATE_FORMAT { $$ = (FunctionType)FUNC_DATE_FORMAT; }
  | ROUND { $$ = (FunctionType)FUNC_ROUND; }
  | LENGTH { $$ = (FunctionType)FUNC_LENGTH; }

select_stmt:        /*  select 语句的语法解析树*/
    SELECT select_attr FROM ID id_list joins where order_by
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        std::reverse($2->begin(), $2->end());
        $$->selection.attributes.swap(*$2);
        delete $2;
      }
      if ($5 != nullptr) {
        $$->selection.relations.swap(*$5);
        delete $5;
      }
      $$->selection.relations.push_back($4);
      std::reverse($$->selection.relations.begin(), $$->selection.relations.end());

      $$->selection.condition = $7;

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
      free($4);
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
  aggregation_func LBRACE select_attr_impl_piece RBRACE {
    $$ = new SelectAttr();
    $$->agg_type = (AggType)$1;
    if ($3 != nullptr) {
      $$->expr_nodes = *$3;
      delete $3;
    }
  }
  | expression {
    $$ = new SelectAttr();
    $$->expr_nodes.emplace_back($1);
    $$->agg_type = AGG_UNDEFINED;
  }
  ;

select_attr_impl_piece:
    {
      $$ = nullptr;
    }
    | expression_list {
      $$ = $1;
    }
    ;

rel_attr:
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
  {
    $$ = nullptr;
  }
  | WHERE expression {
    $$ = $2;
  }

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | LIKE { $$ = LIKE_OP; }
    | NOT_LIKE { $$ = NOT_LIKE_OP; }
    | IS_TOKEN { $$ = IS; }
    | IS_NOT_TOKEN { $$ = IS_NOT; }
    | IN_TOKEN { $$ = IN; }
    | NOT_IN_TOKEN { $$ = NOT_IN; }
    ;

comp_op_single:
  NOT_EXISTS_TOKEN { $$ = NOT_EXISTS; }
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
      $$->set_variable.value = *$4;
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
