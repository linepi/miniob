%top{
/*
这里的代码会被复制到lex_sql.cpp的最开始位置
定义yy_size_t的原因是因为flex生成的代码，会使用yy_size_t与其他类型的数字
做比较，导致编译报警
*/
#define YY_TYPEDEF_YY_SIZE_T
typedef int yy_size_t;

/* 参考生成的lex_sql.cpp代码，这个宏定义会放在每次运行yylex()最开始的地方 */
#define YY_USER_INIT                                         \
  yycolumn = 0;

/* 参考生成的lex_sql.cpp代码，这个宏定义会放在解析一个token之后，也可以在网上找到大量的参考资料 */
/* 我们在这里设置当前解析的token的位置信息，这样在yacc中就可以使用这些信息了 */
#define YY_USER_ACTION                                       \
do {                                                         \
  yylloc->first_line   = yylloc->last_line = yylineno;       \
  yylloc->first_column = yycolumn;                           \
  yylloc->last_column  = yylloc->first_column + yyleng - 1;  \
  yycolumn += yyleng;                                        \
}                                                            \
while (0);

}

%{
#include<string.h>
#include<stdio.h>

/**
 * flex 代码包含三个部分，使用 %% 分隔
 * 第一个部分的代码是C代码，flex会原样复制到目标文件中
 * 第二个部分是规则部分，使用正则表达式定义一系列规则
 * 第三个部分还是C代码，flex 会复制此段代码
 */

#include "sql/parser/parse_defs.h"
#include "common/log/log.h"
#include "yacc_sql.hpp"

#ifndef register
#define register 
#endif // register

extern int atoi();
extern double atof();

#define RETURN_TOKEN(token) LOG_DEBUG("%s", #token);return token
%}

/* Prevent the need for linking with -lfl */
%option noyywrap
%option nounput
%option noinput
%option bison-bridge
%option reentrant
/* 不区分大小写 */
%option case-insensitive
%option bison-locations

WHITE_SAPCE      [\ \t\b\f]
DIGIT            [0-9]+

ID               [A-Za-z_]+[A-Za-z0-9_]*
DOT              \.
QUOTE            [\'\"]

YEAR_RANGE       [1-9][0-9]{1,3}
PING_2           (0?2-(0?[1-9]|1[0-9]|2[0-8]))
BIG_MONTH        ((0?[13578]|1[02])-(0?[1-9]|[12][0-9]|3[01]))
SMALL_MONTH      ((0?[469]|11)-(0?[1-9]|[12][0-9]|30))
RUN_YEAR         (?:(?:[13579][26]|[2468][048]|[48])00|[1-9][0-9]([13579][26]|[2468][048]|0[48])|[1-9]([13579][26]|[2468][048]|0[48])|([13579][26]|[2468][048])|[48])
DATE             {QUOTE}(({YEAR_RANGE}-({PING_2}|{BIG_MONTH}|{SMALL_MONTH}))|({RUN_YEAR}-0?2-29)){QUOTE}

%x STR

/* 规则匹配的优先级：*/
/* 1. 匹配的规则长的优先 */
/* 2. 写在最前面的优先 */

/* yylval 就可以认为是 yacc 中 %union 定义的结构体(union 结构) */
%%

{WHITE_SAPCE}                                        // ignore whitespace
\n                                                   ;

{DIGIT}+                           yylval->number=atoi(yytext); RETURN_TOKEN(NUMBER);
{DIGIT}+{DOT}{DIGIT}+              yylval->floats=(float)(atof(yytext)); RETURN_TOKEN(FLOAT);

{DOT}                                   RETURN_TOKEN(DOT);
EXIT                                    RETURN_TOKEN(EXIT);
HELP                                    RETURN_TOKEN(HELP);
DESC                                    RETURN_TOKEN(DESC);
ASC                                     RETURN_TOKEN(ASC);
CREATE                                  RETURN_TOKEN(CREATE);
DROP                                    RETURN_TOKEN(DROP);
TABLE                                   RETURN_TOKEN(TABLE);
TABLES                                  RETURN_TOKEN(TABLES);
INDEX                                   RETURN_TOKEN(INDEX);
ON                                      RETURN_TOKEN(ON);
SHOW                                    RETURN_TOKEN(SHOW);
SYNC                                    RETURN_TOKEN(SYNC);
SELECT                                  RETURN_TOKEN(SELECT);
CALC                                    RETURN_TOKEN(CALC);
FROM                                    RETURN_TOKEN(FROM);
WHERE                                   RETURN_TOKEN(WHERE);
AND                                     RETURN_TOKEN(AND);
OR                                     RETURN_TOKEN(OR);
INSERT                                  RETURN_TOKEN(INSERT);
INTO                                    RETURN_TOKEN(INTO);
VALUES                                  RETURN_TOKEN(VALUES);
DELETE                                  RETURN_TOKEN(DELETE);
UPDATE                                  RETURN_TOKEN(UPDATE);
SET                                     RETURN_TOKEN(SET);
BEGIN                                   RETURN_TOKEN(TRX_BEGIN);
COMMIT                                  RETURN_TOKEN(TRX_COMMIT);
ROLLBACK                                RETURN_TOKEN(TRX_ROLLBACK);
INT                                     RETURN_TOKEN(INT_T);
CHAR                                    RETURN_TOKEN(STRING_T);
FLOAT                                   RETURN_TOKEN(FLOAT_T);
DATE                                    RETURN_TOKEN(DATE_T);
TEXT                                    RETURN_TOKEN(TEXT_T);
LOAD                                    RETURN_TOKEN(LOAD);
DATA                                    RETURN_TOKEN(DATA);
INFILE                                  RETURN_TOKEN(INFILE);
VIEW                                  RETURN_TOKEN(VIEW);
EXPLAIN                                 RETURN_TOKEN(EXPLAIN);
MAX                                     RETURN_TOKEN(MAX);
MIN                                     RETURN_TOKEN(MIN);
COUNT                                   RETURN_TOKEN(COUNT);
AVG                                     RETURN_TOKEN(AVG);
SUM                                     RETURN_TOKEN(SUM);
"NOT"[ ]+"LIKE"                         RETURN_TOKEN(NOT_LIKE);
"NOT"[ ]+"IN"                           RETURN_TOKEN(NOT_IN_TOKEN);
"IS"[ ]+"NOT"                           RETURN_TOKEN(IS_NOT_TOKEN);
"NOT"[ ]+"EXISTS"                       RETURN_TOKEN(NOT_EXISTS_TOKEN);
LIKE                                    RETURN_TOKEN(LIKE);
NOT                                     RETURN_TOKEN(NOT);
JOIN                                    RETURN_TOKEN(JOIN);
INNER                                   RETURN_TOKEN(INNER);
NULL                                    RETURN_TOKEN(NULL_TOKEN);
NULLABLE                                RETURN_TOKEN(NULLABLE);
IS                                      RETURN_TOKEN(IS_TOKEN);
UNIQUE                                  RETURN_TOKEN(UNIQUE);
IN                                      RETURN_TOKEN(IN_TOKEN);
EXISTS                                  RETURN_TOKEN(EXISTS_TOKEN);
HAVING                                  RETURN_TOKEN(HAVING);
ORDER                                   RETURN_TOKEN(ORDER);
GROUP                                   RETURN_TOKEN(GROUP);
BY                                      RETURN_TOKEN(BY);
AS                                      RETURN_TOKEN(AS);
LENGTH                                  RETURN_TOKEN(LENGTH);
ROUND                                   RETURN_TOKEN(ROUND);
DATE_FORMAT                             RETURN_TOKEN(DATE_FORMAT);
{ID}                                    yylval->string=strdup(yytext); RETURN_TOKEN(ID);
"("                                     RETURN_TOKEN(LBRACE);
")"                                     RETURN_TOKEN(RBRACE);

","                                     RETURN_TOKEN(COMMA);
"="                                     RETURN_TOKEN(EQ);
"<="                                    RETURN_TOKEN(LE);
"<>"                                    RETURN_TOKEN(NE);
"!="                                    RETURN_TOKEN(NE);
"<"                                     RETURN_TOKEN(LT);
">="                                    RETURN_TOKEN(GE);
">"                                     RETURN_TOKEN(GT);

"+" |
"-" |
"*" |
"/"                                     { LOG_DEBUG("%c", yytext[0]); return yytext[0]; }
{DATE}                                  yylval->string = strdup(yytext); RETURN_TOKEN(DATE);
\"[^"]*\"                               yylval->string = strdup(yytext); RETURN_TOKEN(SSS);
'[^']*\'                                yylval->string = strdup(yytext); RETURN_TOKEN(SSS);

.                                       LOG_DEBUG("Unknown character [%c]",yytext[0]); return yytext[0];
%%

void scan_string(const char *str, yyscan_t scanner) {
  yy_switch_to_buffer(yy_scan_string(str, scanner), scanner);
}
