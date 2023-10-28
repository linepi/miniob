#pragma once

extern const char *COMPOP_NAME[];
extern const char *EXPR_NAME[];
/**
 * @brief 描述比较运算符
 * @ingroup SQLParser
 */
enum CompOp 
{
  EQUAL_TO,     
  LESS_EQUAL,   
  NOT_EQUAL,    
  LESS_THAN,    
  GREAT_EQUAL,  
  GREAT_THAN,   
  LIKE_OP,
  NOT_LIKE_OP,
  IS,
  IS_NOT,
  IN,
  NOT_IN,
  EXISTS,
  NOT_EXISTS,
  NO_OP,
};

enum SortType
{
  ASCEND,
  DECLINE,
};
/**
 * @brief 属性的类型
 * 
 */
enum AttrType
{
  UNDEFINED,
  CHARS,          ///< 字符串类型
  INTS,           ///< 整数类型(4字节)
  FLOATS,         ///< 浮点数类型(4字节)
  DATES,          ///< 日期类型
  BOOLEANS,       ///< boolean类型，当前不是由parser解析出来的，是程序内部使用的
  NULL_TYPE,
  LIST_TYPE,
  EMPTY_TYPE,
};


/**
 * @brief condition左右的类型
 */
enum ConType
{
  CON_UNDEFINED,
  CON_ATTR,
  CON_VALUE,
};

enum ConjuctType
{
  CONJ_AND,
  CONJ_OR
};

/**
 * @brief 表达式类型
 * @ingroup Expression
 */
enum class ExprType 
{
  NONE,
  STAR,         ///< 星号，表示所有字段
  FIELD,        ///< 字段。在实际执行时，根据行数据内容提取对应字段的值
  VALUE,        ///< 常量值
  SUB_QUERY,    ///< 子查询
  CAST,         ///< 需要做类型转换的表达式
  COMPARISON,   ///< 需要做比较的表达式
  CONJUNCTION,  ///< 多个表达式使用同一种关系(AND或OR)来联结
  ARITHMETIC,   ///< 算术运算
};

enum FunctionType
{
  FUNC_LENGTH,
  FUNC_ROUND,
  FUNC_DATE_FORMAT,
  FUNC_UNDEFINED
};