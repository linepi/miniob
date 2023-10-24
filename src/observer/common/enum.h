#pragma once

/**
 * @brief 描述比较运算符
 * @ingroup SQLParser
 */
enum CompOp 
{
  EQUAL_TO,     ///< "="
  LESS_EQUAL,   ///< "<="
  NOT_EQUAL,    ///< "<>"
  LESS_THAN,    ///< "<"
  GREAT_EQUAL,  ///< ">="
  GREAT_THAN,   ///< ">"
  LIKE_OP,
  NOT_LIKE_OP,
  IS,
  IS_NOT,
  NO_OP,
  IN,
  NOT_IN,
  EXISTS,
  NOT_EXISTS,
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
};


/**
 * @brief condition左右的类型
 */
enum ConType
{
  CON_ATTR,
  CON_VALUE,
  CON_SUB_SELECT,
  CON_NULL
};