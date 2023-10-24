#include <common/enum.h>

const char *COMPOP_NAME[] = {
  [EQUAL_TO]      = "=",
  [LESS_EQUAL]    = "<=",
  [NOT_EQUAL]     = "!=",
  [LESS_THAN]     = "<",
  [GREAT_EQUAL]   = ">=",
  [GREAT_THAN]    = ">",
  [LIKE_OP]       = "like",
  [NOT_LIKE_OP]   = "not like",
  [IS]            = "is",
  [IS_NOT]        = "is not",
  [IN]            = "in",
  [NOT_IN]        = "not in",
  [EXISTS]        = "exists",
  [NOT_EXISTS]    = "not exists",
  [NO_OP]         = "NO_OP"
};
