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
// Created by WangYunlai on 2023/06/28.
//

#include <sstream>
#include "sql/parser/value.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/time/datetime.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include "common/lang/comparator.h"
#include "sql/expr/expression.h"
#include <regex>

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "dates", "booleans", "texts", "null_type", "empty_type"};

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= EMPTY_TYPE) {
    return ATTR_TYPE_NAME[type];
  }
  return "unknown";
}
AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return UNDEFINED;
}

Value::Value(int val)
{
  set_int(val);
}

Value::Value(float val)
{
  set_float(val);
}

Value::Value(bool val)
{
  set_boolean(val);
}

Value::Value(const char *s, int len /*= 0*/)
{
  set_string(s, len);
}

Value::Value(const char *s, bool isdate)
{
  set_date(s);
}

Value::Value(std::vector<Value> *list) 
{
  list_ = list;
  attr_type_ = LIST_TYPE;
}

void Value::set_null() {
  attr_type_ = NULL_TYPE;
  length_ = 0;
}

void Value::set_empty() {
  attr_type_ = EMPTY_TYPE;
  length_ = 0;
}

void Value::set_list(std::vector<Value> *values) {
  list_ = values;
  attr_type_ = LIST_TYPE;
  length_ = 0;
}

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case CHARS: {
      set_string(data, length);
    } break;
    case INTS: {
      num_value_.int_value_ = *(int *)data;
      length_ = length;
    } break;
    case FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_ = length;
    } break;
    case BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_ = length;
    } break;
    case DATES: {
      set_date(data);
      length_ = length;
    } break;
    case TEXTS: {
      set_text(data,length);
      length_ = length;
    }break;
    case NULL_TYPE: {
    } break;
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{
  attr_type_ = INTS;
  num_value_.int_value_ = val;
  length_ = sizeof(val);
}

void Value::set_float(float val)
{
  attr_type_ = FLOATS;
  num_value_.float_value_ = val;
  length_ = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_ = BOOLEANS;
  num_value_.bool_value_ = val;
  length_ = sizeof(val);
}
void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}

void Value::set_text(const char *s, int len /*= 0*/)
{
  attr_type_ = TEXTS;
  str_value_.assign(s, len);
  length_ = str_value_.length();
}


void Value::set_date(const char *s)
{
  std::string str(s);
  common::DateTime date_time(str);
  tm tm_info = date_time.to_tm();
  std::ostringstream oss;
  oss << tm_info.tm_year + 1900 << "-";
  if ((tm_info.tm_mon + 1) <= 9)
    oss << "0";
  oss << tm_info.tm_mon + 1 << "-";
  if (tm_info.tm_mday <= 9)
    oss << "0";
  oss << tm_info.tm_mday;

  attr_type_ = DATES;
  str_value_ = oss.str();
  length_ = str_value_.length();
}

Value& Value::operator=(const Value &other) {
  if (this != &other) { // Self-assignment check
    attr_type_ = other.attr_type_;

    if (other.attr_type() == LIST_TYPE) {
      list_ = new std::vector<Value>;
      *list_ = *other.list();
    } else {
      num_value_ = other.num_value_;
      str_value_ = other.str_value_;
      length_ = other.length_;
    }
  }

  return *this;
}

bool Value::operator==(const Value &other) {
  if (this == &other) return true;
  bool res = false;
  compare_op(other, CompOp::EQUAL_TO, res);
  return res;
}

void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case INTS: {
      set_int(value.get_int());
    } break;
    case FLOATS: {
      set_float(value.get_float());
    } break;
    case CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case DATES: {
      set_date(value.get_string().c_str());
    } break;
    case BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case NULL_TYPE: {
      set_null();
    } break;
    case EMPTY_TYPE: {
      set_null();
      attr_type_ = EMPTY_TYPE;
    } break;
    case LIST_TYPE: {
      *this = value;
    } break;
    case TEXTS: {
      set_text(value.get_string().c_str());
    } break;
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case CHARS: case DATES: case TEXTS:{
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

bool is_float(const std::string& str) {
  try {
    size_t pos; // 用于保存解析后的位置
    std::stof(str, &pos); 

    // 检查是否解析了整个字符串
    if (pos == str.size()) {
        return true;
    } else {
        return false;
    }

  } catch (const std::invalid_argument& e) {
    return false; 
  } catch (const std::out_of_range& e) {
    return false; 
  }
}

bool is_date(const std::string& str) {
  std::regex datePattern(R"(\d{4}-\d{1,2}-\d{1,2})");
  return std::regex_match(str, datePattern);
}

bool is_int(const std::string &s) {
  std::istringstream iss(s);
  int n;
  iss >> n;

  return !iss.fail() && iss.eof();  // 检查是否解析成功且到达字符串末尾
}

int Value::from_string(std::string str) {
  size_t firstNonSpace = str.find_first_not_of(" \t\n\r");
  size_t lastNonSpace = str.find_last_not_of(" \t\n\r");

  if (firstNonSpace != std::string::npos && lastNonSpace != std::string::npos) {
    str = str.substr(firstNonSpace, lastNonSpace - firstNonSpace + 1);
  } else {
    return -1;
  }
  if (str.find("null", 0) != std::string::npos) {
    set_null();
    return 0;
  }
  if (is_date(str)) {
    set_date(str.c_str());
    return 0;
  }
  if (is_int(str)) {
    set_int(std::stoi(str));
    return 0;
  }
  if (is_float(str)) {
    set_float(std::stof(str));
    return 0;
  }
  set_string(str.c_str(), str.size());
  return 0;
}

static float arith_float(float a, float b, ArithType type) {
  switch (type)
  {
  case ARITH_ADD:
    return a + b;
  case ARITH_SUB:
    return a - b;
  case ARITH_MUL:
    return a * b;
  case ARITH_DIV:
    return a / b;
  default:
    return 0;
  }
}

Value Value::operator_arith(const Value &other, ArithType type) const {
  if (this->attr_type_ == NULL_TYPE || this->attr_type_ == EMPTY_TYPE) return Value(NULL_TYPE);
  if (other.attr_type_ == NULL_TYPE || this->attr_type_ == EMPTY_TYPE) return Value(NULL_TYPE);

  float this_float, other_float;

  this_float = this->get_float();
  other_float = other.get_float();

  Value result;
  if (type == ARITH_DIV && other_float == 0) 
    result.set_null();
  else {
    result.set_float(arith_float(this_float, other_float, type));
  }

  return result;
}

Value Value::operator+(const Value &other) const {
  return operator_arith(other, ARITH_ADD);
}

Value Value::operator-(const Value &other) const {
  return operator_arith(other, ARITH_SUB);
}

Value Value::operator*(const Value &other) const {
  return operator_arith(other, ARITH_MUL);
}

Value Value::operator/(const Value &other) const {
  return operator_arith(other, ARITH_DIV);
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case INTS: {
      os << num_value_.int_value_;
    } break;
    case FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case CHARS: case DATES: case TEXTS: {
      os << str_value_;
    } break;
    case NULL_TYPE: {
      os << "null";
    } break;
    case EMPTY_TYPE: {
      os << "";
    } break;
    case UNDEFINED: {
      os << "";
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

std::string Value::beauty_string() const {
  std::string out = "["; 
  if (list_) {
    for (Value &v : *list_) {
      out += v.to_string() + "(" + attr_type_to_string(v.attr_type()) + ")";
      if (&v != &list_->back()) {
        out += ", ";
      }
    }
  } else {
    out += to_string() + "(" + attr_type_to_string(attr_type()) + ")";
  }
  out += "](size " + std::to_string((list_ ? list_->size() : 1)) + ")";;
  return out;
}

RC Value::is_in(CompOp op, const Value &other, bool &result) const {
  RC rc = RC::SUCCESS;
  assert(op == CompOp::IN || op == CompOp::NOT_IN);

  if (other.attr_type() == EMPTY_TYPE) {
    if (op == CompOp::IN) result = false;
    if (op == CompOp::NOT_IN) result = true;
    return rc;
  } 

  if (other.attr_type() != LIST_TYPE) {
    result = false;
    return RC::SUCCESS;
  }

  if (op == CompOp::IN) {
    for (const Value &v : *other.list()) {
      bool tmp_res;
      rc = compare_op(v, CompOp::EQUAL_TO, tmp_res); 
      if (rc != RC::SUCCESS) 
        break;
      if (tmp_res) {
        result = true;
        break;
      }
    }
  } else {
    result = true;
    for (const Value &v : *other.list()) {
      bool tmp_res;
      rc = compare_op(v, CompOp::EQUAL_TO, tmp_res); 
      if (v.attr_type() == NULL_TYPE || tmp_res) {
        result = false;
        break;
      }
    }
  }
  return rc;
}

RC Value::compare_op(const Value &other, CompOp op, bool &result) const {
  RC rc = RC::SUCCESS;
  if (op == EXISTS || op == NOT_EXISTS) {
    result = (op == NOT_EXISTS && other.attr_type_ == EMPTY_TYPE) ||
      (op == EXISTS && other.attr_type_ != EMPTY_TYPE);
    return rc;
  }

  if (op == IN || op == NOT_IN) {
    rc = is_in(op, other, result);
    if (rc != RC::SUCCESS) {
      LOG_WARN("op `in` error %s", strrc(rc));
      return rc;
    }
    return rc;
  }

  if (list_ && list_->size() > 1 && op != IS && op != IS_NOT) {
    LOG_WARN("list size not valid: %d", list_->size());
    return RC::INVALID_ARGUMENT;
  }
  if (other.list() && other.list()->size() > 1) {
    LOG_WARN("list size not valid: %d", other.list()->size());
    return RC::INVALID_ARGUMENT;
  }

  if (this->attr_type() == LIST_TYPE) {
    return list_->at(0).compare_op(other, op, result);
  }
  if (other.attr_type() == LIST_TYPE) {
    return compare_op(other.list_->at(0), op, result);
  }

  if (op <= CompOp::GREAT_THAN && op >= CompOp::EQUAL_TO) {
    if (this->attr_type_ == NULL_TYPE || other.attr_type_ == NULL_TYPE) {
      result = false;
      return rc;
    }
    if (this->attr_type_ == EMPTY_TYPE || other.attr_type_ == EMPTY_TYPE) {
      result = false;
      return rc;
    }

    int cmp_result;
    rc = compare(other, cmp_result); 

    switch (op) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: 
      assert(0);
    }
    return rc;
  }

  if (op == CompOp::IS || op == CompOp::IS_NOT) {
    if (other.attr_type_ != NULL_TYPE) { return RC::VALUE_COMPERR; }

    if (op == CompOp::IS) 
      result = this->attr_type_ == NULL_TYPE;
    else 
      result = this->attr_type_ != NULL_TYPE;
  }

  if (op == CompOp::LIKE_OP || op == CompOp::NOT_LIKE_OP) {
    if (this->attr_type_ != CHARS || other.attr_type_ != CHARS) { return RC::VALUE_COMPERR; }
    rc = like(other, result);
    if (op == CompOp::NOT_LIKE_OP) result = !result;
    return rc;
  }
  return rc;
}

RC Value::compare(const Value &other, int &result) const
{
  RC rc = RC::SUCCESS;
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        result = common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case FLOATS: {
        result = common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case CHARS: {
        result = common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case BOOLEANS: {
        result = common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      }
      case DATES: {
        result = common::compare_date(this->str_value_.c_str(), other.str_value_.c_str());
      } break;
      case NULL_TYPE: {
        result = 0;
      } break;
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
        rc = RC::VALUE_COMPERR;
      }
    }
  } else if (this->attr_type_ != NULL_TYPE && other.attr_type_ == NULL_TYPE) {
    result = 1;
  } else if (this->attr_type_ == NULL_TYPE && other.attr_type_ != NULL_TYPE) {
    result = -1;
  } else if (this->attr_type_ == DATES || other.attr_type_ == DATES) {
    return RC::VALUE_COMPERR;
  } else { 
    // all to floats
    float this_float, other_float;
    this_float = this->get_float();
    other_float = other.get_float();
    result = common::compare_float((void *)&this_float, (void *)&other_float);
  }
  return rc;
}

bool Value::like(const std::string &column, const std::string &pattern) {
  int m = column.length();
  int n = pattern.length();

  // dp[i][j] 表示 column 的前 i 个字符与 pattern 的前 j 个字符是否匹配
  std::vector<std::vector<bool>> dp(m + 1, std::vector<bool>(n + 1, false));

  // 两个空字符串是匹配的
  dp[0][0] = true;

  // 初始化第一行
  for (int j = 1; j <= n; ++j) {
      if (pattern[j-1] == '%') {
          dp[0][j] = dp[0][j-1];
      }
  }

  for (int i = 1; i <= m; ++i) {
      for (int j = 1; j <= n; ++j) {
          if (pattern[j-1] == column[i-1] || pattern[j-1] == '_') {
              dp[i][j] = dp[i-1][j-1];
          } else if (pattern[j-1] == '%') {
              dp[i][j] = dp[i-1][j] || dp[i][j-1];
          }
      }
  }

  return dp[m][n];
}

RC Value::like(const Value &other, bool &result) const {
  RC rc = RC::SUCCESS;
  if (this->attr_type_ != CHARS || other.attr_type_ != CHARS) {
    result = false;
    return rc;
  }
  result = like(this->to_string(), other.to_string());
  return rc;
}

int Value::get_int() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case INTS: {
      return num_value_.int_value_;
    }
    case FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    case DATES: {
      LOG_WARN("cannot conver date to int.");
      return 0;
    }
    case LIST_TYPE: {
      if (list_->size() != 1) {
        LOG_WARN("list size is not one");
        return false;
      }
      return list_->at(0).get_boolean();
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case INTS: {
      return float(num_value_.int_value_);
    } break;
    case FLOATS: {
      return num_value_.float_value_;
    } break;
    case BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    case DATES: {
      LOG_WARN("cannot conver date to float.");
      return 0;
    } break;
    case LIST_TYPE: {
      if (list_->size() != 1) {
        LOG_WARN("list size is not one");
        return false;
      }
      return list_->at(0).get_boolean();
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const
{
  return this->to_string();
}

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case DATES: {
      LOG_WARN("cannot conver date to boolean.");
      return false;
    } break;
    case FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    case LIST_TYPE: {
      if (list_->size() != 1) {
        LOG_WARN("list size is not one");
        return false;
      }
      return list_->at(0).get_boolean();
    } break;
    case EMPTY_TYPE: case NULL_TYPE: {
      return false;
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}
