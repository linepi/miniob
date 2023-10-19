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

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "floats", "dates", "booleans", "null_type"};

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= DATES) {
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

void Value::set_null() {
  attr_type_ = NULL_TYPE;
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
    }
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case CHARS: case DATES: {
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

Value Value::operator+(const Value &other) {
  if (this->attr_type_ == NULL_TYPE) return other;
  if (other.attr_type_ == NULL_TYPE) return *this;
  Value result;
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        result.set_int(this->num_value_.int_value_ + other.num_value_.int_value_);
      } break;
      case FLOATS: {
        result.set_float(this->num_value_.float_value_ + other.num_value_.float_value_);
      } break;
      case CHARS: {
        LOG_PANIC("cannot add two chars value");
      } break;
      default: {
        LOG_PANIC("unsupported type: %d + %d", 
          attr_type_to_string(this->attr_type_), 
          attr_type_to_string(other.attr_type_));
      }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    result.set_float(this->num_value_.int_value_ + other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    result.set_float(this->num_value_.float_value_ + other.num_value_.int_value_);
  } else {
    LOG_PANIC("unsupported type: %d + %d", 
      attr_type_to_string(this->attr_type_), 
      attr_type_to_string(other.attr_type_));
  }
  return result;
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
    case CHARS: case DATES: {
      os << str_value_;
    } break;
    case NULL_TYPE: {
      os << "null";
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

RC Value::compare(const Value &other, int &result) const
{
  RC rc = RC::SUCCESS;
  if (this->attr_type_ == NULL_TYPE || other.attr_type_ == NULL_TYPE) {
    result = -1;
    return rc;
  }
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
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
        rc = RC::VALUE_COMPERR;
      }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = this->num_value_.int_value_;
    result = common::compare_float((void *)&this_data, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = other.num_value_.int_value_;
    result = common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
  } else if (this->attr_type_ == CHARS && other.attr_type_ == INTS) {
    if (this->str_value_.size() == 0 || !std::isdigit(this->str_value_[0])) {
      result = -1;
    } else {
      int this_int = std::stoi(this->str_value_);
      result = common::compare_int((void *)&this_int, (void *)&other.num_value_.int_value_);
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == CHARS) {
    if (other.str_value_.size() == 0 || !std::isdigit(other.str_value_[0])) {
      result = 1;
    } else {
      int other_int = std::stoi(other.str_value_);
      result = common::compare_int((void *)&this->num_value_.int_value_, (void *)&other_int);
    }
  } else if (this->attr_type_ == CHARS && other.attr_type_ == FLOATS) {
    if (this->str_value_.size() == 0 || !std::isdigit(this->str_value_[0])) {
      result = -1;
    } else {
      float this_float = std::stof(this->str_value_);
      result = common::compare_float((void *)&this_float, (void *)&other.num_value_.float_value_);
    }
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == CHARS) {
    if (other.str_value_.size() == 0 || !std::isdigit(other.str_value_[0])) {
      result = 1;
    } else {
      float other_float = std::stof(other.str_value_);
      result = common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_float);
    }
  } else {
    rc = RC::VALUE_COMPERR;
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
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}
