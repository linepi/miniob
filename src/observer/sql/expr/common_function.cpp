#include "sql/expr/common_function.h"
#include "sql/parser/value.h"
#include "common/log/log.h"
#include <cmath>
#include <cassert>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <ctime>
#include <regex>
#include <unordered_set>

CommonFunction::CommonFunction(FunctionType func_type) : func_type_(func_type) 
{}

CommonFunction::CommonFunction(FunctionType func_type, std::string param) 
  : func_type_(func_type), param_(param)
{}

RC CommonFunction::iterate(Value &value, bool agg_on) {
  switch (func_type_) {
    case FUNC_LENGTH:
      return func_length(value);
    case FUNC_ROUND:
      return func_round(value);
    case FUNC_DATE_FORMAT:
      return func_date_format(value);
    case FUNC_UNDEFINED:
      return RC::SUCCESS;
    default:
      assert(0);
  }
  return RC::SUCCESS;
}

RC CommonFunction::func_length(Value& value) const {
  if (value.attr_type() == CHARS) {
    value.set_int(value.get_string().length());
    return RC::SUCCESS; 
  }
  return RC::INVALID_ARGUMENT; 
}

RC CommonFunction::func_round(Value& value) const {
  if (value.attr_type() == FLOATS) {
    // 将字符串参数转换为整数
    int precision;
    try {
      precision = std::stoi(param_);
    } catch (const std::invalid_argument& e) {
      return RC::INVALID_ARGUMENT;
    }

    // 检查精度是否在合理范围内
    if (precision < 0) {
      return RC::INVALID_ARGUMENT;
    }

    // 计算乘数，例如，如果精度为2，则乘数为100
    float multiplier = std::pow(10, precision);

    // 对值进行四舍五入
    float result = std::round(value.get_float() * multiplier) / multiplier;
    value.set_float(result);
    return RC::SUCCESS;
  }
  return RC::INVALID_ARGUMENT;
}

/*
%D	Day of the month as a numeric value, followed by suffix (1st, 2nd, 3rd, ...)
%M  Month name in full (January to December) by %m
%W	Weekday name in full (Sunday to Saturday) by %w
*/
std::map<int, std::string> daySuffixMap = {
    {1, "1st"}, {2, "2nd"}, {3, "3rd"}, {4, "4th"}, {5, "5th"},
    {6, "6th"}, {7, "7th"}, {8, "8th"}, {9, "9th"}, {10, "10th"},
    {11, "11th"}, {12, "12th"}, {13, "13th"}, {14, "14th"}, {15, "15th"},
    {16, "16th"}, {17, "17th"}, {18, "18th"}, {19, "19th"}, {20, "20th"},
    {21, "21st"}, {22, "22nd"}, {23, "23rd"}, {24, "24th"}, {25, "25th"},
    {26, "26th"}, {27, "27th"}, {28, "28th"}, {29, "29th"}, {30, "30th"},
    {31, "31st"}
};

std::map<int, std::string> monthNameMap = {
    {1, "January"}, {2, "February"}, {3, "March"}, {4, "April"},
    {5, "May"}, {6, "June"}, {7, "July"}, {8, "August"},
    {9, "September"}, {10, "October"}, {11, "November"}, {12, "December"}
};

std::map<int, std::string> weekDayNameMap = {
    {0, "Sunday"}, {1, "Monday"}, {2, "Tuesday"}, {3, "Wednesday"},
    {4, "Thursday"}, {5, "Friday"}, {6, "Saturday"}
};

std::string replaceInvalidFormats(const std::string& input) {
  // 定义一个集合，包含所有有效的格式字符串
  std::unordered_set<std::string> validFormats = {
      "%a", "%b", "%c", "%D", "%d", "%e", "%f", "%H", "%h", "%I", "%i", "%j",
      "%k", "%l", "%M", "%m", "%p", "%r", "%S", "%s", "%T", "%U", "%u", "%V",
      "%v", "%W", "%w", "%X", "%x", "%Y", "%y"
  };

  std::string result = input;
  std::regex formatPattern("%.");  // 正则表达式，匹配 % 后跟任意字符

  // 使用正则迭代器查找所有匹配项
  for (std::sregex_iterator it = std::sregex_iterator(input.begin(), input.end(), formatPattern);
        it != std::sregex_iterator(); ++it) {
      std::smatch match = *it;
      std::string format = match.str();  // 提取匹配的子串

      // 检查是否是有效的格式字符串
      if (validFormats.find(format) == validFormats.end()) {
        result = std::regex_replace(result, std::regex(format), format.substr(1,1));
      }
  }

  return result;
}


RC CommonFunction::func_date_format(Value& value) const {
  if (value.attr_type() == DATES) {
    std::string date = value.to_string();
    std::string format = param_;

    format = replaceInvalidFormats(format);

    if (format.find("%D", 0) != std::string::npos) {
      format = std::regex_replace(format, std::regex("%D"), "$D");
    }
    if (format.find("%M", 0) != std::string::npos) {
      format = std::regex_replace(format, std::regex("%M"), "$M");
    }
    if (format.find("%W", 0) != std::string::npos) {
      format = std::regex_replace(format, std::regex("%W"), "$W");
    }

    // 创建一个 tm 结构体来存储解析出的日期信息
    std::tm tm = {};

    // 使用字符串流和 get_time 来解析输入的日期字符串
    std::istringstream date_ss(date);
    date_ss >> std::get_time(&tm, "%Y-%m-%d");

    // 检查日期是否解析成功
    if (date_ss.fail()) {
      LOG_WARN("date format error");
      return RC::INVALID_ARGUMENT;
    }

    int day, month, weekday; // 0周天, 1 2 3 4 5 6
    std::ostringstream tmp;
    tmp << std::put_time(&tm, "%e-%m-%w");
    sscanf(tmp.str().c_str(), "%d-%d-%d", &day, &month, &weekday);

    std::ostringstream formatted_date_ss;
    formatted_date_ss << std::put_time(&tm, format.c_str());
    std::string result = formatted_date_ss.str();

    if (result.find("$D", 0) != std::string::npos) {
      result = std::regex_replace(result, std::regex("\\$D"), daySuffixMap[day]);
    }
    if (result.find("$M", 0) != std::string::npos) {
      result = std::regex_replace(result, std::regex("\\$M"), monthNameMap[month]);
    }
    if (result.find("$W", 0) != std::string::npos) {
      result = std::regex_replace(result, std::regex("\\$W"), weekDayNameMap[weekday]);
    }
    
    if (result[0] == '\'' || result[0] == '\"') {
      result = result.substr(1, result.size() - 2);
    }
    value.set_string(result.c_str());
    return RC::SUCCESS;
  }
  return RC::INVALID_ARGUMENT;
}