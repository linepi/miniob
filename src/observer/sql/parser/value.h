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
// Created by Wangyunlai 2023/6/27
//

#pragma once

#include <string>
#include <vector>
#include <common/rc.h>
#include <common/enum.h>

const char *attr_type_to_string(AttrType type);
AttrType attr_type_from_string(const char *s);
bool is_float(const std::string& str);
bool is_date(const std::string& str);
bool is_int(const std::string &s);

/**
 * @brief 属性的值
 * 
 */
class Value 
{
public:
  Value() = default;
  ~Value() = default;

  Value(AttrType attr_type, char *data, int length = 4) : attr_type_(attr_type)
  {
    this->set_data(data, length);
  }
  Value (AttrType attr_type) : attr_type_(attr_type) {
    num_value_.int_value_ = 0;
  }

  explicit Value(int val);
  explicit Value(float val);
  explicit Value(bool val);
  explicit Value(const char *s, int len = 0);
  explicit Value(const char *s, bool isdate);
  explicit Value(std::vector<Value> *list);

  Value(const Value &other) = default;
  Value &operator=(const Value &other);
  bool operator==(const Value &other);
  Value operator+(const Value &other) const;
  Value operator-(const Value &other) const;
  Value operator*(const Value &other) const;
  Value operator/(const Value &other) const;
  Value operator_arith(const Value &other, ArithType type) const;

  void set_type(AttrType type)
  {
    this->attr_type_ = type;
  }
  void set_data(char *data, int length);
  void set_data(const char *data, int length)
  {
    this->set_data(const_cast<char *>(data), length);
  }
  void set_int(int val);
  void set_null();
  void set_empty();
  void set_float(float val);
  void set_boolean(bool val);
  void set_string(const char *s, int len = 0);
  void set_date(const char *s);
  void set_value(const Value &value);
  void set_list(std::vector<Value> *values);

  std::string to_string() const;
  std::string beauty_string() const;

  RC compare(const Value &other, int &result) const;
  RC compare_op(const Value &other, CompOp op, bool &result) const;
  RC like(const Value &other, bool &result) const;
  static bool like(const std::string &column, const std::string &pattern);
  RC is_in(CompOp op, const Value &other, bool &result) const;

  int from_string(std::string str);

  const char *data() const;
  int length() const
  {
    return length_;
  }

  AttrType attr_type() const
  {
    return attr_type_;
  }

  std::vector<Value> *list() const { return list_; }

public:
  /**
   * 获取对应的值
   * 如果当前的类型与期望获取的类型不符，就会执行转换操作
   */
  int get_int() const;
  float get_float() const;
  std::string get_string() const;
  bool get_boolean() const;

private:
  AttrType attr_type_ = UNDEFINED;
  int length_ = 0;

  union {
    int int_value_;
    float float_value_;
    bool bool_value_;
  } num_value_;
  std::string str_value_;
  std::vector<Value> *list_ = nullptr;
};