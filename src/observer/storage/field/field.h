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
// Created by Wangyunlai on 2022/07/05.
//

#pragma once

#include "storage/field/field_meta.h"
#include "storage/record/record.h"

/**
 * @brief 字段
 * 
 */
class Table;
class Record;
class Field 
{
public:
  Field() = default;
  Field(const Table *table, const FieldMeta *field) : table_(table), field_(field)
  {}
  Field(const Field &) = default;

  bool operator==(const Field &other) const { 
    return this->field_ == other.field_ && this->table_ == other.table_; }

  const Table *table() const;
  const FieldMeta *meta() const;

  AttrType attr_type() const;
  int attr_len() const;

  const char *table_name() const;
  const char *field_name() const;

  void set_table(const Table *table);
  void set_field(const FieldMeta *field);

  void set_int(Record &record, int value);
  int  get_int(const Record &record);

  const char *get_data(const Record &record);

private:
  const Table *table_ = nullptr;
  const FieldMeta *field_ = nullptr;
};
