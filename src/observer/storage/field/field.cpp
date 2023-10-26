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
// Created by Wangyunlai on 2023/04/24.
//

#include "storage/field/field.h"
#include "storage/table/table.h"
#include "sql/parser/value.h"
#include "storage/record/record.h"
#include "common/log/log.h"

void Field::set_int(Record &record, int value)
{
  ASSERT(field_->type() == AttrType::INTS, "could not set int value to a non-int field");
  ASSERT(field_->len() == sizeof(value), "invalid field len");
  
  char *field_data = record.data() + field_->offset();
  memcpy(field_data, &value, sizeof(value));
}

int Field::get_int(const Record &record)
{
  Value value(field_->type(), const_cast<char *>(record.data() + field_->offset()), field_->len());
  return value.get_int();
}

const char *Field::get_data(const Record &record)
{
  return record.data() + field_->offset();
}

const Table *Field::table() const
{
  return table_;
}
const FieldMeta *Field::meta() const
{
  return field_;
}

AttrType Field::attr_type() const
{
  return field_->type();
}

int Field::attr_len() const
{
  return field_->len();
}

const char *Field::table_name() const
{
  return table_->name();
}
const char *Field::field_name() const
{
  return field_->name();
}

void Field::set_table(const Table *table)
{
  this->table_ = table;
}
void Field::set_field(const FieldMeta *field)
{
  this->field_ = field;
}