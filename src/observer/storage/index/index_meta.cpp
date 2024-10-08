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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "json/json.h"
#include <numeric>
#include <vector>

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString UNIQUE_INDEX("unique_index");

RC IndexMeta::init(const char *name, const std::vector<FieldMeta> &field)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  std::string long_name = std::accumulate(std::begin(field), std::end(field), std::string(),
    [](const std::string &acc, const FieldMeta &f_m) {
        return acc.empty() ? f_m.name() : acc + "-" + f_m.name();
    });
  field_ = long_name;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  json_value[FIELD_FIELD_NAME] = field_;
  json_value[UNIQUE_INDEX] = unique_index;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!fields_value.isString()) {
    // LOG_ERROR("Fields of index [%s] is not a JSON array. json value=%s",
    //     name_value.asCString(),
    //     fields_value.toStyledString().c_str());
    LOG_ERROR("Fields of index is not a JSON array. json value=%s",
      fields_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  
  std::string name = fields_value.asCString();
  std::vector<FieldMeta>field = table.field_mult(fields_value.asCString());
  if (field.empty()) {
        // LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value.asCString());
        return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = index.init(name_value.asCString(), field);
  index.set_unique(&json_value[UNIQUE_INDEX]);
  return rc;
}


const char *IndexMeta::name() const
{

  return name_.c_str();
}

const char *IndexMeta::field() const
{
  return field_.c_str();
}

void IndexMeta::desc(std::ostream &os) const
{
  os << "index name=" << name_ << ", field=" << field_;
}