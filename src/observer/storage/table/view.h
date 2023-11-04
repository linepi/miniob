/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#pragma once

#include <functional>
#include "storage/table/table_meta.h"
#include "storage/table/table.h"

struct RID;
class Record;
class DiskBufferPool;
class RecordFileHandler;
class RecordFileScanner;
class ConditionFilter;
class DefaultConditionFilter;
class Index;
class IndexScanner;
class RecordDeleter;
class Trx;


class View : public Table 
{
public:
  View() {};
  ~View() override;

  int type() override { return Table::VIEW; }

  RC create(int32_t table_id, 
            const char *path, 
            const char *name, 
            const char *base_dir, 
            int attribute_count, 
            const AttrInfoSqlNode attributes[]) override;

  RC open(const char *meta_file, const char *base_dir) override;

  RC make_record(int value_num, const Value *values, Record &record) override;

  RC drop() override;

  RC insert_record(Record &record) override;
  RC delete_record(const Record &record) override;
  RC visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor) override;
  RC get_record(const RID &rid, Record &record) override;
  RC update_record(Record &record) override;
  RC update_record_impl(std::vector<const FieldMeta *> &field_metas, std::vector<Value> &values, Record &record) override;

  std::vector<Index *> indexes() override;

  RC recover_insert_record(Record &record) override;

  RC create_index(Trx *trx, const std::vector<FieldMeta> field_meta, const char *index_name, bool unique) override;
  bool ignore_index(Index *index, const Record &record) override;
  bool update_need_unique_check(Index *index, const char *olddata, const char *newdata) override;

  RC get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly) override;

  RecordFileHandler *record_handler() const override;
  Index *find_index(const char *index_name) const override;
  Index *find_index_by_field(const char *field_name) const override;

public:
  int32_t table_id() const override;
  const char *name() const override;
  const TableMeta &table_meta() const override;
  const char * table_dir() override;
  RC sync() override;
  SelectSqlNode *select_;
};
