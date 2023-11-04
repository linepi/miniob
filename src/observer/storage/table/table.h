/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MEvirtual RCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#pragma once

#include <functional>
#include "storage/table/table_meta.h"

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

/**
 * @brief 表
 * 
 */
class Table 
{
public:
  enum {
    VIEW,
    PHYSICAL
  };

public:
  Table() = default;
  virtual ~Table() = default;

  virtual int type() = 0;

  virtual RC create(int32_t table_id, 
            const char *path, 
            const char *name, 
            const char *base_dir, 
            int attribute_count, 
            const AttrInfoSqlNode attributes[]) = 0;

  virtual RC open(const char *meta_file, const char *base_dir) = 0;

  virtual RC make_record(int value_num, const Value *values, Record &record) = 0;

  virtual RC drop() = 0;

  virtual RC insert_record(Record &record) = 0;
  virtual RC delete_record(const Record &record) = 0;
  virtual RC visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor) = 0;
  virtual RC get_record(const RID &rid, Record &record) = 0;
  virtual RC update_record(Record &record) = 0;
  virtual RC update_record_impl(std::vector<const FieldMeta *> &field_metas, std::vector<Value> &values, Record &record) = 0;

  virtual std::vector<Index *> indexes() = 0;

  virtual RC recover_insert_record(Record &record) = 0;

  virtual RC create_index(Trx *trx, const std::vector<FieldMeta> field_meta, const char *index_name, bool unique) = 0;
  virtual bool ignore_index(Index *index, const Record &record) = 0;
  virtual bool update_need_unique_check(Index *index, const char *olddata, const char *newdata) = 0;

  virtual RC get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly) = 0;

  virtual RecordFileHandler *record_handler() const = 0;
  virtual Index *find_index(const char *index_name) const = 0;
  virtual Index *find_index_by_field(const char *field_name) const = 0;

  virtual RC sync() = 0;
  virtual int32_t table_id() const = 0;
  virtual const char *name() const = 0;
  virtual const TableMeta &table_meta() const = 0;
  virtual const char * table_dir() = 0;

protected:
  std::string base_dir_;
  TableMeta   table_meta_;
  DiskBufferPool *data_buffer_pool_ = nullptr;   
  RecordFileHandler *record_handler_ = nullptr;  
  std::vector<Index *> indexes_;
};
