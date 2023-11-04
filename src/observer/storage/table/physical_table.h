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

/**
 * @brief 表
 * 
 */
class PhysicalTable : public Table
{
public:
  PhysicalTable();
  ~PhysicalTable() override;

  int type() override { return Table::PHYSICAL; }

  /**
   * 创建一个表
   * @param path 元数据保存的文件(完整路径)
   * @param name 表名
   * @param base_dir 表数据存放的路径
   * @param attribute_count 字段个数
   * @param attributes 字段
   */
  RC create(int32_t table_id, 
            const char *path, 
            const char *name, 
            const char *base_dir, 
            int attribute_count, 
            const AttrInfoSqlNode attributes[]) override;

  /**
   * 打开一个表
   * @param meta_file 保存表元数据的文件完整路径
   * @param base_dir 表所在的文件夹，表记录数据文件、索引数据文件存放位置
   */
  RC open(const char *meta_file, const char *base_dir) override;

  /**
   * @brief 根据给定的字段生成一个记录/行
   * @details 通常是由用户传过来的字段，按照schema信息组装成一个record。
   * @param value_num 字段的个数
   * @param values    每个字段的值
   * @param record    生成的记录数据
   */

  RC make_record(int value_num, const Value *values, Record &record) override;

  /**
   * 删除磁盘中的表记录
   */
  RC drop() override;

  /**
   * @brief 在当前的表中插入一条记录
   * @details 在表文件和索引中插入关联数据。这里只管在表中插入数据，不关心事务相关操作。
   * @param record[in/out] 传入的数据包含具体的数据，插入成功会通过此字段返回RID
   */
  RC insert_record(Record &record) override;
  RC delete_record(const Record &record) override;
  RC visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor) override;
  RC get_record(const RID &rid, Record &record) override;
  RC update_record(Record &record) override;
  RC update_record_impl(std::vector<const FieldMeta *> &field_metas, std::vector<Value> &values, Record &record) override;

  std::vector<Index *> indexes() override;

  RC recover_insert_record(Record &record) override;

  // TODO refactor
  RC create_index(Trx *trx, const std::vector<FieldMeta> field_meta, const char *index_name, bool unique) override;
  bool ignore_index(Index *index, const Record &record) override;
  bool update_need_unique_check(Index *index, const char *olddata, const char *newdata) override;

  RC get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly) override;

  RecordFileHandler *record_handler() const
  {
    return record_handler_;
  }
  Index *find_index(const char *index_name) const override;
  Index *find_index_by_field(const char *field_name) const override;

public:
  int32_t table_id() const override { return table_meta_.table_id(); }
  const char *name() const override;
  const TableMeta &table_meta() const override;
  const char * table_dir() override;
  RC sync() override;

private:
  RC insert_entry_of_indexes(const Record &record, const RID &rid) ;
  RC delete_entry_of_indexes(const Record &record, const RID &rid, bool error_on_not_exists,  bool if_update);
  RC init_record_handler(const char *base_dir);
};
