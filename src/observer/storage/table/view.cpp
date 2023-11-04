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
#include <functional>
#include "storage/table/table_meta.h"
#include "storage/table/table.h"
#include <string.h>
#include <algorithm>

#include "common/defs.h"
#include "storage/table/table.h"
#include "storage/table/view.h"
#include "storage/table/table_meta.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/record/record_manager.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/index.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/trx/trx.h"
#include "event/sql_debug.h"

View::~View() {
  
}

int32_t View::table_id() const {
	return table_meta_.table_id();
}

const char *View::name() const
{
  return table_meta_.name();
}

const TableMeta &View::table_meta() const
{
  return table_meta_;
}

const char * View::table_dir()
{
  return base_dir_.c_str();
}

RC View::sync() {
	return RC::SUCCESS;
}

RC View::create(int32_t table_id, 
					const char *path, 
					const char *name, 
					const char *base_dir, 
					int attribute_count, 
					const AttrInfoSqlNode attributes[]) 
{
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT;
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create view %s:%s", base_dir, name);

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create view file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create view file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // 创建文件
  if ((rc = table_meta_.init(table_id, name, attribute_count, attributes, select_)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // delete table file
  }

  std::fstream fs;
  fs.open(path, std::ios_base::out | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  base_dir_ = base_dir;
  LOG_INFO("Successfully create view %s:%s", base_dir, name);
  return rc;
}

RC View::open(const char *meta_file, const char *base_dir) {
	// 加载元数据文件
  std::fstream fs;
  std::string meta_file_path = std::string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file;
  fs.open(meta_file_path, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  base_dir_ = base_dir;

  return RC::SUCCESS;
}

RC View::drop() {
	RC rc = RC::SUCCESS;
  std::string view_meta_path = table_meta_file(base_dir_.c_str(), table_meta_.name());
  if (::remove(view_meta_path.c_str()) != 0) {
    LOG_ERROR("%s", strerror(errno));
    rc = RC::FILE_REMOVE;
  }
  return rc;
}

RC View::make_record(int value_num, const Value *values, Record &record) {
	return RC::SUCCESS;
}

RC View::insert_record(Record &record) {
	return RC::SUCCESS;
}

RC View::delete_record(const Record &record) {
	return RC::SUCCESS;
}

RC View::visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor) {
	return RC::SUCCESS;
}

RC View::get_record(const RID &rid, Record &record) {
	return RC::SUCCESS;
}

RC View::update_record(Record &record) {
	return RC::SUCCESS;

}

RC View::update_record_impl(std::vector<const FieldMeta *> &field_metas, std::vector<Value> &values, Record &record) {
	return RC::SUCCESS;
}

std::vector<Index *> View::indexes() {
	return std::vector<Index *>();
}

RC View::recover_insert_record(Record &record) {
	return RC::SUCCESS;
}

RC View::create_index(Trx *trx, const std::vector<FieldMeta> field_meta, const char *index_name, bool unique) {
	return RC::SUCCESS;
}

bool View::ignore_index(Index *index, const Record &record) {
	return false;
}

bool View::update_need_unique_check(Index *index, const char *olddata, const char *newdata) {
	return false;
}

RC View::get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly) {
	return RC::SUCCESS;
}

RecordFileHandler *View::record_handler() const {
	return nullptr;
}

Index *View::find_index(const char *index_name) const {
	return nullptr;
}

Index *View::find_index_by_field(const char *field_name) const {
	return nullptr;
}
