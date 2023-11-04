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
// Created by Meiyi & Wangyunlai on 2021/5/13.
//

#include <limits.h>
#include <string.h>
#include <algorithm>

#include "common/defs.h"
#include "storage/table/table.h"
#include "storage/table/physical_table.h"
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

PhysicalTable::PhysicalTable() {}

PhysicalTable::~PhysicalTable()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (std::vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();

  LOG_INFO("PhysicalTable has been closed: %s", name());
}

RC PhysicalTable::create(int32_t table_id, 
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
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attribute_count <= 0 || nullptr == attributes) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d, attributes=%p", name, attribute_count, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // 创建文件
  if ((rc = table_meta_.init(table_id, name, attribute_count, attributes)) != RC::SUCCESS) {
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

  std::string data_file = table_data_file(base_dir, name);
  BufferPoolManager &bpm = BufferPoolManager::instance();
  rc = bpm.create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;
  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}

RC PhysicalTable::drop()
{
  RC rc = RC::SUCCESS;
  std::string table_meta_path = table_meta_file(base_dir_.c_str(), table_meta_.name());
  std::string table_data_path = table_data_file(base_dir_.c_str(), table_meta_.name()); 
  if (::remove(table_meta_path.c_str()) != 0) {
    LOG_ERROR("%s", strerror(errno));
    rc = RC::FILE_REMOVE;
  }
  if (::remove(table_data_path.c_str()) != 0) {
    LOG_ERROR("%s", strerror(errno));
    rc = RC::FILE_REMOVE;
  }

  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    std::string index_path = table_index_file(base_dir_.c_str(), name(), index_meta->name());
    if (::remove(index_path.c_str()) != 0) {
      LOG_ERROR("%s", strerror(errno));
      rc = RC::FILE_REMOVE;
    }
  }
  return rc;
}

RC PhysicalTable::open(const char *meta_file, const char *base_dir)
{
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

  // 加载数据文件
  RC rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;

  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta *index_meta = table_meta_.index(i);
    std::vector<FieldMeta> field_meta = table_meta_.field_mult(index_meta->field());
    if (field_meta.empty()) {
      LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
                name(), index_meta->name(), index_meta->field());
      // skip cleanup
      //  do all cleanup action in destructive PhysicalTable function
      return RC::INTERNAL;
    }

    BplusTreeIndex *index = new BplusTreeIndex();
    std::string index_file = table_index_file(base_dir, name(), index_meta->name());
    rc = index->open(index_file.c_str(), *index_meta, field_meta);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive PhysicalTable function.
      return rc;
    }
    indexes_.push_back(index);
  }

  return rc;
}


RC PhysicalTable::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;

  for (Index *index : indexes_) {
    if (ignore_index(index, record))
      continue;
    rc = index->unique_check(record.data(), &record.rid());
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  rc = record_handler_->insert_record(record.data(), table_meta_.record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record, record.rid());
  if (rc != RC::SUCCESS) { 
    LOG_WARN("insert entry of indexes error");
  }
  return rc;
}

RC PhysicalTable::update_record_impl(std::vector<const FieldMeta *> &field_metas, std::vector<Value> &values, Record &record) {
  assert(field_metas.size() == values.size());
  for (size_t i = 0; i < field_metas.size(); i++) {
    Value *value = &values[i];
    const FieldMeta *field_meta = field_metas[i];
    if (value->attr_type() != NULL_TYPE) {
      int len;
      if (value->attr_type() == CHARS) {
        len = min(value->length() + 1, field_meta->len());
      } else {
        len = min(value->length(), field_meta->len());
      }
      memcpy(record.data() + field_meta->offset(), value->data(), len);
    } 
    if (value->attr_type() == NULL_TYPE) {
      record.set_null(field_metas[i]->name(), this);
    } else {
      record.unset_null(field_metas[i]->name(), this);
    }
  }
  return RC::SUCCESS;
}

bool PhysicalTable::ignore_index(Index *index, const Record &record) {
  for (const FieldMeta &fm : index->field_meta()) {
    bool isnull;
    record.get_null(fm.name(), this, isnull);
    if (isnull) return true;
  }
  return false;
}

bool PhysicalTable::update_need_unique_check(Index *index, const char *olddata, const char *newdata) {
  if (index->get_index_meta_unique()) {
    bool all_the_same = true;
    for (const FieldMeta &fm : index->field_meta()) {
      if (memcmp(olddata + fm.offset(), newdata + fm.offset(), fm.len()) != 0) {
        all_the_same = false;
        break;
      }
    }
    if (all_the_same) {
      return false;
    } else {
      return true;
    }
  } else {
    return false;
  }
}

RC PhysicalTable::update_record(Record &record)
{
  RC rc = RC::SUCCESS;

  const int record_size = table_meta_.record_size();
  char *data_bak = (char *)malloc(record_size);

  auto copier = [&](Record &record_src) {
    memcpy(data_bak, record_src.data(), record_size);
  };
  rc = record_handler_->visit_record(record.rid(), false/*readonly*/, copier);
  if (rc != RC::SUCCESS) {
    free(data_bak);
    LOG_WARN("failed to visit record");
    return rc;
  }

  for (Index *index : indexes_) {
    if (ignore_index(index, record))
      continue;
    if (update_need_unique_check(index, data_bak, record.data())) {
      rc = index->unique_check(record.data(), &record.rid());
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    rc = index->delete_entry(data_bak, &record.rid());
    if (RC::SUCCESS != rc) {
      LOG_WARN("failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
      return rc;
    }
    rc = index->insert_entry(record.data(), &record.rid());
    if (RC::SUCCESS != rc) {
      LOG_WARN("failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
      return rc;
    }
  }

  rc = record_handler_->update_record(record.data(), table_meta_.record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_WARN("update record error %s", strrc(rc));
    return rc;
  }

  return rc;
}

RC PhysicalTable::visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor)
{
  return record_handler_->visit_record(rid, readonly, visitor);
}

RC PhysicalTable::get_record(const RID &rid, Record &record)
{
  const int record_size = table_meta_.record_size();
  char *record_data = (char *)malloc(record_size);
  ASSERT(nullptr != record_data, "failed to malloc memory. record data size=%d", record_size);

  auto copier = [&record, record_data, record_size](Record &record_src) {
    memcpy(record_data, record_src.data(), record_size);
    record.set_rid(record_src.rid());
  };
  RC rc = record_handler_->visit_record(rid, true/*readonly*/, copier);
  if (rc != RC::SUCCESS) {
    free(record_data);
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), name(), strrc(rc));
    return rc;
  }

  record.set_data_owner(record_data, record_size);
  return rc;
}

RC PhysicalTable::recover_insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc = record_handler_->recover_insert_record(record.data(), table_meta_.record_size(), record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record, record.rid());
  if (rc != RC::SUCCESS) { // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record, record.rid(), false/*error_on_not_exists*/,false);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

const char *PhysicalTable::name() const
{
  return table_meta_.name();
}

const TableMeta &PhysicalTable::table_meta() const
{
  return table_meta_;
}

const char * PhysicalTable::table_dir()
{
  return base_dir_.c_str();
}

RC PhysicalTable::make_record(int value_num, const Value *values, Record &record)
{
  // 检查字段类型是否一致
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    LOG_WARN("Input values don't match the table's schema, table name:%s", table_meta_.name());
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    Value &value = const_cast<Value &>(values[i]);
    if (!field->match(value)) {
      LOG_ERROR("Invalid value type. table name=%s, field name=%s, type=%d, but given=%d",
                table_meta_.name(), field->name(), field->type(), value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // 需要record末尾几个字节来作为数据是否为null的判据
  int record_size = table_meta_.record_size();
  char *record_data = (char *)malloc(record_size);

  int column = table_meta_.field_num();
  int nr_null_bytes = NR_NULL_BYTE(column);
  char null_bytes[nr_null_bytes];
  memset(null_bytes, 0x00, nr_null_bytes);

  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    const Value &value = values[i];
    // 如果是字符串类型，则拷贝字符串的len + 1字节。
    size_t copy_len = field->len();
    if (field->type() == CHARS || field->type() == DATES) {
      const size_t data_len = value.length();
      if (copy_len > data_len) {
        copy_len = data_len + 1;
      }
    }
    int isnotnull = value.attr_type() != NULL_TYPE;
    null_bytes[i/8] |= (isnotnull << (i % 8));
    memcpy(record_data + field->offset(), value.data(), copy_len);
  }
  memcpy(record_data + record_size - nr_null_bytes, null_bytes, nr_null_bytes);

  record.set_data_owner(record_data, record_size);
  return RC::SUCCESS;
}

RC PhysicalTable::init_record_handler(const char *base_dir)
{
  std::string data_file = table_data_file(base_dir, table_meta_.name());

  RC rc = BufferPoolManager::instance().open_file(data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler();
  rc = record_handler_->init(data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC PhysicalTable::get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly)
{
  RC rc = scanner.open_scan(this, *data_buffer_pool_, trx, readonly, nullptr);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

RC PhysicalTable::create_index(Trx *trx, const std::vector<FieldMeta> field_meta, const char *index_name, bool unique)
{
  if (common::is_blank(index_name) || field_meta.empty() == true) {
    LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or attribute_name is blank", name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;
  RC rc = new_index_meta.init(index_name, field_meta);
  if (rc != RC::SUCCESS) {
    LOG_INFO("Failed to init IndexMeta in table:%s", name());
    return rc;
  }

  // 创建索引相关数据
  BplusTreeIndex *index = new BplusTreeIndex();
  std::string index_file = table_index_file(base_dir_.c_str(), name(), index_name);
  rc = index->create(index_file.c_str(), new_index_meta, field_meta);
  index->set_index_meta_unique(unique);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  RecordFileScanner scanner;
  rc = get_record_scanner(scanner, trx, true/*readonly*/);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s", 
             name(), index_name, strrc(rc));
    return rc;
  }

  Record record;
  while (scanner.has_next()) {
    rc = scanner.next(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      return rc;
    }
    rc = index->insert_entry_first(record.data(), &record.rid());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      return rc;         
    }
  }
  scanner.close_scan();
  LOG_INFO("inserted all records into new index. table=%s, index=%s", name(), index_name);
  
  indexes_.push_back(index);

  /// 接下来将这个索引放到表的元数据中
  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }

  /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
  /// 这样可以防止文件内容不完整
  // 创建元数据临时文件
  std::string tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  // 覆盖原始元数据文件
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());
  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
              tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }

  table_meta_.swap(new_table_meta);

  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, name());
  return rc;
}

RC PhysicalTable::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    if (ignore_index(index, record)) continue;

    rc = index->delete_entry(record.data(), &record.rid());
    ASSERT(RC::SUCCESS == rc, 
           "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
           name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
  }
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC PhysicalTable::insert_entry_of_indexes(const Record &record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    if (ignore_index(index, record)) continue;

    rc = index->insert_entry(record.data(), &rid);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  return rc;
}

RC PhysicalTable::delete_entry_of_indexes(const Record &record, const RID &rid, bool error_on_not_exists, bool if_update)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    assert(!ignore_index(index, record));
    rc = index->delete_entry(record.data(), &rid);
    if(rc == RC::UNIQUE_INDEX){
      return rc;
    }
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *PhysicalTable::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}

std::vector<Index *> PhysicalTable::indexes()
{
  return indexes_;
}

Index *PhysicalTable::find_index_by_field(const char *field_name) const
{
  const TableMeta &table_meta = this->table_meta();
  const IndexMeta *index_meta = table_meta.find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

RC PhysicalTable::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc;
    }
  }
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}