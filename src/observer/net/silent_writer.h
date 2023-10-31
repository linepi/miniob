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
// Created by Wangyunlai on 2023/06/16.
//

#pragma once

#include <cstdint>
#include <string>
#include "sql/parser/parse_defs.h"
#include "net/writer.h"
#include <common/rc.h>

/**
 * @brief 支持以缓存模式写入数据到文件/socket
 * @details 缓存使用ring buffer实现，当缓存满时会自动刷新缓存。
 * 看起来直接使用fdopen也可以实现缓存写，不过fdopen会在close时直接关闭fd。
 * @note 在执行close时，描述符fd并不会被关闭
 */
class SilentWriter : public Writer
{
public:
  SilentWriter() = default;
  ~SilentWriter() = default;

  RC close() override { return RC::SUCCESS; };

  RC write(const char *data, int32_t size, int32_t &write_size) override;

  RC writen(const char *data, int32_t size) override;

  RC accept(std::vector<Value> &vs) override;

  RC flush() override { return RC::SUCCESS; }

  std::string &content() { return content_; }
  std::vector<AttrInfoSqlNode> attr_infos() { return attr_infos_; }

  bool create_table_;
private:
  std::string content_;
  std::vector<AttrInfoSqlNode> attr_infos_;
};