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

#include <common/rc.h>
#include <cstdint>
#include <vector>
#include <sql/parser/value.h>

class Writer
{
public:
  Writer() = default;
  virtual ~Writer() = default;

  virtual RC close() = 0;
  virtual RC write(const char *data, int32_t size, int32_t &write_size) = 0;
  virtual RC writen(const char *data, int32_t size) = 0;
  virtual RC flush() = 0;
  virtual RC accept(std::vector<Value> &vs) = 0;
};