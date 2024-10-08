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
// Created by Wangyunlai on 2023/06/25.
//

#include "net/plain_communicator.h"
#include "net/buffered_writer.h"
#include "sql/expr/tuple.h"
#include "sql/expr/aggregation_func.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/groupby_physical_operator.h"
#include "event/session_event.h"
#include "session/session.h"
#include "common/io/io.h"
#include "common/log/log.h"
#include <memory>

PlainCommunicator::PlainCommunicator()
{
  send_message_delimiter_.assign(1, '\0');
  debug_message_prefix_.resize(2);
  debug_message_prefix_[0] = '#';
  debug_message_prefix_[1] = ' ';
}

RC PlainCommunicator::read_event(SessionEvent *&event)
{
  RC rc = RC::SUCCESS;

  event = nullptr;

  int data_len = 0;
  int read_len = 0;

  const int max_packet_size = 81920;
  std::vector<char> buf(max_packet_size);

  // 持续接收消息，直到遇到'\0'。将'\0'遇到的后续数据直接丢弃没有处理，因为目前仅支持一收一发的模式
  while (true) {
    read_len = ::read(fd_, buf.data() + data_len, max_packet_size - data_len);
    if (read_len < 0) {
      if (errno == EAGAIN) {
        continue;
      }
      break;
    }
    if (read_len == 0) {
      break;
    }

    if (read_len + data_len > max_packet_size) {
      data_len += read_len;
      break;
    }

    bool msg_end = false;
    for (int i = 0; i < read_len; i++) {
      if (buf[data_len + i] == 0) {
        data_len += i + 1;
        msg_end = true;
        break;
      }
    }

    if (msg_end) {
      break;
    }

    data_len += read_len;
  }

  if (data_len > max_packet_size) {
    LOG_WARN("The length of sql exceeds the limitation %d", max_packet_size);
    return RC::IOERR_TOO_LONG;
  }
  if (read_len == 0) {
    LOG_INFO("The peer has been closed %s", addr());
    return RC::IOERR_CLOSE;
  } else if (read_len < 0) {
    LOG_ERROR("Failed to read socket of %s, %s", addr(), strerror(errno));
    return RC::IOERR_READ;
  }

  LOG_INFO("receive command(size=%d): %s", data_len, buf.data());
  event = new SessionEvent(this);
  event->set_query(std::string(buf.data()));
  return rc;
}

RC PlainCommunicator::write_state(SessionEvent *event, bool &need_disconnect)
{
  SqlResult *sql_result = event->sql_result();
  const int buf_size = 2048;
  char *buf = new char[buf_size];
  const std::string &state_string = sql_result->state_string();
  if (state_string.empty()) {
    const char *result = RC::SUCCESS == sql_result->return_code() ? "SUCCESS" : "FAILURE";
    snprintf(buf, buf_size, "%s\n", result);
  } else {
    snprintf(buf, buf_size, "%s > %s\n", strrc(sql_result->return_code()), state_string.c_str());
  }

  RC rc = writer_->writen(buf, strlen(buf));
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to send data to client. err=%s", strerror(errno));
    need_disconnect = true;
    delete[] buf;
    return RC::IOERR_WRITE;
  }

  need_disconnect = false;
  delete[] buf;

  return RC::SUCCESS;
}

RC PlainCommunicator::write_debug(SessionEvent *request, bool &need_disconnect)
{
  if (!session_->sql_debug_on() || !request->main_query_) {
    return RC::SUCCESS;
  }

  SqlDebug &sd = request->sql_debug();
  const std::list<std::string> &debug_infos = sd.get_debug_infos();
  for (auto &debug_info : debug_infos) {
    RC rc = writer_->writen(debug_message_prefix_.data(), debug_message_prefix_.size());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));
      need_disconnect = true;
      return RC::IOERR_WRITE;
    }

    rc = writer_->writen(debug_info.data(), debug_info.size());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));
      need_disconnect = true;
      return RC::IOERR_WRITE;
    }

    char newline = '\n';
    rc = writer_->writen(&newline, 1);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send new line to client. err=%s", strerror(errno));
      need_disconnect = true;
      return RC::IOERR_WRITE;
    }
  }

  need_disconnect = false;
  return RC::SUCCESS;
}

RC PlainCommunicator::write_result(SessionEvent *event, bool &need_disconnect)
{
  RC rc = write_result_internal(event, need_disconnect);
  if (!need_disconnect) {
    rc = write_debug(event, need_disconnect);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send debug info to client. rc=%s, err=%s", strrc(rc), strerror(errno));
    }
    rc = writer_->writen(send_message_delimiter_.data(), send_message_delimiter_.size());
    if (OB_FAIL(rc)) {
      LOG_ERROR("Failed to send data back to client. ret=%s, error=%s", strrc(rc), strerror(errno));
      need_disconnect = true;
      return rc;
    }
  }
  writer_->flush(); // TODO handle error
  return rc;
}

RC PlainCommunicator::write_tuple(SqlResult *sql_result) {
  RC rc = RC::SUCCESS;

  Tuple *tuple = nullptr;
  bool aggregate = false;
  std::vector<Value> last_values;
  std::unique_ptr<PhysicalOperator> &oper = sql_result->get_operator();
  ProjectTuple *pj = nullptr;
  if (oper->type() == PhysicalOperatorType::PROJECT) {
    pj = &static_cast<ProjectPhysicalOperator *>(oper.get())->project_tuple();
    pj->get_aggregate(aggregate);
    std::vector<Expression *> &exprs = pj->exprs();
    std::vector<Value> v_for_type;
    for (Expression *expr : exprs) {
      if (expr->type() == ExprType::STAR) {
        StarExpr *star_expr = static_cast<StarExpr *>(expr);
        for (Field &f : star_expr->field()) {
          Value v;
          v.set_type(f.attr_type());
          v.set_length(f.attr_len());
          v_for_type.push_back(v);
        }
      } else {
        Value v(expr->value_type());
        if (v.attr_type() == DATES)
          v.set_length(10);
        else
          v.set_length(4);
        v_for_type.push_back(v);
      }
    }
    writer_->accept(v_for_type);
  }

  while (RC::SUCCESS == (rc = sql_result->next_tuple(tuple))) {
    int cell_num = tuple->cell_num();
    if (tuple->type() == Tuple::PROJECT) {
      ProjectTuple *pj = static_cast<ProjectTuple *>(tuple);
      rc = pj->get_aggregate(aggregate);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }

    for (int i = 0; i < cell_num; i++) {
      if (i != 0 && !aggregate) {
        const char *delim = " | ";
        writer_->writen(delim, strlen(delim));
      }

      Value value;
      rc = tuple->cell_at(i, value);
      if (rc != RC::SUCCESS)
        return rc;

      if (value.attr_type() == TEXTS)
      {
        RID *rid = reinterpret_cast<RID *>(const_cast<char *>(value.data()));
        size_t len = rid->text_value;
        char *ss = new char[len + 1];
        char *p = ss;
        while (rid != nullptr && rid->init) {
          Record rec_new;
          tuple->get_text_record(rec_new, rid);
          memcpy(p, rec_new.data(), rec_new.len());
          p += rec_new.len();
          rid = rid->next_RID;
        }
        writer_->writen(ss, len);
        delete ss;
      }
      if (!aggregate && value.attr_type() != TEXTS) {
        writer_->writen(value.to_string().c_str(), value.to_string().size());
      }

      if (last_values.size() == (size_t)cell_num)
        last_values[i] = value;
      else
        last_values.push_back(value);
    }
    writer_->accept(last_values);
    if (!aggregate)
      writer_->writen("\n", 1);
  }
  if (aggregate) {
    if (last_values.size() == 0 && pj) {
      std::vector<Expression *> &exprs = pj->exprs();
      for (Expression *expr : exprs) {
        AggType type;
        expr->get_aggregate(type);
        if (type == AGG_COUNT) {
          last_values.push_back(Value(0));
        } else {
          last_values.push_back(Value(NULL_TYPE));
        }
      }
    }
    for (Value &v : last_values) {
      if (&v != &last_values.front()) {
        const char *delim = " | ";
        writer_->writen(delim, strlen(delim));
      }
      writer_->writen(v.to_string().c_str(), v.to_string().size());
    }
    writer_->writen("\n", 1);
  }
  return rc;
}

RC PlainCommunicator::write_result_internal(SessionEvent *event, bool &need_disconnect)
{
  RC rc = RC::SUCCESS;
  need_disconnect = true;

  SqlResult *sql_result = event->sql_result();

  if (RC::SUCCESS != sql_result->return_code() || !sql_result->has_operator()) {
    return write_state(event, need_disconnect);
  }

  rc = sql_result->open();
  if (OB_FAIL(rc)) {
    sql_result->close();
    sql_result->set_return_code(rc);
    return write_state(event, need_disconnect);
  }

  TupleSchema &schema = const_cast<TupleSchema &>(sql_result->tuple_schema());
  const int cell_num = schema.cell_num();

  for (int i = 0; i < cell_num; i++) {
    const TupleCellSpec &spec = schema.cell_at(i);
    if (i != 0) {
      const char *delim = " | ";
      writer_->writen(delim, strlen(delim));
    }
    std::string out;
    if (spec.alias() || spec.alias()[0] != 0) {
      out = spec.alias();
    } else {
      if (spec.table_name()) {
        out = spec.table_name() + std::string(".") + spec.field_name();
      } else {
        out = spec.field_name();
      }
    }
    writer_->writen(out.c_str(), out.size());
  }

  if (cell_num > 0) {
    char newline = '\n';
    rc = writer_->writen(&newline, 1);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to send data to client. err=%s", strerror(errno));
      sql_result->close();
      return rc;
    }
  }

  rc = write_tuple(sql_result);
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  } else {
    LOG_WARN("write tuple failed: %s", strrc(rc));
    sql_result->close();
    sql_result->set_return_code(rc);
    return write_state(event, need_disconnect);
  }

  if (cell_num == 0) {
    // 除了select之外，其它的消息通常不会通过operator来返回结果，表头和行数据都是空的
    // 这里针对这种情况做特殊处理，当表头和行数据都是空的时候，就返回处理的结果
    // 可能是insert/delete等操作，不直接返回给客户端数据，这里把处理结果返回给客户端
    RC rc_close = sql_result->close();
    if (rc == RC::SUCCESS) {
      rc = rc_close;
    }
    sql_result->set_return_code(rc);
    return write_state(event, need_disconnect);
  } else {
    need_disconnect = false;
  }

  RC rc_close = sql_result->close();
  if (OB_SUCC(rc)) {
    rc = rc_close;
  }

  return rc;
}