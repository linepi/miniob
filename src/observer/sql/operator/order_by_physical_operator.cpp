// OrderByPhysicalOperator.cpp

#include <algorithm>
#include "sql/operator/order_by_physical_operator.h"

RC OrderByPhysicalOperator::open(Trx *trx)
{
  RC rc = children_[0]->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
  if (is_mult_table) {
    return &current_tuple_join_;
  } else {
    return &current_tuple_;
  }
}

RC OrderByPhysicalOperator::next()
{
  if (!is_mult_table) {
    if (inited_ == false) {
      RC                rc    = RC::SUCCESS;
      PhysicalOperator *child = children_.front().get();
      while ((rc = child->next()) == RC::SUCCESS) {
        RowTuple *tuple = static_cast<RowTuple *>(child->current_tuple());
        if (tuple == nullptr) {
          break;
        }
        size_t record_len = tuple->table()->table_meta().record_size();
        char  *data       = (char *)malloc(record_len);
        memcpy(data, tuple->record().data(), record_len);

        Record *r_d = new Record(tuple->record());
        r_d->set_data(data);

        RowTuple t(*tuple);
        t.set_record(r_d);
        buffer_.push_back(t);
      };
      auto comparator = [&](const RowTuple &lhs, const RowTuple &rhs) -> bool {
        for (size_t i = 0; i < orderByColumns.size(); ++i) {
          const Field &field = orderByColumns[i];
          Value        lhsValue, rhsValue;

          TupleCellSpec spec(field.table_name(), field.field_name());
          lhs.find_cell(spec, lhsValue);
          rhs.find_cell(spec, rhsValue);

          int compareResult;
          lhsValue.compare(rhsValue, compareResult);

          if (compareResult < 0) {
            return sort_info[i];
          } else if (compareResult > 0) {
            return !sort_info[i];
          }
        }
        return false;
      };
      std::sort(buffer_.begin(), buffer_.end(), comparator);
      child->close();
      inited_ = true;
    }

    if (current_position_ >= buffer_.size()) {
      return RC::RECORD_EOF;
    }

    current_tuple_ = buffer_[current_position_];
    current_position_++;
  } else {
    if (inited_ == false) {
      RC                rc    = RC::SUCCESS;
      PhysicalOperator *child = children_.front().get();
      while ((rc = child->next()) == RC::SUCCESS) {
        JoinedTuple *tuple = static_cast<JoinedTuple *>(child->current_tuple());
        if (tuple == nullptr) {
          break;
        }

        JoinedTuple *copiedTuple = new JoinedTuple(*tuple);
        buffer_join_.push_back(*copiedTuple);
      };

      auto comparator = [&](const JoinedTuple &lhs, const JoinedTuple &rhs) -> bool {
        for (size_t i = 0; i < orderByColumns.size(); ++i) {
          const Field &field = orderByColumns[i];
          Value        lhsValue, rhsValue;

          TupleCellSpec spec(field.table_name(), field.field_name());
          lhs.find_cell(spec, lhsValue);
          rhs.find_cell(spec, rhsValue);

          int compareResult;
          lhsValue.compare(rhsValue, compareResult);

          if (compareResult < 0) {
            return sort_info[i];
          } else if (compareResult > 0) {
            return !sort_info[i];
          }
        }
        return false;
      };
      std::sort(buffer_join_.begin(), buffer_join_.end(), comparator);
      child->close();
      inited_ = true;
    }

    if (current_position_ >= buffer_join_.size()) {
      return RC::RECORD_EOF;
    }

    current_tuple_join_ = buffer_join_[current_position_];
    current_position_++;
  }

  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{

  current_position_ = 0;

  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}