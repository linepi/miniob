// OrderByPhysicalOperator.cpp

#include <algorithm>
#include "sql/operator/order_by_physical_operator.h"

#define MAX_BUFFER_SIZE 1000


RC OrderByPhysicalOperator::open(Trx* trx) {
    // 首先，打开子操作符
    RC rc = children_[0]->open(trx);
    if (rc != RC::SUCCESS) {
        return rc;
    }
    return RC::SUCCESS;
}


Tuple *OrderByPhysicalOperator::current_tuple()
{
  if(is_mult_table){
    return &current_tuple_join_;
  }else {
    return &current_tuple_;
  }
}

RC OrderByPhysicalOperator::next() {

if(!is_mult_table){
    if(inited_ == false)
    {
        RC rc =RC::SUCCESS;
        PhysicalOperator* child = children_.front().get();
        while ((rc = child->next()) == RC::SUCCESS) {
            RowTuple* tuple = static_cast<RowTuple*>(child->current_tuple());
            if (tuple == nullptr) {
                break;
            }
            size_t record_len = tuple->table()->table_meta().record_size();
            char *data = (char *)malloc(record_len);
            memcpy(data, tuple->record().data(), record_len);

            Record *r_d = new Record(tuple->record());
            r_d->set_data(data);


            RowTuple *new_tuple(tuple);
            new_tuple->set_record(r_d);

            buffer_.push_back(*new_tuple);
        };
        // 使用lambda函数定义排序规则
        auto comparator = [&](const RowTuple& lhs, const RowTuple& rhs) -> bool {
        for (size_t i = 0; i < orderByColumns.size(); ++i) {
            const Field& field = orderByColumns[i];
            Value lhsValue, rhsValue;

            // 使用TupleCellSpec查找指定字段的值
            TupleCellSpec spec(field.table_name(), field.field_name()); // 假设FieldMeta有table_name和name方法
            lhs.find_cell(spec, lhsValue);
            rhs.find_cell(spec, rhsValue);

            // 假设值可以进行比较，且返回值为-1、0或1，分别表示小于、等于或大于
            int compareResult;
            lhsValue.compare(rhsValue,compareResult); // 假设Value类有一个compare方法

            if (compareResult < 0) {
                return sort_info[i]; // 如果为true，则按升序，否则按降序
            } else if (compareResult > 0) {
                return !sort_info[i]; // 反向排序
            }
            // 如果比较结果是0，将继续使用下一个排序字段
            }
            return false; // 所有字段都相等
        };
        std::sort(buffer_.begin(), buffer_.end(), comparator);
        child->close();
        inited_ =true;
    }


    if (current_position_ >= buffer_.size()) {
        return RC::RECORD_EOF;  
    }

    // 获取当前位置的元组并保存
    current_tuple_ = buffer_[current_position_];
    current_position_++;
    } 
    else {
    if(inited_ == false)
    {
        RC rc =RC::SUCCESS;
        PhysicalOperator* child = children_.front().get();
        while ((rc = child->next()) == RC::SUCCESS) {
            JoinedTuple* tuple = static_cast<JoinedTuple*>(child->current_tuple());
            if (tuple == nullptr) {
                break;
            }

            JoinedTuple* copiedTuple = new JoinedTuple(*tuple);
            // size_t record_len = tuple->table()->table_meta().record_size();
            // char *data = (char *)malloc(record_len);
            // memcpy(data, tuple->record().data(), record_len);

            // Record *r_d = new Record(tuple->record());
            // r_d->set_data(data);


            // RowTuple *new_tuple(tuple);
            // new_tuple->set_record(r_d);

            buffer_join_.push_back(*copiedTuple);
        };
        // 使用lambda函数定义排序规则
        auto comparator = [&](const JoinedTuple& lhs, const JoinedTuple& rhs) -> bool {
        for (size_t i = 0; i < orderByColumns.size(); ++i) {
            const Field& field = orderByColumns[i];
            Value lhsValue, rhsValue;

            // 使用TupleCellSpec查找指定字段的值
            TupleCellSpec spec(field.table_name(), field.field_name()); // 假设FieldMeta有table_name和name方法
            lhs.find_cell(spec, lhsValue);
            rhs.find_cell(spec, rhsValue);

            // 假设值可以进行比较，且返回值为-1、0或1，分别表示小于、等于或大于
            int compareResult;
            lhsValue.compare(rhsValue,compareResult); // 假设Value类有一个compare方法

            if (compareResult < 0) {
                return sort_info[i]; // 如果为true，则按升序，否则按降序
            } else if (compareResult > 0) {
                return !sort_info[i]; // 反向排序
            }
            // 如果比较结果是0，将继续使用下一个排序字段
            }
            return false; // 所有字段都相等
        };
        std::sort(buffer_join_.begin(), buffer_join_.end(), comparator);
        child->close();
        inited_ =true;
    }


    if (current_position_ >= buffer_join_.size()) {
        return RC::RECORD_EOF;  
    }

    // 获取当前位置的元组并保存
    current_tuple_join_ = buffer_join_[current_position_];
    current_position_++;
}

    return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()  {

    current_position_ = 0;

    if (!children_.empty()) {
        children_[0]->close();
    }
    return RC::SUCCESS;
}