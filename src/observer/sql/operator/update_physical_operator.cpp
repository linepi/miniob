#include "common/log/log.h"
#include "sql/operator/update_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/stmt/update_stmt.h"//

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    for (size_t i = 0; i < values_.size(); i++) {
      FieldMeta *field_meta = const_cast<FieldMeta *>(field_metas_[i]);
      Value &v = values_[i];
      if (!field_meta->match(v)) {
        LOG_WARN("field does not match value(%s and %s)", 
            attr_type_to_string(field_meta->type()), 
            attr_type_to_string(v.attr_type()));
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record &record = row_tuple->record();

    rc = trx_->visit_record(table_, record, false);
    if (rc == RC::RECORD_INVISIBLE) {
      continue;
    } else if (rc == RC::LOCKED_CONCURRENCY_CONFLICT) {
      LOG_WARN("update conflict: %s", strrc(rc));
      return rc;
    }

    char *data_changed = (char *)malloc(table_->table_meta().record_size());
    memcpy(data_changed, record.data(), table_->table_meta().record_size());
    Record record_changed(record);
    record_changed.set_data_owner(data_changed, table_->table_meta().record_size());

    table_->update_record_impl(field_metas_, values_, record_changed);

    rc = trx_->update_record(table_, record_changed);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
