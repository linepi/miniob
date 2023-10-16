#include <sql/operator/update_logical_operator.h> 

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, Value value, const FieldMeta *field_meta) 
	: table_(table), value_(value), field_meta_(field_meta)
{}