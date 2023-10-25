#include <sql/operator/update_logical_operator.h> 

UpdateLogicalOperator::UpdateLogicalOperator(
	Table *table, std::vector<ValueWrapper> values, std::vector<const FieldMeta *> field_metas) 
	: table_(table), values_(values), field_metas_(field_metas)
{}