#include <storage/record/record.h>
#include <storage/table/table.h>

RC Record::set_null_impl(const std::string &field_name, const Table *table, int mode, bool &isnull) const{
	int column_idx = -1;
	const TableMeta &table_meta = table->table_meta();
	for (size_t i =table_meta.sys_field_num(); i < (size_t)table_meta.field_num(); i++) {
		if (strcasecmp(field_name.c_str(), (*table_meta.field_metas())[i].name()) == 0) 
			column_idx = i; 
	}
	assert(column_idx != -1);
	column_idx -= table_meta.sys_field_num();
	char *null_byte_start = data_ + table_meta.record_size() - NR_NULL_BYTE(table_meta.field_num());

	if (mode == 1) {
		char thing = ~(1 << (column_idx % 8));
		null_byte_start[column_idx/8] &= thing;
	} else if (mode == 0) {
		char thing = (1 << (column_idx % 8));
		null_byte_start[column_idx/8] |= thing;
	} else if (mode == -1) {
		char thing = ~(null_byte_start[column_idx/8]);
		isnull = (thing & (1 << (column_idx % 8))) != 0;
	} else {
		assert(0);
	}
	return RC::SUCCESS;
}

RC Record::set_null(const std::string &field_name, const Table *table) const{
	bool isnull;
	RC rc = set_null_impl(field_name, table, 1, isnull);
	(void)isnull;
	return rc;
}

RC Record::unset_null(const std::string &field_name, const Table *table) const{
	bool isnull;
	RC rc = set_null_impl(field_name, table, 0, isnull);
	(void)isnull;
	return rc;
}

RC Record::get_null(const std::string &field_name, const Table *table, bool &isnull) const{
	RC rc = set_null_impl(field_name, table, -1, isnull);
	(void)isnull;
	return rc;
}