#include <net/silent_writer.h>
#include <sql/parser/value.h>

RC SilentWriter::write(const char *data, int32_t size, int32_t &write_size) {
	std::string tmp(data, size);
	content_ += tmp;
	write_size = size;
	return RC::SUCCESS;
}

RC SilentWriter::writen(const char *data, int32_t size) {
	std::string tmp(data, size);
	content_ += tmp;
	return RC::SUCCESS;
}

RC SilentWriter::accept(std::vector<Value> &vs) {
	if (!create_table_) return RC::SUCCESS;
	if (attr_infos_.size() == 0) {
		attr_infos_.resize(vs.size());
	}
	for (size_t i = 0; i < vs.size(); i++) {
		Value &v = vs[i];
		AttrInfoSqlNode &attr_info = attr_infos_[i];
		if ((size_t)v.length() > attr_info.length)
			attr_info.length = v.length();
		attr_info.type = v.attr_type();
	}
	return RC::SUCCESS;
}