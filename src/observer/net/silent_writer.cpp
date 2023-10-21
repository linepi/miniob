#include <net/silent_writer.h>

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