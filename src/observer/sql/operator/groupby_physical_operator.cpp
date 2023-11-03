#include <algorithm>
#include "sql/parser/value.h"
#include "sql/operator/groupby_physical_operator.h"

GroupByPhysicalOperator::GroupByPhysicalOperator(std::vector<Expression *> groupby, Expression *having, std::vector<Expression *> select_exprs) 
	: groupby_(groupby), having_(having), select_exprs_(select_exprs)
{
	for (Expression * e : groupby) {
		fields_.push_back(static_cast<FieldExpr *>(e)->field());
	}
}

RC GroupByPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("GroupBy operator must has one child");
    return RC::INTERNAL;
  }

  return children_[0]->open(trx);
}

bool is_a_group(std::vector<Value> &a, std::vector<Value> &b) {
	if (a.size() != b.size()) return false;
	for (size_t i = 0; i < a.size(); i++) {
		Value &av = a[i];
		Value &bv = b[i];
		if (!(av == bv)) {
			if (av.attr_type() == NULL_TYPE && bv.attr_type() == NULL_TYPE) {
				continue;
			} else {
				return false;
			}
		}
	}
	return true;
}

RC GroupByPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
	if (inited_) {
		res_idx_++;
		if (res_idx_ >= results_.size()) {
			return RC::RECORD_EOF;
		}
		return rc;
	}

	PhysicalOperator *child = children_.front().get();
	std::vector<Value> cur_value;
	std::vector<Tuple *> cur_group;

	while ((rc = child->next()) == RC::SUCCESS) {
		Tuple *tuple = child->current_tuple();
		if (tuple == nullptr) 
			break;
		
		std::vector<Value> the_value;
		for (Field &f : fields_) {
			Value v;
			TupleCellSpec spec(f.table_name(), f.field_name());
			tuple->find_cell(spec, v);
			the_value.push_back(v);
		}
		if (cur_value.size() == 0) {
			cur_value = the_value;
		}

		if (!is_a_group(the_value, cur_value)) {
			groups_.push_back(cur_group);
			cur_group.clear();
			cur_value = the_value;
		}
		cur_group.push_back(tuple);
	};
	groups_.push_back(cur_group);

	for (std::vector<Tuple *> &group : groups_) {
		// first traverse to get aggregation values in having
		if (having_) {
			having_->toggle_aggregate(true);
			having_->reset_aggregate();
			for (Tuple *t : group) {
				Value tmp;
				rc = having_->get_value(*t, tmp);
				if (rc != RC::SUCCESS) {
					LOG_WARN("having get value error");
					return rc;
				}
			}
			having_->toggle_aggregate(false);
		}
		GroupTuple group_tuple;
		for (Tuple *&t : group) {
			// filter by having
			if (having_) {
				Value filter_v;
				rc = having_->get_value(*t, filter_v);
				if (rc != RC::SUCCESS) {
					LOG_WARN("group by get having value error");
					return rc;
				}
				if (!filter_v.get_boolean())
					continue;
			}

			// iterate
			for (Expression *attr : select_exprs_) {
				Value v;
				rc = attr->get_value(*t, v);
				if (rc != RC::SUCCESS) {
					LOG_WARN("group by get attr value error");
					return rc;
				}
				// if it is the last iterate, push the value
				if (&t == &group.back()) {
					attr->reset_aggregate();
					group_tuple.cells_.push_back(v);
				}
			}
		}
		if (group_tuple.cells_.size() != 0)
			results_.push_back(std::move(group_tuple));
	}

	inited_ = true;
	if (res_idx_ >= results_.size()) {
		return RC::RECORD_EOF;
	}
  return rc;
}

RC GroupByPhysicalOperator::close()
{
	if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *GroupByPhysicalOperator::current_tuple()
{
  return &results_[res_idx_];
}
