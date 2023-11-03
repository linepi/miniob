#include <sql/expr/aggregation_func.h>
#include <cfloat>

AggregationFunc::AggregationFunc(AggType agg_type) 
  : agg_type_(agg_type)
{ 
  result_.set_null(); 
  sum_.set_null();
}

void AggregationFunc::reset() {
  result_.set_null(); 
  sum_.set_null();
	cnt_ = 0;
}

RC AggregationFunc::iterate(Value &value, bool agg_on) {
  if (agg_on) {
    RC rc = aggregate(&value);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  value = result();
  return RC::SUCCESS;
}

Value AggregationFunc::result() {
  if (result_.attr_type() == NULL_TYPE && agg_type_ == AGG_COUNT) {
    return Value(0);
  } 
  return result_;
}

RC AggregationFunc::aggregate(Value *value) {
  if (value->attr_type() == NULL_TYPE) return RC::SUCCESS;
  if (value->attr_type() == CHARS && (agg_type_ == AGG_AVG || agg_type_ == AGG_SUM)) {
    try {
      std::stof(value->to_string());
    } catch (std::exception const &ex) {
      return RC::INVALID_ARGUMENT;
    }
  }
  switch (agg_type_) {
    case AGG_AVG:
      avg(value);
      break;
    case AGG_MIN:
      min(value);
      break;
    case AGG_MAX:
      max(value);
      break;
    case AGG_COUNT:
      count(value);
      break;
    case AGG_SUM:
      sum(value);
      break;
    default:
      break;
  }
  return RC::SUCCESS;
}

void AggregationFunc::min(Value *value) {
  if (result_.attr_type() == NULL_TYPE) {
    result_ = *value;
    return;
  }
  int cmp;
  result_.compare(*value, cmp);
  if (cmp > 0) result_ = *value;
}

void AggregationFunc::max(Value *value) {
  if (result_.attr_type() == NULL_TYPE ) {
    result_ = *value;
    return;
  }
  int cmp;
  result_.compare(*value, cmp);
  if (cmp < 0) result_ = *value;
}

void AggregationFunc::avg(Value *value) {
  if (sum_.attr_type() == NULL_TYPE ) 
    sum_ = *value;
  else
    sum_ = sum_ + *value;
  cnt_ = cnt_ + 1;
  result_.set_float(sum_.get_float() / cnt_);
}

void AggregationFunc::count(Value *value) {
  cnt_ = cnt_ + 1;
  result_.set_int(cnt_);
}

void AggregationFunc::sum(Value *value) {
  if (sum_.attr_type() == NULL_TYPE ) 
    sum_ = *value;
  else
    sum_ = result_ + *value;
  result_ = sum_;
}
