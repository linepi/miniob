#include <sql/expr/aggregation_func.h>
#include <sql/parser/value.h>
#include <cfloat>

const char *AGG_TYPE_NAME[] = {
  "UNDEFINED", 
  "MIN", 
  "MAX",
  "AVG",         
  "SUM",         
  "COUNT",       
};

AggregationFunc::AggregationFunc(AggType agg_type, bool star, std::string field_name) 
  : agg_type_(agg_type), star_(star), field_name_(std::move(field_name))
{ 
  switch (agg_type_) {
    case AGG_MIN:
      result_.set_float(FLT_MAX); 
      break;
    case AGG_MAX:
      result_.set_float(FLT_MIN); 
      break;
    default:
      result_.set_float(0.0); 
      break;
  }
  sum_.set_float(0.0);
}

RC AggregationFunc::aggregate(Value *value) {
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
  if (result_.attr_type() != value->attr_type() && 
    (value->attr_type() == CHARS || value->attr_type() == DATES)) {
    result_ = *value;
    return;
  }
  int cmp;
  result_.compare(*value, cmp);
  if (cmp > 0) result_ = *value;
}

void AggregationFunc::max(Value *value) {
  if (result_.attr_type() != value->attr_type() && 
    (value->attr_type() == CHARS || value->attr_type() == DATES)) {
    result_ = *value;
    return;
  }
  int cmp;
  result_.compare(*value, cmp);
  if (cmp < 0) result_ = *value;
}

void AggregationFunc::avg(Value *value) {
  sum_ = sum_ + *value;
  cnt_ = cnt_ + 1;
  result_.set_float(sum_.get_float() / cnt_);
}

void AggregationFunc::count(Value *value) {
  cnt_ = cnt_ + 1;
  result_.set_int(cnt_);
}

void AggregationFunc::sum(Value *value) {
  sum_ = result_ + *value;
  result_ = sum_;
}
