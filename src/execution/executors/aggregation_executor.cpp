#include "onebase/execution/executors/aggregation_executor.h"
#include "onebase/common/exception.h"
#include "onebase/type/type_id.h"

namespace onebase {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                          std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void AggregationExecutor::Init() {
  result_tuples_.clear();
  cursor_ = 0;
  child_executor_->Init();

  struct AggState {
    std::vector<Value> group_vals;
    std::vector<Value> aggs;
  };

  std::unordered_map<std::string, AggState> groups;

  const auto &group_bys = plan_->GetGroupBys();
  const auto &agg_exprs = plan_->GetAggregates();
  const auto &agg_types = plan_->GetAggregateTypes();
  const auto &child_schema = child_executor_->GetOutputSchema();

  auto make_group_key = [&](const std::vector<Value> &vals) -> std::string {
    std::string key;
    for (size_t i = 0; i < vals.size(); i++) {
      if (i > 0) {
        key.push_back('|');
      }
      key += vals[i].ToString();
    }
    return key;
  };

  auto init_aggs = [&]() -> std::vector<Value> {
    std::vector<Value> aggs;
    aggs.reserve(agg_types.size());
    for (auto t : agg_types) {
      switch (t) {
        case AggregationType::CountStarAggregate:
        case AggregationType::CountAggregate:
          aggs.emplace_back(TypeId::INTEGER, 0);
          break;
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          aggs.emplace_back(TypeId::INTEGER);  // NULL integer by default
          break;
      }
    }
    return aggs;
  };

  auto update_aggs = [&](AggState &state, const Tuple &tup) {
    for (size_t i = 0; i < agg_types.size(); i++) {
      const auto type = agg_types[i];
      if (type == AggregationType::CountStarAggregate) {
        state.aggs[i] = state.aggs[i].Add(Value(TypeId::INTEGER, 1));
        continue;
      }
      const Value v = agg_exprs[i]->Evaluate(&tup, &child_schema);
      if (type == AggregationType::CountAggregate) {
        if (!v.IsNull()) {
          state.aggs[i] = state.aggs[i].Add(Value(TypeId::INTEGER, 1));
        }
        continue;
      }
      if (v.IsNull()) {
        continue;
      }
      if (type == AggregationType::SumAggregate) {
        if (state.aggs[i].IsNull()) {
          state.aggs[i] = v;
        } else {
          state.aggs[i] = state.aggs[i].Add(v);
        }
        continue;
      }
      if (type == AggregationType::MinAggregate) {
        if (state.aggs[i].IsNull() || v.CompareLessThan(state.aggs[i]).GetAsBoolean()) {
          state.aggs[i] = v;
        }
        continue;
      }
      if (type == AggregationType::MaxAggregate) {
        if (state.aggs[i].IsNull() || v.CompareGreaterThan(state.aggs[i]).GetAsBoolean()) {
          state.aggs[i] = v;
        }
        continue;
      }
    }
  };

  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    std::vector<Value> group_vals;
    group_vals.reserve(group_bys.size());
    for (const auto &expr : group_bys) {
      group_vals.push_back(expr->Evaluate(&child_tuple, &child_schema));
    }
    const std::string gk = make_group_key(group_vals);
    auto it = groups.find(gk);
    if (it == groups.end()) {
      AggState st;
      st.group_vals = std::move(group_vals);
      st.aggs = init_aggs();
      update_aggs(st, child_tuple);
      groups.emplace(gk, std::move(st));
    } else {
      update_aggs(it->second, child_tuple);
    }
  }

  // empty input special case: no group-by still returns 1 row of defaults
  if (groups.empty() && group_bys.empty()) {
    std::vector<Value> vals;
    vals.reserve(agg_types.size());
    for (auto t : agg_types) {
      switch (t) {
        case AggregationType::CountStarAggregate:
        case AggregationType::CountAggregate:
          vals.emplace_back(TypeId::INTEGER, 0);
          break;
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          vals.emplace_back(TypeId::INTEGER);  // NULL
          break;
      }
    }
    result_tuples_.emplace_back(std::move(vals));
    return;
  }

  for (auto &kv : groups) {
    auto &st = kv.second;
    std::vector<Value> out;
    out.reserve(st.group_vals.size() + st.aggs.size());
    out.insert(out.end(), st.group_vals.begin(), st.group_vals.end());
    out.insert(out.end(), st.aggs.begin(), st.aggs.end());
    result_tuples_.emplace_back(std::move(out));
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= result_tuples_.size()) {
    return false;
  }
  *tuple = result_tuples_[cursor_++];
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase
