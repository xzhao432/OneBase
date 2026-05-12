#include "onebase/execution/executors/hash_join_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                    std::unique_ptr<AbstractExecutor> left_executor,
                                    std::unique_ptr<AbstractExecutor> right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void HashJoinExecutor::Init() {
  hash_table_.clear();
  result_tuples_.clear();
  cursor_ = 0;

  left_executor_->Init();
  Tuple left_tuple;
  RID left_rid;
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &left_key_expr = plan_->GetLeftKeyExpression();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    const auto key = left_key_expr->Evaluate(&left_tuple, &left_schema);
    hash_table_[key.ToString()].push_back(left_tuple);
  }

  right_executor_->Init();
  Tuple right_tuple;
  RID right_rid;
  const auto &right_schema = right_executor_->GetOutputSchema();
  const auto &right_key_expr = plan_->GetRightKeyExpression();

  while (right_executor_->Next(&right_tuple, &right_rid)) {
    const auto key = right_key_expr->Evaluate(&right_tuple, &right_schema);
    auto it = hash_table_.find(key.ToString());
    if (it == hash_table_.end()) {
      continue;
    }
    for (const auto &lt : it->second) {
      std::vector<Value> vals;
      vals.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
        vals.push_back(lt.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
        vals.push_back(right_tuple.GetValue(&right_schema, i));
      }
      result_tuples_.emplace_back(std::move(vals));
    }
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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
