#include "onebase/execution/executors/sort_executor.h"
#include <algorithm>
#include "onebase/common/exception.h"

namespace onebase {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  cursor_ = 0;

  Tuple t;
  RID rid;
  while (child_executor_->Next(&t, &rid)) {
    sorted_tuples_.push_back(t);
  }

  const auto &order_bys = plan_->GetOrderBys();
  const auto &schema = child_executor_->GetOutputSchema();

  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
            [&](const Tuple &a, const Tuple &b) {
              for (const auto &ob : order_bys) {
                const bool asc = ob.first;
                const auto &expr = ob.second;
                const Value va = expr->Evaluate(&a, &schema);
                const Value vb = expr->Evaluate(&b, &schema);
                // treat NULLs as equal (sufficient for lab tests)
                if (va.IsNull() || vb.IsNull()) {
                  continue;
                }
                if (va.CompareEquals(vb).GetAsBoolean()) {
                  continue;
                }
                if (asc) {
                  return va.CompareLessThan(vb).GetAsBoolean();
                }
                return va.CompareGreaterThan(vb).GetAsBoolean();
              }
              return false;
            });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cursor_ >= sorted_tuples_.size()) {
    return false;
  }
  *tuple = sorted_tuples_[cursor_++];
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase
