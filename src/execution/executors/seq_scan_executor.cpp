#include "onebase/execution/executors/seq_scan_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw OneBaseException("SeqScanExecutor::Init table not found");
  }
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_info_ == nullptr) {
    return false;
  }
  while (iter_ != end_) {
    *tuple = *iter_;
    *rid = tuple->GetRID();
    ++iter_;
    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      const auto pred_val = predicate->Evaluate(tuple, &table_info_->schema_);
      if (!pred_val.GetAsBoolean()) {
        continue;
      }
    }
    return true;
  }
  return false;
}

}  // namespace onebase
