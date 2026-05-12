#include "onebase/execution/executors/seq_scan_executor.h"
#include "onebase/common/exception.h"
#include "onebase/storage/table/tuple.h"
#include "onebase/type/value.h"
#include <vector>

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
    const auto &schema = table_info_->schema_;
    std::vector<Value> materialized;
    materialized.reserve(schema.GetColumnCount());
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      materialized.push_back(tuple->GetValue(&schema, i));
    }
    *tuple = Tuple(std::move(materialized));
    tuple->SetRID(*rid);
    return true;
  }
  return false;
}

}  // namespace onebase
