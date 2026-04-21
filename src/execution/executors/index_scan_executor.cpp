#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"

namespace onebase {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  // This educational skeleton's Catalog::IndexInfo does not store an index structure.
  // We provide a safe fallback implementation that scans the base table.
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw OneBaseException("IndexScanExecutor::Init table not found");
  }
  iter_ = table_info_->table_->Begin();
  end_ = table_info_->table_->End();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (iter_ != end_) {
    *tuple = *iter_;
    *rid = tuple->GetRID();
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace onebase
