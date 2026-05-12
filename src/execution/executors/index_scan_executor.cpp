#include "onebase/execution/executors/index_scan_executor.h"
#include "onebase/common/exception.h"
#include "onebase/execution/expressions/abstract_expression.h"
#include "onebase/storage/table/table_heap.h"
#include "onebase/storage/table/tuple.h"
#include "onebase/type/type_id.h"
#include "onebase/type/value.h"

#include <mutex>
#include <unordered_map>
#include <vector>

namespace onebase {

namespace {

// Fallback full-table scan state lives only in this .cpp so the header stays unchanged.
struct IndexScanFallbackState {
  TableHeap::Iterator iter{nullptr, RID(INVALID_PAGE_ID, 0)};
  TableHeap::Iterator end{nullptr, RID(INVALID_PAGE_ID, 0)};
};

std::mutex g_index_scan_fallback_mu;
std::unordered_map<const IndexScanExecutor *, IndexScanFallbackState> g_index_scan_fallback;

auto MaterializeFromHeapTuple(TableInfo *table_info, Tuple *tuple, const RID &current_rid) -> void {
  const auto &schema = table_info->schema_;
  std::vector<Value> materialized;
  materialized.reserve(schema.GetColumnCount());
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    materialized.push_back(tuple->GetValue(&schema, i));
  }
  *tuple = Tuple(std::move(materialized));
  tuple->SetRID(current_rid);
}

}  // namespace

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw OneBaseException("IndexScanExecutor::Init table not found");
  }
  index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  matching_rids_.clear();
  cursor_ = 0;

  {
    std::lock_guard<std::mutex> lock(g_index_scan_fallback_mu);
    g_index_scan_fallback.erase(this);
  }

  if (index_info_ != nullptr && index_info_->SupportsPointLookup() && plan_->GetLookupKey() != nullptr) {
    const Value key_val = plan_->GetLookupKey()->Evaluate(nullptr, nullptr);
    if (!key_val.IsNull() && key_val.GetTypeId() == TypeId::INTEGER) {
      if (const auto *rids = index_info_->LookupInteger(key_val.GetAsInteger()); rids != nullptr) {
        matching_rids_ = *rids;
      }
    }
    return;
  }

  IndexScanFallbackState st;
  st.iter = table_info_->table_->Begin();
  st.end = table_info_->table_->End();
  std::lock_guard<std::mutex> lock(g_index_scan_fallback_mu);
  g_index_scan_fallback[this] = std::move(st);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_info_ != nullptr && index_info_->SupportsPointLookup() && plan_->GetLookupKey() != nullptr) {
    while (cursor_ < matching_rids_.size()) {
      const RID current_rid = matching_rids_[cursor_++];
      *tuple = table_info_->table_->GetTuple(current_rid);
      if (rid != nullptr) {
        *rid = current_rid;
      }
      const auto &predicate = plan_->GetPredicate();
      if (predicate != nullptr) {
        const auto pred_val = predicate->Evaluate(tuple, &table_info_->schema_);
        if (!pred_val.GetAsBoolean()) {
          continue;
        }
      }
      MaterializeFromHeapTuple(table_info_, tuple, current_rid);
      return true;
    }
    return false;
  }

  std::lock_guard<std::mutex> lock(g_index_scan_fallback_mu);
  auto it = g_index_scan_fallback.find(this);
  if (it == g_index_scan_fallback.end()) {
    return false;
  }
  auto &st = it->second;
  while (st.iter != st.end) {
    *tuple = *st.iter;
    const RID current_rid = tuple->GetRID();
    if (rid != nullptr) {
      *rid = current_rid;
    }
    ++st.iter;
    const auto &predicate = plan_->GetPredicate();
    if (predicate != nullptr) {
      const auto pred_val = predicate->Evaluate(tuple, &table_info_->schema_);
      if (!pred_val.GetAsBoolean()) {
        continue;
      }
    }
    MaterializeFromHeapTuple(table_info_, tuple, current_rid);
    return true;
  }
  g_index_scan_fallback.erase(it);
  return false;
}

}  // namespace onebase
