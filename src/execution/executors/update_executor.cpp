#include "onebase/execution/executors/update_executor.h"
#include "onebase/common/exception.h"
#include "onebase/type/type_id.h"

namespace onebase {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  has_updated_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  auto *table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  if (table_info == nullptr) {
    throw OneBaseException("UpdateExecutor::Next table not found");
  }

  int32_t count = 0;
  Tuple old_tuple;
  RID old_rid;
  const auto &child_schema = child_executor_->GetOutputSchema();
  const auto &exprs = plan_->GetUpdateExpressions();
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    std::vector<Value> new_values;
    new_values.reserve(exprs.size());
    for (const auto &expr : exprs) {
      new_values.push_back(expr->Evaluate(&old_tuple, &child_schema));
    }
    Tuple new_tuple(std::move(new_values));
    table_info->table_->UpdateTuple(old_rid, new_tuple);
    count++;
  }

  *tuple = Tuple({Value(TypeId::INTEGER, count)});
  if (rid != nullptr) {
    *rid = RID{};
  }
  return true;
}

}  // namespace onebase
