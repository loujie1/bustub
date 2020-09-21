//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
: AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
    const auto Catalog = exec_ctx_->GetCatalog();
    table_heap_ = Catalog->GetTable(plan_->GetTableOid())->table_.get();
    iterator_ = std::make_unique<TableIterator>(table_heap_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple) {
    // Get the iterator.
    auto& iter = *(iterator_);

    while(iter != table_heap_->End()){
        auto tuple_ = *(iter++);
        bool eval = true;
        if(plan_->GetPredicate() != nullptr){
            eval = plan_->GetPredicate()->Evaluate(&tuple_, plan_->OutputSchema()).GetAs<bool>();
        }
        if(eval){
            *tuple = Tuple(tuple_);
            return true;
        }
    }
    return false;
}

}  // namespace bustub
