//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)}{}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {
    SimpleCatalog *Catalog = exec_ctx_->GetCatalog();
    table_MetaData_= Catalog->GetTable(plan_->TableOid());

    if(!plan_->IsRawInsert()){
        child_executor_->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
    RID rid;
    bool inserted;
    Tuple tup_to_Insert;

    if(plan_->IsRawInsert()){
        size_t size_cols = plan_->RawValues().size();
        for(size_t i = 0; i < size_cols; ++i){
            tup_to_Insert= Tuple(plan_->RawValuesAt(i), &table_MetaData_->schema_);

            LOG_DEBUG("begin next.");
            inserted = table_MetaData_->table_->InsertTuple(tup_to_Insert, &rid, exec_ctx_->GetTransaction());
            if(!inserted) {
                return false;
            };
        }
    }
    // Get tuple from child_executor and insert it into table.
    else{
        while(child_executor_->Next(&tup_to_Insert)){
            inserted = table_MetaData_->table_->InsertTuple(tup_to_Insert, &rid, exec_ctx_->GetTransaction());
            if(!inserted) {
                return false;

            }
        }
    }
    return true;
}

}  // namespace bustub
