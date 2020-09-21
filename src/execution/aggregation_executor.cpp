//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
    aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
    child_->Init();

    // Build Aggregation Hash Table.
    aht_.GenerateInitialAggregateValue();
    Tuple tuple;
    while(child_->Next(&tuple)){
        auto key = MakeKey(&tuple);
        auto value = MakeVal(&tuple);
        aht_.InsertCombine(key, value);
    }
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple) {
    while(aht_iterator_ != aht_.End()){
        auto group_bys = aht_iterator_.Key().group_bys_;
        auto aggregates = aht_iterator_.Val().aggregates_;
        if(!plan_->GetHaving()
        || plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()){
            std::vector<Value> out_values;
            size_t num_cols = plan_->OutputSchema()->GetColumnCount();
            for(size_t i = 0; i < num_cols; ++i){
                out_values.push_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(group_bys, aggregates));
            }

            *tuple = Tuple(out_values, plan_->OutputSchema());
            ++aht_iterator_;
            return true;
        }else{
            ++aht_iterator_;
        }
    }
    return false;
}

}  // namespace bustub
