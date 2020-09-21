//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    left_(std::move(left)),
    right_(std::move(right)),
    jht_("HashTable", exec_ctx->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_)
    {}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
//    const HT *GetJHT() const { return &jht_; }

void HashJoinExecutor::Init() {
    Tuple tuple;
    left_->Init();
    // Phase 1: Build.
    while(left_->Next(&tuple)){
        hash_t hash_key = HashValues(&tuple, left_->GetOutputSchema(), plan_->GetLeftKeys());
        jht_.Insert(exec_ctx_->GetTransaction(), hash_key, tuple);
    }

    right_->Init();
}

bool HashJoinExecutor::Next(Tuple *tuple) {
    Tuple tuple_;
    auto left_schema = left_->GetOutputSchema();
    auto right_schema = right_->GetOutputSchema();
    while(right_->Next(&tuple_)){
        std::vector<Tuple> tuples;
        //Phase 2: Probe.
        hash_t  hash_key = HashValues(&tuple_, right_schema, plan_->GetRightKeys());
        jht_.GetValue(exec_ctx_->GetTransaction(), hash_key, &tuples);

        for(auto tup : tuples){
            if(plan_->Predicate()->EvaluateJoin(&tup, left_schema, &tuple_, right_schema).GetAs<bool>()){
                std::vector<Value> output_values;
                size_t num_cols = plan_->OutputSchema()->GetColumnCount();
                for(size_t i = 0; i < num_cols; ++i){
                    output_values.push_back(plan_->OutputSchema()->GetColumn(i)
                    .GetExpr()->EvaluateJoin(&tup, left_schema, &tuple_, right_schema));
                }
                *tuple = Tuple(output_values, plan_->OutputSchema());
                return true;
            }
        }
    }
    return false;
}
}  // namespace bustub
