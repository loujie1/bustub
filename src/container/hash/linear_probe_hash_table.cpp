//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <strhash.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "common/logger.h"
namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,     // Is num_buckets # of blocks or buckets?
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
        auto header_page_p = buffer_pool_manager_->NewPage(&header_page_id_);
        header_page_p->WLatch();
        auto header_page_t = reinterpret_cast<HashTableHeaderPage*>(header_page_p->GetData());
        header_page_t->SetPageId(header_page_id_);
        header_page_t->SetSize(num_buckets* BLOCK_ARRAY_SIZE);

        // Allocate Block pages
        for(size_t i = 0; i < num_buckets; ++i){
            page_id_t block_page_id;
            buffer_pool_manager_->NewPage((&block_page_id));
            if(block_page_id == INVALID_PAGE_ID) { --i; continue; }
            header_page_t->AddBlockPageId(block_page_id);
            buffer_pool_manager_->UnpinPage(block_page_id, false);
        }

        header_page_p->WUnlatch();
        buffer_pool_manager_->UnpinPage(header_page_id_, true);
    }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
    table_latch_.RLock();
    // Fetch the header page.
    auto header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
    header_page_p->RLatch();
    auto header_page_t = reinterpret_cast<HashTableHeaderPage* >(header_page_p->GetData());

    // Get the index, bucket index and block index for this key.
    size_t index, bucket_ind, block_ind;
    GetIndex(key, header_page_t->NumBlocks(), index, block_ind, bucket_ind);

    page_id_t block_page_id = header_page_t->GetBlockPageId(block_ind);
    auto block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
    block_page_p->RLatch();
    auto block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());

    while(block_page_t->IsOccupied(bucket_ind)){
        if(!comparator_(key, block_page_t->KeyAt(bucket_ind))&& block_page_t->IsReadable(bucket_ind)){
            result->push_back(block_page_t->ValueAt(bucket_ind));
        }
        bucket_ind++;

        // If go back to the original bucket, break.
        if(block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index) break;
        if(bucket_ind == BLOCK_ARRAY_SIZE){
            // Searching in this block page is finished.
            block_page_p->RUnlatch();
            buffer_pool_manager_->UnpinPage(block_page_id, false);

            block_ind++;
            bucket_ind = 0;
            if(block_ind == header_page_t->NumBlocks()){
                block_ind = 0;
            }

            // Fetch the next block page.
            block_page_id = header_page_t->GetBlockPageId(block_ind);
            block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
            block_page_p->RLatch();
            block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());
        }
    }

        return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
    table_latch_.RLock();
    auto header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
    header_page_p->RLatch();
    auto header_page_t = reinterpret_cast<HashTableHeaderPage* >(header_page_p->GetData());

    size_t index, bucket_ind, block_ind;
    GetIndex(key, header_page_t->NumBlocks(), index, block_ind, bucket_ind);

    page_id_t block_page_id = header_page_t->GetBlockPageId(block_ind);
    auto block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
    block_page_p->WLatch();
    auto block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());

    while(!block_page_t->Insert(bucket_ind, key, value)){
        // If there is already an identical <k,v> pair, insertion is to be terminated.
        if(!comparator_(key, block_page_t->KeyAt(bucket_ind)) && value == block_page_t->ValueAt(bucket_ind)){
            block_page_p->WUnlatch();
            header_page_p->RUnlatch();
            buffer_pool_manager_->UnpinPage(header_page_id_, false);
            buffer_pool_manager_->UnpinPage(block_page_id, false);
            table_latch_.RUnlock();
            return false;
        }

        bucket_ind++;

        // Got back to the original bucket, the hash table is full and need to be RESIZED.
        if(block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index){
            block_page_p->WUnlatch();
            header_page_p->RUnlatch();
            buffer_pool_manager_->UnpinPage(header_page_id_, false);
            buffer_pool_manager_->UnpinPage(block_page_id, false);
            Resize(header_page_t->NumBlocks()* BLOCK_ARRAY_SIZE);

            header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
            header_page_p->RLatch();
            header_page_t = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());

            GetIndex(key, header_page_t->NumBlocks(), index, block_ind, bucket_ind);

            block_page_id = header_page_t->GetBlockPageId(block_ind);
            block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
            block_page_p->WLatch();
            block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());
        }

        if(bucket_ind == BLOCK_ARRAY_SIZE){
            block_page_p->WUnlatch();
            buffer_pool_manager_->UnpinPage(block_page_id, false);

            block_ind++;
            bucket_ind = 0;
            if(block_ind == header_page_t->NumBlocks()){
                block_ind = 0;
            }

            // Fetch the next block page.
            block_page_id = header_page_t->GetBlockPageId(block_ind);
            block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
            block_page_p->WLatch();
            block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());
        }
    }

    block_page_p->WUnlatch();
    header_page_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, true);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.RUnlock();
    return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {

    table_latch_.RLock();
    auto header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
    header_page_p->RLatch();
    auto header_page_t = reinterpret_cast<HashTableHeaderPage* >(header_page_p->GetData());

    size_t index, bucket_ind, block_ind;
    GetIndex(key, header_page_t->NumBlocks(), index, block_ind, bucket_ind);

    page_id_t block_page_id = header_page_t->GetBlockPageId(block_ind);
    auto block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
    block_page_p->WLatch();
    auto block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());

    bool removed = false;
    while(block_page_t->IsOccupied(bucket_ind)){
        if(block_page_t->IsReadable(bucket_ind) && !comparator_(key, block_page_t->KeyAt(bucket_ind))
        && value == block_page_t->ValueAt(bucket_ind)){

            block_page_t->Remove(bucket_ind);
            block_page_p->WUnlatch();
            header_page_p->RUnlatch();
            buffer_pool_manager_->UnpinPage(block_page_id, true);
            buffer_pool_manager_->UnpinPage(header_page_id_, false);
            return true;
        }
        bucket_ind++;

        if(block_ind * BLOCK_ARRAY_SIZE + bucket_ind == index) break;
        if(bucket_ind == BLOCK_ARRAY_SIZE){
            // Searching in this block page is finished.
            block_page_p->WUnlatch();
            buffer_pool_manager_->UnpinPage(block_page_id, false);

            block_ind++;
            bucket_ind = 0;
            if(block_ind == header_page_t->NumBlocks()){
                block_ind = 0;
            }

            // Fetch the next block page.
            block_page_id = header_page_t->GetBlockPageId(block_ind);
            block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
            block_page_p->WLatch();
            block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());
        }
    }

    block_page_p->WUnlatch();
    header_page_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(block_page_id, false);
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.RUnlock();
    return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
    table_latch_.RLock();

    page_id_t old_header_page_id = header_page_id_;
    auto new_header_page_p = buffer_pool_manager_->NewPage(&header_page_id_);
    new_header_page_p->WLatch();
    auto new_header_page_t = reinterpret_cast<HashTableHeaderPage*>(new_header_page_p->GetData());
    new_header_page_t->SetPageId(header_page_id_);
    size_t num_buckets = 2 * initial_size / BLOCK_ARRAY_SIZE;
    new_header_page_t->SetSize(num_buckets * BLOCK_ARRAY_SIZE);

    for(size_t i = 0; i < num_buckets; ++i){
        page_id_t block_page_id;
        buffer_pool_manager_->NewPage((&block_page_id));
        if(block_page_id == INVALID_PAGE_ID) { --i; continue; }
        new_header_page_t->AddBlockPageId(block_page_id);
        buffer_pool_manager_->UnpinPage(block_page_id, false);
    }

    auto old_header_page_p = buffer_pool_manager_->NewPage((&old_header_page_id));
    old_header_page_p->RLatch();
    auto old_header_page_t = reinterpret_cast<HashTableHeaderPage*>(old_header_page_p->GetData());

    for(size_t block_ind = 0; block_ind < initial_size / BLOCK_ARRAY_SIZE; ++block_ind){
        page_id_t block_page_id = old_header_page_t->GetBlockPageId(block_ind);
        auto block_page_p = buffer_pool_manager_->FetchPage(block_page_id);
        block_page_p->RLatch();
        auto block_page_t = reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page_p->GetData());

        for(size_t bucket_ind = 0; bucket_ind < BLOCK_ARRAY_SIZE; ++bucket_ind){
            if(block_page_t->IsReadable(bucket_ind)) {
                KeyType key = block_page_t->KeyAt(bucket_ind);
                ValueType value = block_page_t->ValueAt(bucket_ind);
                Insert(nullptr, key, value);
            }
        }
        block_page_p->RUnlatch();
        buffer_pool_manager_->UnpinPage(block_page_id, false);
        buffer_pool_manager_->DeletePage(block_page_id);
    }

    old_header_page_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_header_page_id, false);
    buffer_pool_manager_->DeletePage(old_header_page_id);
    new_header_page_p->WUnlatch();
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.WUnlock();
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
    table_latch_.RLock();
    auto header_page_p = buffer_pool_manager_->FetchPage(header_page_id_);
    header_page_p->RLatch();
    auto header_page_t = reinterpret_cast<HashTableHeaderPage *>(header_page_p->GetData());

    size_t size = header_page_t->GetSize();
    header_page_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(header_page_id_, false);
    table_latch_.RUnlock();
    return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::GetIndex(const KeyType &key, const size_t &numBlocks,size_t &index, size_t &block_ind, size_t &bucket_ind){
    index = hash_fn_.GetHash(key) % (numBlocks * BLOCK_ARRAY_SIZE);
    block_ind = index / BLOCK_ARRAY_SIZE;
    bucket_ind = index % BLOCK_ARRAY_SIZE;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
