//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "buffer/clock_replacer.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pg_latch_.lock();
  pt_latch_.lock();

  if(page_table_.find(page_id)!= page_table_.end()){
    frame_id_t target = page_table_[page_id];

    replacer_->Pin(target);
    pages_[target].pin_count_++;

    pg_latch_.unlock();
    pt_latch_.unlock();

    LOG_INFO("Fetch page %d, frame %d from buffer pool", page_id, target);
    return &pages_[target];
  }
  // Find a replacement page R
  else{
    fl_latch_.lock();
    frame_id_t target;

    if(!free_list_.empty()){
      target = free_list_.front();
      free_list_.pop_front();
    }else{
      if(!replacer_->Victim(&target)) {
        LOG_ERROR("ERROR: No victim page found");
        fl_latch_.unlock();
        pt_latch_.unlock();
        pg_latch_.unlock();
        return nullptr;
      }
    }
    fl_latch_.unlock();

    page_id_t target_page_id = pages_[target].GetPageId();

    if(pages_[target].IsDirty()){
      if(!FlushPageImpl(target_page_id)) {
        pt_latch_.unlock();
        pg_latch_.unlock();
        LOG_ERROR("Can't flush page: %d into disk", target_page_id);
        return nullptr;
      }
    }
    // Pin page and update page's metadata
    replacer_->Pin(target);
    pages_[target].pin_count_++;

    page_table_.erase(target_page_id);
    page_table_.insert({page_id, target});

    disk_manager_->ReadPage(page_id, pages_[target].data_);
    pages_[target].page_id_ = page_id;
    pages_[target].is_dirty_ = false;

    pt_latch_.unlock();
    pg_latch_.unlock();

    LOG_INFO("Fetch page %d from replacer/free list", page_id);
    return &pages_[target];
  }

  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  pt_latch_.lock();

  if(page_table_.find(page_id) == page_table_.end()){
    pt_latch_.unlock();
    return true;
  }
  pg_latch_.lock();
  frame_id_t target = page_table_[page_id];
  pt_latch_.unlock();

  if(pages_[target].GetPinCount() <= 0){
    pg_latch_.unlock();
    LOG_ERROR("Unpin page %d failed, pincnt <= 0", page_id);
    return false;
  }else {
    pages_[target].pin_count_--;
    pages_[target].is_dirty_ |= is_dirty;

    // Add into replacer if no threads working on this frame.
    if(pages_[target].GetPinCount() == 0){
      replacer_->Unpin(target);
    }
    pg_latch_.unlock();
    LOG_INFO("Unpin page %d from bf, present pin_cnt: %d", page_id, pages_[target].pin_count_);
    return true;
  }
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if(page_id == INVALID_PAGE_ID){return false;}

  if(page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  else {
    frame_id_t target = page_table_[page_id];
    // Pin the page and now working on it.
    if(!pages_[target].IsDirty()){
      LOG_INFO("Flush page %d : page is not dirty", page_id);
      return true;
    }

    disk_manager_->WritePage(page_id, pages_[target].data_);
    pages_[target].is_dirty_ = false;

    LOG_INFO("Flush page %d : page was dirty", page_id);
    return true;
  }
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  pg_latch_.lock();
  pt_latch_.lock();
  fl_latch_.lock();

 // Allocate a new page using a free frame.
  if(!free_list_.empty()){
    frame_id_t free_frame = free_list_.front();
    free_list_.pop_front();
    fl_latch_.unlock();

    *page_id = disk_manager_->AllocatePage();
    page_table_[*page_id] = free_frame;
    replacer_->Pin(free_frame);
    pages_[free_frame].ResetMemory();
    pages_[free_frame].page_id_ = *page_id;
    pages_[free_frame].pin_count_ = 1;
    pages_[free_frame].is_dirty_ = false;

    pt_latch_.unlock();
    pg_latch_.unlock();

    LOG_INFO("New page %d allocated from free list", *page_id);
    return &pages_[free_frame];
  }
  // Allocate a new page with a replacement frame evicted.
  else{
    fl_latch_.unlock();

    frame_id_t victim;
    if(!replacer_->Victim(&victim)) {
      pt_latch_.unlock();
      pg_latch_.unlock();

      LOG_ERROR("No available space in buffer pool now: No victim can be found.");
      return nullptr;
    }

    if(pages_[victim].IsDirty()){
      if(!FlushPageImpl(pages_[victim].page_id_)) {
        pt_latch_.unlock();
        pg_latch_.unlock();
        LOG_ERROR("Can't flush page: %d into disk", pages_[victim].page_id_);
        return nullptr;
      }
    }

    page_id_t victim_page_id = pages_[victim].page_id_;
    page_table_.erase(victim_page_id);

    *page_id = disk_manager_->AllocatePage();
    page_table_[*page_id] = victim;
    replacer_->Pin(victim);
    pages_[victim].ResetMemory();
    pages_[victim].page_id_ = *page_id;
    pages_[victim].pin_count_ = 1;
    pages_[victim].is_dirty_ = false;

    pt_latch_.unlock();
    pg_latch_.unlock();

    LOG_INFO("New page %d allocated from another page %d evicted", *page_id, victim_page_id);
    return &pages_[victim];
  }
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  pt_latch_.lock();

  if(page_table_.find(page_id) == page_table_.end()){
    pt_latch_.unlock();
    pg_latch_.unlock();

    LOG_INFO("Delete page: %d: succeed, page is not in buffer pool.", page_id);
    return true;
  }
  else{
    frame_id_t target = page_table_[page_id];
    pg_latch_.lock();

    // Page is in use
    if(pages_[target].GetPinCount() != 0) {
      pt_latch_.unlock();
      pg_latch_.unlock();

      LOG_ERROR("Delete page: %d: failed, someone is using the page.", page_id);
      return false;
    }else{  // Delete the page from buffer pool
      page_table_.erase(page_id);
      // disk_manager_->DeallocatePage(page_id);
      pages_[target].ResetMemory();
      pages_[target].page_id_ = INVALID_PAGE_ID;
      pages_[target].is_dirty_ = false;

      fl_latch_.lock();
      free_list_.push_back(target);

      fl_latch_.unlock();
      pg_latch_.unlock();
      pt_latch_.unlock();

      LOG_INFO("Delete page %d from buffer pool: succeed.", page_id);
      return true;
    }
  }
  return false;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  pg_latch_.lock();

  for(size_t i = 0; i < pool_size_; ++i){
    if(pages_[i].IsDirty()){
      if(!FlushPageImpl(pages_[i].page_id_)) {
        LOG_ERROR("Can't flush page: %d into disk", pages_[i].page_id_);
      }
    }
  }
  pg_latch_.unlock();
  LOG_INFO("All pages flushed.");
}

}  // namespace bustub
