//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"


namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  clock_hand = 0;
  buffer_size = num_pages;
  in_ = std::vector<bool>(num_pages, 0);
  ref_ = std::vector<bool>(num_pages, 0);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  latch_.lock();

  while(Size() > 0){
      clock_hand = clock_hand % buffer_size;
      if(in_[clock_hand] && !ref_[clock_hand]){
          in_[clock_hand] = false;
          *frame_id = clock_hand;
          clock_hand++;
          latch_.unlock();
          return true;
      }else if(ref_[clock_hand]){
          ref_[clock_hand] = false;
          clock_hand++;
      }else{
          clock_hand++;
      }
  }

  latch_.unlock();
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  latch_.lock();
  in_[frame_id] = false;

  latch_.unlock();
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  latch_.lock();
  ref_[frame_id] = true;
  in_[frame_id] = true;

  latch_.unlock();
}

size_t ClockReplacer::Size() {
    size_t counter = 0;
    for(size_t i = 0; i < buffer_size; ++i){
        if(in_[i]){
            counter++;
        }
    }
    return counter;
}

}  // namespace bustub
