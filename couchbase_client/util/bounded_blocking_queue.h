// Copyright 2017, Xiaojie Chen (swly@live.com). All rights reserved.
// https://github.com/vorfeed/couchbase_client
//
// Use of this source code is governed by a BSD-style license
// that can be found in the License file.

#pragma once

#include <mutex>
#include <condition_variable>

#include <boost/circular_buffer.hpp>

namespace cbc {

template <typename Task>
class BoundedBlockingQueue {
 public:
  BoundedBlockingQueue(size_t max_size) : queue_(max_size) {}

  BoundedBlockingQueue(const BoundedBlockingQueue&) = delete;
  BoundedBlockingQueue& operator=(const BoundedBlockingQueue&) = delete;

  void Put(Task&& task) {
    Lock lock(mutex_);
    not_full_.wait(lock, [this] { return !queue_.full(); });
    queue_.push_back(std::move(task));
    lock.unlock();
    not_empty_.notify_one();
  }

  Task Take() {
    Lock lock(mutex_);
    not_empty_.wait(lock, [this] { return !queue_.empty(); });
    Task task(std::move(queue_.front()));
    queue_.pop_front();
    lock.unlock();
    not_full_.notify_one();
    return std::move(task);
  }

  size_t Size() {
    Lock lock(mutex_);
    return queue_.size();
  }

 private:
  typedef std::unique_lock<std::mutex> Lock;

  std::mutex mutex_;
  std::condition_variable not_empty_, not_full_;
  boost::circular_buffer<Task> queue_;
};

}
