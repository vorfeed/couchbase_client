// Copyright 2017, Xiaojie Chen (swly@live.com). All rights reserved.
// https://github.com/vorfeed/couchbase_client
//
// Use of this source code is governed by a BSD-style license
// that can be found in the License file.

#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

namespace Couchbase {

class Client;

}

namespace cbc {

typedef std::unordered_map<std::string, std::unordered_map<std::string, std::pair<bool ,std::string>>> GetResult;
typedef std::function<void (GetResult&)> GetCallback;
typedef std::unordered_map<std::string, std::unordered_map<std::string, bool>> SetResult;
typedef std::function<void (SetResult&)> SetCallback;

class CouchbaseClients {
 public:
  static CouchbaseClients& GetInstance(const std::vector<std::pair<std::string, std::string>>& connstrs = {});

  CouchbaseClients(const std::vector<std::pair<std::string, std::string>>& connstrs);

  bool Start();

  void Stop();

  bool AsyncSet(const std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>& keys, SetCallback callback);

  bool AsyncGet(const std::vector<std::pair<std::string, std::vector<std::string>>>& keys, GetCallback callback);

  std::pair<bool, std::future<SetResult>> Set(const std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>& keys);

  std::pair<bool, std::future<GetResult>> Get(const std::vector<std::pair<std::string, std::vector<std::string>>>& keys);

 private:
  struct TimerThread {
    TimerThread() : work(io_service), thread([this] {
      while (running_) {
        try {
          io_service.run();
        } catch (...) {
          io_service.reset();
        }
      }
    }) {}
    ~TimerThread() {
      running_ = false;
      io_service.stop();
      thread.join();
    }
    boost::asio::io_service io_service;
    boost::asio::io_service::work work;
    std::thread thread;
   private:
    bool running_ { true };
  };

  class HealthCollector {
   public:
    HealthCollector(TimerThread& timer_thread, double threshold = 0.5, int bucket_interval_s = 1,
        int recover_interval_s = 10, int minimal_request = 10) :
      timer_(timer_thread.io_service), threshold_(threshold), bucket_interval_s_(std::max(bucket_interval_s, 1)),
      recover_interval_s_(recover_interval_s), minimal_request_(std::max(minimal_request, 1)) {}

    bool Collect();

    void Update(bool succeed) {
      succeed ? ++succeed_ : ++failed_;
    }

    bool Heathy() const {
      return !degradation_;
    }

   private:
    boost::asio::deadline_timer timer_;
    double threshold_;
    int bucket_interval_s_;
    int recover_interval_s_;
    int minimal_request_;
    int degradation_ { 0 };
    std::atomic_int succeed_, failed_;
  };

  struct CouchbaseData;

  struct ClientData;

  typedef std::unique_lock<std::mutex> Lock;

  bool running_ { true };
  TimerThread timer_thread_;
  std::unordered_map<std::string, std::shared_ptr<ClientData>> clients_;
};

}
