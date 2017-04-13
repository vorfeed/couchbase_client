// Copyright 2017, Xiaojie Chen (swly@live.com). All rights reserved.
// https://github.com/vorfeed/couchbase_client
//
// Use of this source code is governed by a BSD-style license
// that can be found in the License file.

#include <algorithm>
#include <unordered_set>

#include <libcouchbase/couchbase++.h>

#include "couchbase_client/util/bounded_blocking_queue.h"
#include "couchbase++_clients.h"

namespace cbc {

struct CouchbaseClients::CouchbaseData {
  CouchbaseData(std::shared_ptr<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>> set,
      const std::vector<std::pair<std::string, std::string>>& set_kvs) : is_set(true), set(set), set_kvs(set_kvs) {}
  CouchbaseData(std::shared_ptr<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>> get,
      const std::vector<std::string>& get_keys) : is_set(false), get(get), get_keys(get_keys) {}
  bool is_set;
  std::shared_ptr<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>> set;
  std::shared_ptr<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>> get;
  std::vector<std::pair<std::string, std::string>> set_kvs;
  std::vector<std::string> get_keys;
};

struct CouchbaseClients::ClientData {
  ClientData(const std::string& client_name, TimerThread& timer_thread) :
      client(client_name), health_collector(timer_thread) {}
  Couchbase::Client client;
  std::thread thread;
  HealthCollector health_collector;
  BoundedBlockingQueue<CouchbaseData> queue { 1024 };
  std::unordered_set<std::shared_ptr<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>>> set_recycle;
  std::unordered_set<std::shared_ptr<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>>> get_recycle;
};

bool CouchbaseClients::HealthCollector::Collect() {
  timer_.expires_from_now(boost::posix_time::seconds(bucket_interval_s_));
  timer_.async_wait([this](const boost::system::error_code& ec) {
    if (degradation_--) {
      Collect();
      return;
    }
    degradation_ = 0;
    int failed = failed_;
    int succeed = succeed_;
    int total = failed + succeed;
    if (total > minimal_request_) {
      double failed_ratio = failed * 1.0 / total;
      if (failed_ratio > threshold_) {
        degradation_ = std::max(recover_interval_s_ / bucket_interval_s_, 1);
      }
    }
    failed_ = succeed_ = 0;
  });
  return true;
}

CouchbaseClients& CouchbaseClients::GetInstance(const std::vector<std::pair<std::string, std::string>>& connstrs) {
  static CouchbaseClients couchbase_clients(connstrs);
  return couchbase_clients;
}

CouchbaseClients::CouchbaseClients(const std::vector<std::pair<std::string, std::string>>& connstrs) {
  for (const std::pair<std::string, std::string>& connstr : connstrs) {
    clients_.emplace(connstr.first, std::make_shared<ClientData>(connstr.second + "&readj_ts_wait=0", timer_thread_));
  }
}

bool CouchbaseClients::Start() {
  for (auto& client : clients_) {
    Couchbase::Status status = client.second->client.connect();
    if (!status.success()) {
      std::cerr << "connect to " << client.first << " failed: " << status << std::endl;
      return false;
    }
    std::cout << "connect to " << client.first << " succeed." << std::endl;
    client.second->thread = std::thread([this, &client] {
      auto& client_data = client.second;
      while (running_) {
        const size_t kMaxOnce = 64;
        size_t queue_size = client_data->queue.Size();
        size_t count = std::min(queue_size, kMaxOnce);
        while (count--) {
          CouchbaseData couchbase_data(std::move(client_data->queue.Take()));
          if (couchbase_data.is_set) {
            for (const auto& kv : couchbase_data.set_kvs) {
              couchbase_data.set->add(kv.first, kv.second);
            }
            couchbase_data.set->submit();
          } else {
            for (const std::string& key : couchbase_data.get_keys) {
              couchbase_data.get->add(key);
            }
            couchbase_data.get->submit();
          }
        }
        client_data->client.wait();
        client_data->set_recycle.clear();
        client_data->get_recycle.clear();
        if (queue_size <= kMaxOnce) {
          usleep(300);
        }
      }
    });
    client.second->health_collector.Collect();
  }
  return true;
}

void CouchbaseClients::Stop() {
  running_ = false;
  for (auto& client : clients_) {
    client.second->thread.join();
  }
}

bool CouchbaseClients::AsyncSet(const std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>& keys,
    SetCallback callback) {
  std::shared_ptr<std::atomic_int> shared_counter = std::make_shared<std::atomic_int>(std::accumulate(keys.begin(), keys.end(), 0,
      [](int current, const std::pair<std::string, std::vector<std::pair<std::string, std::string>>>& element) {
    return current + element.second.size();
  }));
  std::shared_ptr<SetResult> shared_result = std::make_shared<SetResult>();
  bool ret = true;
  for (const auto& client_keys : keys) {
    const std::string& client_name = client_keys.first;
    auto ite_client = clients_.find(client_name);
    if (ite_client == clients_.end()) {
      std::cerr << "client " << client_name << " doesn't exits!" << std::endl;
      continue;
    }
    auto& client_data = *clients_[client_name];
    if (!client_data.health_collector.Heathy()) {
      ret = false;
      *shared_counter -= client_keys.second.size();
      continue;
    }
    auto& client = client_data.client;
    struct SetHolder { std::shared_ptr<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>> hold; };
    std::shared_ptr<SetHolder> shared_holder = std::make_shared<SetHolder>();
    std::shared_ptr<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>> set =
        std::make_shared<Couchbase::CallbackCommand<Couchbase::UpsertCommand, Couchbase::StoreResponse>>(client,
        [this, &client_data, shared_holder, client_name, shared_counter, shared_result, callback](Couchbase::StoreResponse& resp) {
      bool succeed = resp.status() || resp.status().isDataError();
      (*shared_result)[client_name].emplace(resp.key(), succeed);
      clients_[client_name]->health_collector.Update(succeed);
      if (!--*shared_counter) {
        callback(*shared_result);
        client_data.set_recycle.emplace(shared_holder->hold);
        shared_holder->hold.reset();
        return;
      }
      client_data.set_recycle.emplace(shared_holder->hold);
      shared_holder->hold.reset();
    });
    shared_holder->hold = set;
    clients_[client_name]->queue.Put(std::move(CouchbaseData(set, client_keys.second)));
  }
  return ret;
}

bool CouchbaseClients::AsyncGet(const std::vector<std::pair<std::string, std::vector<std::string>>>& keys,
    GetCallback callback) {
  std::shared_ptr<std::atomic_int> shared_counter = std::make_shared<std::atomic_int>(std::accumulate(keys.begin(), keys.end(), 0,
      [](int current, const std::pair<std::string, std::vector<std::string>>& element) {
    return current + element.second.size();
  }));
  std::shared_ptr<GetResult> shared_result = std::make_shared<GetResult>();
  bool ret = true;
  for (const auto& client_keys : keys) {
    const std::string& client_name = client_keys.first;
    auto ite_client = clients_.find(client_name);
    if (ite_client == clients_.end()) {
      std::cerr << "client " << client_name << " doesn't exits!" << std::endl;
      continue;
    }
    auto& client_data = *ite_client->second;
    if (!client_data.health_collector.Heathy()) {
      ret = false;
      *shared_counter -= client_keys.second.size();
      continue;
    }
    auto& client = ite_client->second->client;
    struct GetHolder { std::shared_ptr<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>> hold; };
    std::shared_ptr<GetHolder> shared_holder = std::make_shared<GetHolder>();
    std::shared_ptr<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>> get =
        std::make_shared<Couchbase::CallbackCommand<Couchbase::GetCommand, Couchbase::GetResponse>>(client,
        [this, &client_data, shared_holder, client_name, shared_counter, shared_result, callback](Couchbase::GetResponse& resp) {
      bool succeed = resp.status() || resp.status().isDataError();
      (*shared_result)[client_name].emplace(resp.key(), std::make_pair(succeed, resp.value()));
      clients_[client_name]->health_collector.Update(succeed);
      if (!--*shared_counter) {
        callback(*shared_result);
        client_data.get_recycle.emplace(shared_holder->hold);
        shared_holder->hold.reset();
        return;
      }
      client_data.get_recycle.emplace(shared_holder->hold);
      shared_holder->hold.reset();
    });
    shared_holder->hold = get;
    clients_[client_name]->queue.Put(std::move(CouchbaseData(get, client_keys.second)));
  }
  return ret;
}

std::pair<bool, std::future<SetResult>> CouchbaseClients::Set(const std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>& keys) {
  std::shared_ptr<std::promise<SetResult>> p(std::make_shared<std::promise<SetResult>>());
  bool ret = AsyncSet(keys, [p](SetResult& result) {
    p->set_value(std::move(result));
  });
  return std::make_pair(ret, p->get_future());
}

std::pair<bool, std::future<GetResult>> CouchbaseClients::Get(const std::vector<std::pair<std::string, std::vector<std::string>>>& keys) {
  std::shared_ptr<std::promise<GetResult>> p(std::make_shared<std::promise<GetResult>>());
  bool ret = AsyncGet(keys, [p](GetResult& result) {
    p->set_value(std::move(result));
  });
  return std::make_pair(ret, p->get_future());
}

}
