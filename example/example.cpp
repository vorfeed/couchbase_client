// Copyright 2017, Xiaojie Chen (swly@live.com). All rights reserved.
// https://github.com/vorfeed/couchbase_client
//
// Use of this source code is governed by a BSD-style license
// that can be found in the License file.

#include "couchbase_client/couchbase++_clients.h"

#include <iostream>

using namespace std;
using namespace Couchbase;
using namespace cbc;

int main(int argc, char* argv[]) {
  string connstr1("couchbase://10.10.10.1:1608;10.10.10.2:1608;10.10.10.3:1608/bucket1?username=user1&operation_timeout=0.02");
  string connstr2("couchbase://10.10.20.1:1608;10.10.20.2:1608;10.10.20.3:1608/bucket2?username=user2&operation_timeout=0.02");
  CouchbaseClients manager({ { "client1", connstr1 }, { "client2", connstr2 } });
  if (!manager.Start()) {
    cout << "manager start failed!" << endl;
    exit(EXIT_FAILURE);
  }
  const int kCount = 10;
  std::vector<std::pair<std::string, std::string>> kvs1, kvs2;
  for (int i = 0; i < kCount; ++i) {
    kvs1.emplace_back("key1_" + to_string(i), "value1_" + to_string(i));
    kvs2.emplace_back("key2_" + to_string(i), "value2_" + to_string(i));
  }
  manager.AsyncSet({ { "client1", kvs1 }, { "client2", kvs2 } }, [](SetResult& result) {
    int suc = 0;
    for (const auto& client_result : result) {
      for (const auto& key_status : client_result.second) {
        if (key_status.second) {
          ++suc;
        }
      }
    }
    cout << "set, succeed: " << suc << ", total: " << kCount * 2 << endl;
  });
  int index = 0, count = 0;
  int suc1 = 0, suc2 = 0, total = 0;
  while (++count) {
    bool b = manager.AsyncGet({
      {
        "client1",
        { "key1_" + to_string(index++ % kCount) }
      },
      {
        "client2",
        { "key2_" + to_string(index++ % kCount) }
      }
    }, [&suc1, &suc2, &total](GetResult& result) {
      for (const auto& client_result : result) {
        for (const auto& key_status : client_result.second) {
          if (key_status.second.first) {
            if (key_status.first.substr(0, 5) == "key1_") {
              ++suc1;
            } else if (key_status.first.substr(0, 5) == "key2_") {
              ++suc2;
            }
            if (key_status.first.substr(5) != key_status.second.second.substr(7)) {
              cout << "invalid, key; " << key_status.first << ", value: " << key_status.second.second << endl;
            }
          }
          ++total;
          if (total % 10000 == 0) {
            cout << "get, client1 succeed: " << suc1 <<
                ", client2 succeed: " << suc2 << ", total: " << total << endl;
          }
        }
      }
    });
    if (!b) {
      cout << "degradation mode, client1 succeed: " << suc1 <<
          ", client2 succeed: " << suc2 << ", total: " << total << endl;
      sleep(1);
    }
  }
}
