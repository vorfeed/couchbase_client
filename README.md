# Couchbase Client

A Couchbase Client for Multi-cluster Asynchronous Access implemented with C++

用C++实现的支持多Couchbase集群异步访问的客户端


## 功能

* 基于couchbase官方推荐的libcouchbase++库进行二次封装开发

* 支持多集群同时访问，每个集群可以同时请求多个key，只异步触发一次回调

* 提供同步访问接口

* 具备简单的服务降级功能


## 设计

* 以couchbase url的形式注册couchbase集群，包括集群地址、bucket、用户名、访问超时等

* 启动时对所有couchbase集群建立连接，每个连接独立线程，事件循环在该线程中操作 

* 应用层对多个集群多个key的一次访问会被拆分为多次操作，用一个堆上的计数器记录总的操作次数

* 同时在堆上保存每个client每次操作的回调数据，并在回调函数中指向这份回调数据，循环引用保证回调数据在回调前不会被析构掉

* 对每个client，在应用层一次访问的所有key都处理完后，进行回调数据的清理

* 由于回调函数本身存储在回调数据中，在回调函数中对回调数据析构会出错，因此对已回调过的数据建立一个单独的回收队列，在每次loop操作完成后在事件循环外释放回调数据

* 在应用层一次访问的所有集群的所有key都处理完成后，进行一次总的应用层回调

* 应用层回调可将后续操作丢入其他线程池中，避免阻塞couchbase事件操作

* 每个集群连接还包含一个定时器线程，定期检查couchbase的处理状况，异常比例过高时降级

* 降级策略为一段时间内不再向该集群发送请求

* 集群意外故障恢复后会尝试重连接，这部分由libcouchbase++框架实现


## 依赖库

* cmake

* gcc

* boost

* libev/libcouchbase/libcouchbase++


================================
by Xiaojie Chen (swly@live.com)

