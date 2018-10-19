/*
Tencent is pleased to support the open source community by making
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company.
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may
not use this file except in compliance with the License. You may
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

See the AUTHORS file for names of contributors.
*/

#pragma once

#include <map>
#include <sys/epoll.h>
#include "timer.h"
#include "notify.h"

namespace phxpaxos {

#define MAX_EVENTS 1024

class Event;
class TcpAcceptor;
class TcpClient;
class MessageEvent;
class NetWork;

// 干看EventLoop实际上很没有意思。这里需要需求是如何导出EventLoop的。
// 1. 可能EventLoop一开始就只是完成一个epoll/epoll_wait之类的操作。
//    那么就需要提供两方面的接口。
//    map <fd, event数据结构> 
//    epoll_wait返回的时候，只需要去map中查一下当初注册的是哪一个接口。
//    然后看一下应该读数据还是写入数据。
//    其中epoll_wait会返回<fd, event_type>
//    从效率着想，最好是这里的epoll_wait能够返回一个指针
//         *ptr = &struct{fd, Event*}
//    通过这种方式可以减少去map中的查询时间
//    但是在这里并没有通过这种实现方式，而是
//    从epoll_wait返回之后<fd, event_type>
///     pEvent = map.find(fd);
//      if event_type == IN: pEvent->read();
//      if event_type == OUT: pEvent->write();
// 2. 从Eventloop的接口上来说，主要是有两方面的需求
//    a. event已经有了，fd也有了。比如Notify()
//       那么就是通过Event::AddEvent()->EventLoop::ModEvent()
//       添加到epoll中
//    b. 只专进来了<fd, socket address>
//       创建MessageEvent, 然后通过Event::AddEvent()
//       添加到epoll系统中
// 3. Timer, 如果epoll_wait没有返回，那么就会一直等下去。虽然也会可以
//    设置timout。但是更希望的是，如果监听某些事件，或者说连接超时，希望
//    有一定的处理办法。这个时候就需要将 timer与event进行关联。
//    但是这里并没有使用map<timer, pEvent>
//    而是使用map<timer_id, fd>
//    map<fd, pEvent>
//    这种效率并不高!!!
//    应该是需要优化的。
//    这两个表将timer_id与event联系在一起。
//    当timer超时的时候，就找到相应的event进行处理。
// StartLoop
// 于是在start_loop中主要就是做两方面的事情。
// 1. 处理超时
//    pEvent的超时，这里只有如下操作：
//     a. 从timer_id -> file_id中移除
//        而<fd, event>可能还在epoll中继续处理
//     b. 让event自己处理timeout()
//        - 如果event要reconnnect, 那么调用connect函数。
// 2. OneLoop() -> epoll_wait
//    处理fd中可能到来的各种事件
class EventLoop {
 public:
  EventLoop(NetWork * poNetWork);
  virtual ~EventLoop();

  int Init(const int iEpollLength);

  void ModEvent(const Event * poEvent, const int iEvents);

  void RemoveEvent(const Event * poEvent);

  void StartLoop();

  void Stop();

  void OnError(const int iEvents, Event * poEvent);

  virtual void OneLoop(const int iTimeoutMs);

 public:
  void SetTcpClient(TcpClient * poTcpClient);

  void JumpoutEpollWait();

 public:
  bool AddTimer(const Event * poEvent, const int iTimeout, const int iType, uint32_t & iTimerID);

  void RemoveTimer(const uint32_t iTimerID);

  void DealwithTimeout(int & iNextTimeout);

  void DealwithTimeoutOne(const uint32_t iTimerID, const int iType);

 public:
  void AddEvent(int iFD, SocketAddress oAddr);

  void CreateEvent();

  void ClearEvent();

  int GetActiveEventCount();

 public:
  typedef struct EventCtx {
    Event * m_poEvent;
    int m_iEvents;
  } EventCtx_t;

 private:
  bool m_bIsEnd;

 protected:
  // 这两个和epoll相关
  int m_iEpollFd;
  epoll_event m_EpollEvents[MAX_EVENTS];
  // event map.
  // int表示socket fd
  std::map<int, EventCtx_t> m_mapEvent;
  // 这两个都是由TcpIOThread给
  NetWork * m_poNetWork;
  // 有的消息是需要call m_poTcpClient.DealWithWrite().
  TcpClient * m_poTcpClient;

  Notify * m_poNotify;

 protected:
  Timer m_oTimer;
  std::map<uint32_t, int> m_mapTimerID2FD;

  std::queue<std::pair<int, SocketAddress> > m_oFDQueue;
  std::mutex m_oMutex;
  std::vector<MessageEvent *> m_vecCreatedEvent;
};

}
