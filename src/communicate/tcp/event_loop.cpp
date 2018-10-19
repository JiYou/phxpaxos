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

#include "event_loop.h"
#include "event_base.h"
#include "tcp_acceptor.h"
#include "tcp_client.h"
#include "comm_include.h"
#include "message_event.h"
#include "phxpaxos/network.h"

using namespace std;

namespace phxpaxos {

EventLoop::EventLoop(NetWork * poNetWork) {
  m_iEpollFd = -1;
  m_bIsEnd = false;
  m_poNetWork = poNetWork;
  m_poTcpClient = nullptr;
  m_poNotify = nullptr;
  // 清空epoll事件表
  memset(m_EpollEvents, 0, sizeof(m_EpollEvents));
}

EventLoop::~EventLoop() {
  // ClearEvent只是释放EventList中可以Destroy的那部分Event.
  ClearEvent();
}

// 跳出Epoll等待，就是发一个通知。
void EventLoop::JumpoutEpollWait() {
  m_poNotify->SendNotify();
}

// 这个只有在TcpWrite类里面是使用了的。
void EventLoop::SetTcpClient(TcpClient * poTcpClient) {
  m_poTcpClient = poTcpClient;
}

// 初始化就是创建epoll fd
// 然后利用Notify生成一个pipe[2]的数组
// 然后把pipe[0]添加到epoll里面。
int EventLoop::Init(const int iEpollLength) {
  // iEpollLength是需要监听的fd的数目
  // 20480
  // 所有的epoll都这个数值
  m_iEpollFd = epoll_create(iEpollLength);
  // 如果创建失败
  if (m_iEpollFd == -1) {
    PLErr("epoll_create fail, ret %d", m_iEpollFd);
    return -1;
  }
  // Notify就是个pipe
  m_poNotify = new Notify(this); // ！！this指向这个epoll的EventLoop.
  assert(m_poNotify != nullptr);
  // 初始化linux pipe
  int ret = m_poNotify->Init();
  // 会把pipe[0]添加到fd中。前面初始化的时候,m_poNotify就记录了
  // EventLoop，所以可以得到相应的epoll_fd.
  if (ret != 0) {
    return ret;
  }

  return 0;
}

// 这里实际上就是通过poEvent找到相应的FD
// 然后把这个fd添加到epoll里面。
// iEvent是epoll要监听的动作。
// 这里需要注意epoll_ctl
// 与epoll_wait里面的event标志是不一样的。
// EPOLL_CTL_XXX表示控制信息
// EPOLLOUT没有CTL则表示监听消息。
// 所以这个函数，简单地说，就是给一个fd添加相应的监听事件。
// map只是用来看一下这个事件是不是已经添加在里面了。
void EventLoop::ModEvent(const Event * poEvent, const int iEvents) {
  // 根据Event的FD找到相应的pos.
  auto it = m_mapEvent.find(poEvent->GetSocketFd());
  int iEpollOpertion = 0;
  if (it == end(m_mapEvent)) {
    // 如果没有找到这个socket fd
    // 那么就需要添加到epoll表中
    iEpollOpertion = EPOLL_CTL_ADD;
  } else {
    // 这里再看一下相应的event还在不在
    // 如果在，那么就是修改
    // 如果不在，那么就需要添加
    // 这里代码应该改成
    // AddTimer添加进来的iEvents就是0
    iEpollOperation = it->second.m_iEvents ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    // 并且如果发现iEvent一样，实际上没有必要处理。
  }

  // 这里用操作系统提供的epoll操作接口
  // 按照epoll event的格式来准备数据。
  // EPOLLIN：表示对应的文件描述符可以读；
  // EPOLLOUT：表示对应的文件描述符可以写；
  // EPOLLPRI：表示对应的文件描述符有紧急的数可读；

  // EPOLLERR：表示对应的文件描述符发生错误；
  // EPOLLHUP：表示对应的文件描述符被挂断；
  // EPOLLET：    ET的epoll工作模式；
  epoll_event tEpollEvent;
  tEpollEvent.events = iEvents;
  tEpollEvent.data.fd = poEvent->GetSocketFd(); // 指向要添加的socket.

  // 添加到epoll里面。
  // TODO: 这里直接MOD也是不对的。最好是需要比较一下
  // 是不是与原来的一样，如果与原来的一样，那么就不需要MOD
  // 直接返回就可以了。
  int ret = epoll_ctl(m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd(), &tEpollEvent);
  if (ret == -1) {
    PLErr("epoll_ctl fail, EpollFd %d EpollOpertion %d SocketFd %d EpollEvent %d",
          m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd(), iEvents);
    //to do
    // 这里添加失败之后，什么也不做。
    return;
  }

  EventCtx tCtx;
  tCtx.m_poEvent = (Event *)poEvent;
  tCtx.m_iEvents = iEvents;

  // 不管三七二十一，直接更新
  // 这里实际上是可以优化的。
  // 如果已经存在，可以直接用前面find的结果
  // 如果不存在，那么才需要添加。
  m_mapEvent[poEvent->GetSocketFd()] = tCtx;
}

// 这个函数就是通过fd
// 然后在epoll_ctl
// 里面把这个fd关联的监听事件删除掉。
void EventLoop::RemoveEvent(const Event * poEvent) {
  auto it = m_mapEvent.find(poEvent->GetSocketFd());
  if (it == end(m_mapEvent)) {
    return;
  }

  int iEpollOpertion = EPOLL_CTL_DEL;

  epoll_event tEpollEvent;
  tEpollEvent.events = 0;
  tEpollEvent.data.fd = poEvent->GetSocketFd();

  int ret = epoll_ctl(m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd(), &tEpollEvent);
  if (ret == -1) {
    PLErr("epoll_ctl fail, EpollFd %d EpollOpertion %d SocketFd %d",
          m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd());

    // to do
    // when error
    // TODO: 删除失败的时候应该怎么办？
    return;
  }

  m_mapEvent.erase(poEvent->GetSocketFd());
}

void EventLoop::StartLoop() {
  m_bIsEnd = false;
  while(true) {
    BP->GetNetworkBP()->TcpEpollLoop();

    // iNextTimeout = 1000这里并没有什么实际作用。
    // 在DealWithTimeout里面会修改这个值
    int iNextTimeout = 1000;
    DealwithTimeout(iNextTimeout);

    //PLHead("nexttimeout %d", iNextTimeout);

    OneLoop(iNextTimeout);

    CreateEvent();

    if (m_poTcpClient != nullptr) {
      m_poTcpClient->DealWithWrite();
    }

    if (m_bIsEnd) {
      PLHead("TCP.EventLoop [END]");
      break;
    }
  }
}

void EventLoop::Stop() {
  m_bIsEnd = true;
}

// OneLoop里面也没有用到iTimeoutMs这个参数，可以考虑删除掉
void EventLoop::OneLoop(const int iTimeoutMs) {
  int n = epoll_wait(m_iEpollFd, m_EpollEvents, MAX_EVENTS, 1);
  if (n == -1) {
    if (errno != EINTR) {
      PLErr("epoll_wait fail, errno %d", errno);
      return;
    }
  }

  for (int i = 0; i < n; i++) {
    int iFd = m_EpollEvents[i].data.fd;
    auto it = m_mapEvent.find(iFd);
    if (it == end(m_mapEvent)) {
      continue;
    }

    int iEvents = m_EpollEvents[i].events;
    Event * poEvent = it->second.m_poEvent;

    int ret = 0;
    if (iEvents & EPOLLERR) {
      OnError(iEvents, poEvent);
      continue;
    }

    try {
      // 有数据进来
      if (iEvents & EPOLLIN) {
        ret = poEvent->OnRead();
      }
      // 有数据需要写出去
      if (iEvents & EPOLLOUT) {
        ret = poEvent->OnWrite();
      }
    } catch (...) {
      ret = -1;
    }

    if (ret != 0) {
      OnError(iEvents, poEvent);
    }
  }
}

// 处理相应的Socket/Event出错的情况
// 如果这个Event可以被删除，那么就清除掉
void EventLoop::OnError(const int iEvents, Event * poEvent) {
  BP->GetNetworkBP()->TcpOnError();

  PLErr("event error, events %d socketfd %d socket ip %s errno %d",
        iEvents, poEvent->GetSocketFd(), poEvent->GetSocketHost().c_str(), errno);

  RemoveEvent(poEvent);

  bool bNeedDelete = false;
  poEvent->OnError(bNeedDelete);

  if (bNeedDelete) {
    poEvent->Destroy();
  }
}

bool EventLoop::AddTimer(const Event * poEvent,
                         const int iTimeout,
                         const int iType,
                         uint32_t & iTimerID) {
  // 如果没有FD
  if (poEvent->GetSocketFd() == 0) {
    return false;
  }

  // 如果map中还没有这个event
  if (m_mapEvent.find(poEvent->GetSocketFd()) == end(m_mapEvent)) {
    // 那么创建一个
    EventCtx tCtx;
    tCtx.m_poEvent = (Event *)poEvent;
    tCtx.m_iEvents = 0;
    // iEvent为0的时候，在底层OS那里会自动加上EPOLL_ERROR|EPOLL_HUB
    // FD -> Event上下文的映射
    m_mapEvent[poEvent->GetSocketFd()] = tCtx;
  }

  uint64_t llAbsTime = Time::GetSteadyClockMS() + iTimeout;
  // 添加相应的定时器
  m_oTimer.AddTimerWithType(llAbsTime, iType, iTimerID);

  // 把TimerID与FD进行映射
  m_mapTimerID2FD[iTimerID] = poEvent->GetSocketFd();

  return true;
}

void EventLoop::RemoveTimer(const uint32_t iTimerID) {
  auto it = m_mapTimerID2FD.find(iTimerID);
  if (it != end(m_mapTimerID2FD)) {
    m_mapTimerID2FD.erase(it);
  }
}

// 已经发现超时了，那么开始接收这个超时情况。
// 根据timerID -> file fd
// 然后再根据fd找到events
// 再根据event处理OnTimeout
void EventLoop::DealwithTimeoutOne(const uint32_t iTimerID, const int iType) {
  // 通过超时器的ID，找到fd
  auto it = m_mapTimerID2FD.find(iTimerID);
  // 如果没有找到
  if (it == end(m_mapTimerID2FD)) {
    // TODO:
    // 这种情况应该说是不必要存在的吧？
    // 如果存在，那么如何检测？
    // 两者相关联的情况，一个有，一个不存在了。
    //PLErr("Timeout aready remove!, timerid %u iType %d", iTimerID, iType);
    return;
  }

  // 找到相应的socket file handler.
  int iSocketFd = it->second;
  // 删除这个item
  m_mapTimerID2FD.erase(it);

  // 然后再找到相应的Event.
  // 注意Timer要删除
  // 但是Event并不删除
  auto eventIt = m_mapEvent.find(iSocketFd);
  // 如果没有找到，那么返回
  if (eventIt == end(m_mapEvent)) {
    // 这种情况是正常的还是不正常的？
    // TODO:
    return;
  }

  // 如果找到，那么这里直接处理OnTimeOut
  // 这里如果是发现超时。那么根据type
  // 如果type是reconnect
  // 那么会自动发起重连。
  eventIt->second.m_poEvent->OnTimeout(iTimerID, iType);
}

// 前面在调用的时候，这个timeout设置为1000
// 也就是一秒。
void EventLoop::DealwithTimeout(int & iNextTimeout) {
  bool bHasTimeout = true;

  while(bHasTimeout) {
    uint32_t iTimerID = 0;
    int iType = 0;
    // PopTimeout()
    // 并不会直接弹，只有发现超时了，才会把TimerID和iType弹出来。
    bHasTimeout = m_oTimer.PopTimeout(iTimerID, iType);

    if (bHasTimeout) {
      // 如果有超时，那么开始处理这个超时。
      DealwithTimeoutOne(iTimerID, iType);

      // GetNextTimeOut返回的是下一次距离超时还有多久
      // 如果是一个正数，那么超时就还有很久
      // 所以这里需要跳出循环
      // GetNextTimeout()只是获取这个值，并且弹出这个Timer
      iNextTimeout = m_oTimer.GetNextTimeout();
      if (iNextTimeout != 0) {
        break;
      }
    }
  }
}

void EventLoop::AddEvent(int iFD, SocketAddress oAddr) {
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);
  m_oFDQueue.push(make_pair(iFD, oAddr));
}

// 通过一个队列加到m_vecCreatedEvent中。
// CreateEvent是创建Event
void EventLoop::CreateEvent() {
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  if (m_oFDQueue.empty()) {
    return;
  }
  // 去掉没用的。就是event->IsDestroy()为true的
  // event.
  ClearEvent();

  int iCreatePerTime = 200;
  // 把Q中都弹出来，准备创建，并且最多一次
  while ((!m_oFDQueue.empty()) && iCreatePerTime--) {
    auto oData = m_oFDQueue.front();
    m_oFDQueue.pop();

    //create event for this fd
    MessageEvent * poMessageEvent = new MessageEvent(MessageEventType_RECV,
        oData.first, // fd
        oData.second, // SocketAddress.
        this,
        m_poNetWork);
    // 创建的时候采用的是EPOLLIN
    // 这里会将event添加到epoll和map[fd->event]中。
    poMessageEvent->AddEvent(EPOLLIN);

    m_vecCreatedEvent.push_back(poMessageEvent);
  }
}

void EventLoop::ClearEvent() {
  for (auto it = m_vecCreatedEvent.begin(); it != end(m_vecCreatedEvent);) {
    // (*it)是一个MessageEvent*
    if ((*it)->IsDestroy()) {
      delete (*it);
      it = m_vecCreatedEvent.erase(it); // 注意用法：删除之后自动++
    } else {
      it++;
    }
  }
}

int EventLoop::GetActiveEventCount() {
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);
  ClearEvent(); // 如果这里经常调用，效率可能会不高
  return (int)m_vecCreatedEvent.size();
}

}


