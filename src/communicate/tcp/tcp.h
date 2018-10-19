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

#include "event_loop.h"
#include "tcp_acceptor.h"
#include "tcp_client.h"
#include "utils_include.h"

namespace phxpaxos {

class TcpRead : public Thread {
 public:
  TcpRead(NetWork * poNetWork);
  ~TcpRead();

  int Init();

  void Run();

  void Stop();

  EventLoop * GetEventLoop();

 private:
  EventLoop m_oEventLoop;
};

/////////////////////////////////////////////

class TcpWrite : public Thread {
 public:
  TcpWrite(NetWork * poNetWork);
  ~TcpWrite();

  int Init();

  void run();

  void Stop();

  // 利用TcpClient将消息发送出去
  // TcpClient发送消息的时候，会通过EventLoop将消息AddMessage
  int AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage);

 private:
  // TcpWrite就是一个发送消息方。所以需要一个TcpClient端来发送消息。
  // 而TcpWrite主要是一个包装类，里面也是包含了一下EventLoop.
  TcpClient m_oTcpClient; // ? 这个的作用是什么？
  EventLoop m_oEventLoop;
};

class TcpIOThread {
 public:
  TcpIOThread(NetWork * poNetWork);
  ~TcpIOThread();

  int Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount);

  void Start();

  void Stop();

  int AddMessage(const int iGroupIdx, const std::string & sIP, const int iPort, const std::string & sMessage);

 private:
  // 当接收到消息之后，仍然需要通过Network->OnRecieveMessage()
  // 来处理消息
  // 所以这里也需要有一个Netowrk的指针
  // 所以在构建TcpIOThread的时候，也是需要传入一个DefaultNetwork的指针
  // 在DefaultNetwork的构造函数里面会TcpIOThread(this)来构造。
  NetWork * m_poNetWork;
  // Acceptor专门用来处理系统发过来的accept请求。
  TcpAcceptor m_oTcpAcceptor;
  // 分别处理TcpRead/TcpWrite
  std::vector<TcpRead *> m_vecTcpRead;
  std::vector<TcpWrite *> m_vecTcpWrite;
  bool m_bIsStarted;
};

}
