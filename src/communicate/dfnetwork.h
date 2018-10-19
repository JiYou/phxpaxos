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

#include <string>
#include "udp.h"
#include "tcp.h"
#include "phxpaxos/network.h"

namespace phxpaxos {

// 实现Network的接口，收发消息
// 真正的工作由UDPRecv & UDPSend
// TcpIOThread这三个类来处理。
// 这里只是进行了一下包装
class DFNetWork : public NetWork {
 public:
  DFNetWork();
  virtual ~DFNetWork();

  // 在抽象类中并没有提供这个Init方法
  int Init(const std::string & sListenIp,
           const int iListenPort,
           const int iIOThreadCount);

  void RunNetWork();

  void StopNetWork();

  int SendMessageTCP(const int iGroupIdx,
                     const std::string & sIp,
                     const int iPort,
                     const std::string & sMessage);

  int SendMessageUDP(const int iGroupIdx,
                     const std::string & sIp,
                     const int iPort,
                     const std::string & sMessage);
  // OnRecieveMessage() 这个函数在
  // Network类里面已经设置好了，收到消息直接交给PaxosNode处理
  // 所以在这个子类里面并没有处理消息的部分。
 private:
  // 接收到消息之后，是需要将消息进行处理一下。
  // 而在DefaultNetwork里面是将消息直接写死交给PNode*来处理
  // 在UDPRecv的构造函数里面是需要写清楚Network是谁？
  // 所以在DefaultNetwork构造UDPRecv的时候，需要将this指针给
  // UDPRecv的构造函数。
  UDPRecv m_oUDPRecv;
  UDPSend m_oUDPSend;
  // Tcp接收到消息之后一，仍然是需要交给OnRecieveMessage
  // 这个入口仍然是在DefaultNetwork里面
  // 所以TcpIOThread的构造函数也会指向DefaultNetwork
  TcpIOThread m_oTcpIOThread;
};

}
