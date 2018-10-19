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

#include "dfnetwork.h"
#include "udp.h"

namespace phxpaxos {

DFNetWork::DFNetWork() : m_oUDPRecv(this), m_oTcpIOThread(this) {
}

DFNetWork::~DFNetWork() {
  PLHead("NetWork Deleted!");
}

void DFNetWork::StopNetWork() {
  m_oUDPRecv.Stop();
  m_oUDPSend.Stop();
  m_oTcpIOThread.Stop();
}

// 缺省网络模块的初始化
// 做了三个工作
// - UDP发送服务
// - UDP接收服务
// - TCP服务
// iIIOThreadCount是通过Options.iIOThreadCount来进行控制的
// 与group index没有关系。
int DFNetWork::Init(const std::string & sListenIp,
                    const int iListenPort,
                    const int iIOThreadCount) {
  // 发送方的Init
  // 就是建一个socket fd
  // socket(AF_INET, SOCK_DGRAM, 0))
  int ret = m_oUDPSend.Init();
  if (ret != 0) {
    return ret;
  }

  // 接收方还需要建socket
  // 设置地址
  // bind开始见听
  ret = m_oUDPRecv.Init(iListenPort);
  if (ret != 0) {
    return ret;
  }

  ret = m_oTcpIOThread.Init(sListenIp, iListenPort, iIOThreadCount);
  if (ret != 0) {
    PLErr("m_oTcpIOThread Init fail, ret %d", ret);
    return ret;
  }

  return 0;
}

void DFNetWork::RunNetWork() {
  // 这三个类是Thread的子类
  // Thread类中有一个Start函数。这个函数会调用
  // 每个子类的Run()函数。
  // 所以这里相当于是启动了这些独立的线程。
  // 后面可以考虑一下如果用libco来处理可以怎么处理。
  m_oUDPSend.start();
  m_oUDPRecv.start();
  m_oTcpIOThread.Start();
}

int DFNetWork::SendMessageTCP(const int iGroupIdx,
                              const std::string & sIp,
                              const int iPort,
                              const std::string & sMessage) {
  return m_oTcpIOThread.AddMessage(iGroupIdx, sIp, iPort, sMessage);
}

int DFNetWork::SendMessageUDP(const int iGroupIdx,
                              const std::string & sIp,
                              const int iPort,
                              const std::string & sMessage) {
  return m_oUDPSend.AddMessage(sIp, iPort, sMessage);
}

}


