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

#include "udp.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "dfnetwork.h"
#include "comm_include.h"
#include <poll.h>

namespace phxpaxos {

UDPRecv::UDPRecv(DFNetWork * poDFNetWork)
  : m_poDFNetWork(poDFNetWork), m_iSockFD(-1), m_bIsEnd(false), m_bIsStarted(false) {
}

UDPRecv::~UDPRecv() {
  if (m_iSockFD != -1) {
    close(m_iSockFD);
    m_iSockFD = -1;
  }
}

void UDPRecv::Stop() {
  if (m_bIsStarted) {
    m_bIsEnd = true;
    join();
  }
}

int UDPRecv::Init(const int iPort) {
  // 生成udp句柄
  if ((m_iSockFD = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return -1;
  }
  // 指定地址
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  // 开始设置参数
  addr.sin_family = AF_INET;
  addr.sin_port = htons(iPort);
  // 设置地址为ANY
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  int enable = 1;
  // 设置端口可重用
  setsockopt(m_iSockFD, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
  // 把端口绑定在addr上
  if (bind(m_iSockFD, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    return -1;
  }

  return 0;
}

void UDPRecv::run() {
  m_bIsStarted = true;

  // 64K的UDP数据包
  // 那么发送方在发送消息的时候也需要注意这个限制？
  char sBuffer[65536] = {0};

  // 实际上后面并没有用到这些参数
  // 也就是用来打印消息。这里可以把这个关掉
  struct sockaddr_in addr;
  socklen_t addr_len = sizeof(struct sockaddr_in);
  memset(&addr, 0, sizeof(addr));

  while(true) {
    if (m_bIsEnd) {
      PLHead("UDPRecv [END]");
      return;
    }

    struct pollfd fd;
    int ret;

    fd.fd = m_iSockFD;
    fd.events = POLLIN;
    // 这里会等500毫秒
    ret = poll(&fd, 1, 500);

    if (ret == 0 || ret == -1) {
      continue;
    }

    int iRecvLen = recvfrom(m_iSockFD, sBuffer, sizeof(sBuffer), 0,
                            (struct sockaddr *)&addr, &addr_len);

    //printf("recvlen %d, buffer %s client %s\n",
    //iRecvLen, sBuffer, inet_ntoa(addr.sin_addr));

    BP->GetNetworkBP()->UDPReceive(iRecvLen);

    if (iRecvLen > 0) {
      // 这里调用的是DefaultNetwork
      // DefaultNetwork接收到消息之后，会直接通过
      // DefaultNetwork->PNode::OnRecvieMessage()
      // 然后找到相应的group index
      // 再通过group index找到Group()->Instance::OnRecvieveMessage()
      // 路线略长了一点。
      m_poDFNetWork->OnReceiveMessage(sBuffer, iRecvLen);
    }
  }
}

//////////////////////////////////////////////

UDPSend::UDPSend() : m_iSockFD(-1), m_bIsEnd(false), m_bIsStarted(false) {
}

UDPSend::~UDPSend() {
  while (!m_oSendQueue.empty()) {
    // peek就是带条件变量的锁
    // 如果队列为空，那么就会一直等
    // peek取出第一个
    QueueData * poData = m_oSendQueue.peek();
    // 弹出队列
    m_oSendQueue.pop();
    delete poData;
  }
}

int UDPSend::Init() {
  // 发送方只需要创建一个fd
  if ((m_iSockFD = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return -1;
  }

  return 0;
}

void UDPSend::Stop() {
  if (m_bIsStarted) {
    m_bIsEnd = true;
    join();
  }
}

// 这是一个内部子函数
// 直接把消息发送出去
// 会被Run函数调用
void UDPSend::SendMessage(const std::string & sIP, const int iPort, const std::string & sMessage) {
  struct sockaddr_in addr;
  int addr_len = sizeof(struct sockaddr_in);
  memset(&addr, 0, sizeof(addr));
  // send给A
  // 那么这里需要填上A的地址
  addr.sin_family = AF_INET;
  addr.sin_port = htons(iPort);
  addr.sin_addr.s_addr = inet_addr(sIP.c_str());
  // 在发送的时候再指定地址
  int ret = sendto(m_iSockFD, sMessage.data(), (int)sMessage.size(), 0, (struct sockaddr *)&addr, addr_len);
  if (ret > 0) {
    BP->GetNetworkBP()->UDPRealSend(sMessage);
  }
}

// Run函数是给Thread::Start()函数使用
void UDPSend::run() {
  m_bIsStarted = true;

  while(true) {
    QueueData * poData = nullptr;

    m_oSendQueue.lock();

    bool bSucc = m_oSendQueue.peek(poData, 1000);
    if (bSucc) {
      m_oSendQueue.pop();
    }

    m_oSendQueue.unlock();

    if (poData != nullptr) {
      SendMessage(poData->m_sIP, poData->m_iPort, poData->m_sMessage);
      // 释放传进来的数据
      delete poData;
    }

    if (m_bIsEnd) {
      PLHead("UDPSend [END]");
      return;
    }
  }
}

int UDPSend::AddMessage(const std::string & sIP, const int iPort, const std::string & sMessage) {
  m_oSendQueue.lock();
  // 注意看一下queue里面的内存大小
  if ((int)m_oSendQueue.size() > UDP_QUEUE_MAXLEN) {
    BP->GetNetworkBP()->UDPQueueFull();
    //PLErr("queue length %d too long, can't enqueue", m_oSendQueue.size());
    m_oSendQueue.unlock();
    return -2;
  }

  // 生成新的数据项
  QueueData * poData = new QueueData;
  // 把数据放到队列中
  poData->m_sIP = sIP;
  poData->m_iPort = iPort;
  poData->m_sMessage = sMessage;

  m_oSendQueue.add(poData);
  m_oSendQueue.unlock();

  return 0;
}

}


