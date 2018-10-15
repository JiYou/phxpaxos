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

#include "phxpaxos/node.h"
#include "pnode.h"

namespace phxpaxos {

int Node::RunNode(const Options & oOptions, Node *& poNode) {
  // InsideOptions::Instance()
  // 是Singleton设计模式

  // 这里根据传进来的参数，看一下是不是会有较大数据Case的情况
  // 如果数据量太大，那么需要打开大buffer模式
  if (oOptions.bIsLargeValueMode) {
    InsideOptions::Instance()->SetAsLargeBufferMode();
  }
  // 设置
  InsideOptions::Instance()->SetGroupCount(oOptions.iGroupCount);

  // 先把原来的node置空
  poNode = nullptr;
  // network也没有
  NetWork * poNetWork = nullptr;

  // PhxPaxos会在程序运行的一些关键位置调用这些断点函数
  // 开发者可以自行实现这些函数，比如可以用来监控或统计上报。
  Breakpoint::m_poBreakpoint = nullptr;
  BP->SetInstance(oOptions.poBreakpoint);

  PNode * poRealNode = new PNode();
  int ret = poRealNode->Init(oOptions, poNetWork);
  if (ret != 0) {
    delete poRealNode;
    return ret;
  }

  //step1 set node to network
  //very important, let network on recieve callback can work.
  poNetWork->m_poNode = poRealNode;

  //step2 run network.
  //start recieve message from network, so all must init before this step.
  //must be the last step.
  poNetWork->RunNetWork();


  poNode = poRealNode;

  return 0;
}

}


