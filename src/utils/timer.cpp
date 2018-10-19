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

#include "timer.h"
#include "util.h"
#include <algorithm>

namespace phxpaxos {

Timer::Timer() : m_iNowTimerID(1) {
}

Timer::~Timer() {
}

// iTimerID是被传进来，要被修改的参数
// result-value
void Timer::AddTimer(const uint64_t llAbsTime,
                     uint32_t & iTimerID) {
  return AddTimerWithType(llAbsTime, 0, iTimerID);
}

// iTimerID是被传进来，要被修改的参数
// result-value
void Timer::AddTimerWithType(const uint64_t llAbsTime,
                             const int iType,
                             uint32_t & iTimerID) {
  // 序号顺次增长
  iTimerID = m_iNowTimerID++;
  TimerObj tObj(iTimerID, llAbsTime, iType);
  // 放到堆里面，然后用push_heap来进行堆的构建
  m_vecTimerHeap.push_back(tObj);
  // 如果是从vector的头开始看，那么建的就是一个大堆
  push_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
}

// 这里返回下一次超时的时间
// 由于是最小堆，那么front()
// 元素就记录了最先超时的定时器。
// 所以这里只查看第一个定时器。
const int Timer::GetNextTimeout() const {
  if (m_vecTimerHeap.empty()) {
    return -1;
  }

  int iNextTimeout = 0;
  // 先取最接近超时的定时器。
  TimerObj tObj = m_vecTimerHeap.front();
  // 取得当前的时间
  uint64_t llNowTime = Time::GetSteadyClockMS();
  if (tObj.m_llAbsTime > llNowTime) {
    iNextTimeout = (int)(tObj.m_llAbsTime - llNowTime);
  }

  return iNextTimeout;
}

// PopTimeout并不会直接把时间弹出来
// 而是先看一下有没有超时
// 如果没有超时，是返回false
// 如果超时，那么就会把结果放到iTimerID & iType里面。
bool Timer::PopTimeout(uint32_t & iTimerID, int & iType) {
  if (m_vecTimerHeap.empty()) {
    return false;
  }

  TimerObj tObj = m_vecTimerHeap.front();
  uint64_t llNowTime = Time::GetSteadyClockMS();
  if (tObj.m_llAbsTime > llNowTime) {
    return false;
  }

  pop_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
  m_vecTimerHeap.pop_back();

  iTimerID = tObj.m_iTimerID;
  iType = tObj.m_iType;

  return true;
}

}


