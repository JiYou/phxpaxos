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

#include <vector>
#include <inttypes.h>

namespace phxpaxos {

// 整个类的设计思想：
// 用一个最小堆来记录一系列定时器。
// 每次检查的时候，就检查第一个元素。
// 注意make_heap()
// push_heap()
// pop_heap()
// 这三个函数的使用。
// 1. 
// 默认的情况下这三个函数构建的都是最大堆。
// 但是TimerObj的比较函数 <
// 在实现的时候，用的是obj.time < this.time
// 那么构建出来的堆就是最小堆。
// 2.
// push_heap(),使用方式是:
//   vs.push_back(x)
//   push_heap(vs.begin(), vs.end());
// 3.
// pop_heap()的使用方式是
//   pop_heap(vs.begin(), vs.end());
//   vs.pop_back();

class Timer {
 public:
  Timer();
  ~Timer();

  void AddTimer(const uint64_t llAbsTime,
                uint32_t & iTimerID);

  void AddTimerWithType(const uint64_t llAbsTime,
                        const int iType,
                        uint32_t & iTimerID);

  bool PopTimeout(uint32_t & iTimerID,
                  int & iType);

  const int GetNextTimeout() const;

 private:
  struct TimerObj {
    TimerObj(uint32_t iTimerID,
             uint64_t llAbsTime,
             int iType)
      : m_iTimerID(iTimerID),
      m_llAbsTime(llAbsTime),
      m_iType(iType) {
      }

    // Timer ID
    uint32_t m_iTimerID;
    uint64_t m_llAbsTime;
    int m_iType;

    // 注意这个特别扯淡的函数。
    // 正常情况下的<应该是
    // m_llAbsTime < obj.m_llAbsTime
    // 但是这里用的是
    // obj.m_llAbsTime < m_llAbsTime
    // 这样就把小于变成了大于的结果。
    // NOTE:
    // - 如果是采用m_llAbsTime < obj.m_llAbsTime
    //   那么建出来的堆就是最大堆
    // - 这里反过来比较，那么建出来的堆就是最小堆。
    //   在这种情况下，后面的Timer就是最小堆
    bool operator < (const TimerObj & obj) const {
      // 如果时间一样，就看ID
      if (obj.m_llAbsTime == m_llAbsTime) {
        return obj.m_iTimerID < m_iTimerID;
      // 否则就比较时间大小
      } else {
        return obj.m_llAbsTime < m_llAbsTime;
      }
    }
  };

 private:
  uint32_t m_iNowTimerID;
  std::vector<TimerObj> m_vecTimerHeap;
};

}
