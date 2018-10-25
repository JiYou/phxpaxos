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

#include "base.h"
#include <string>
#include "ioloop.h"
#include "msg_counter.h"

namespace phxpaxos {

class ProposerState {
 public:
  ProposerState(const Config * poConfig);
  ~ProposerState();

  void Init();

  // 在最开始工作的时候，需要设置一个proposer id
  void SetStartProposalID(const uint64_t llProposalID);

  // 生成一次新的提议，比如可能是因为某次提议没有通过
  void NewPrepare();
  // 当prepare收到很多消息之后，要开始准备后面accept的值。
  void AddPreAcceptValue(const BallotNumber & oOtherPreAcceptBallot, const std::string & sOtherPreAcceptValue);

  /////////////////////////
  // 一个简单的get/set函数
  const uint64_t GetProposalID();
  const std::string & GetValue();
  void SetValue(const std::string & sValue);
  // 记住最大的proposal id.
  void SetOtherProposalID(const uint64_t llOtherProposalID);

  // 重置第二阶段准备的accept议题
  void ResetHighestOtherPreAcceptBallot();

 public:
  uint64_t m_llProposalID;
  // 如果消息被拒绝了，那么需要记住被拒绝消息里面
  // 对方，acceptor能够接受的最大的编号。
  // 注意与m_oHighestOtherPreAcceptBallot进行区别。
  uint64_t m_llHighestOtherProposalID;
  std::string m_sValue;
  // 这里记住的是被acceptor同意的消息的最大的编号
  // 也就是为了取得最大议题的value用的。
  BallotNumber m_oHighestOtherPreAcceptBallot;

  Config * m_poConfig;
};

//////////////////////////////////////////////////

class Learner;

class Proposer : public Base {
 public:
  Proposer(
    const Config * poConfig,
    const MsgTransport * poMsgTransport,
    const Instance * poInstance,
    const Learner * poLearner,
    const IOLoop * poIOLoop);
  ~Proposer();

  void SetStartProposalID(const uint64_t llProposalID);

  virtual void InitForNewPaxosInstance();

  int NewValue(const std::string & sValue);

  bool IsWorking();

  /////////////////////////////

  void Prepare(const bool bNeedNewBallot = true);

  void OnPrepareReply(const PaxosMsg & oPaxosMsg);

  // 超时的处理
  void OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg);

  void Accept();

  void OnAcceptReply(const PaxosMsg & oPaxosMsg);

  // Accept超时的处理
  void OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg);

  void OnPrepareTimeout();

  void OnAcceptTimeout();

  void ExitPrepare();

  void ExitAccept();

  void CancelSkipPrepare();

  /////////////////////////////

  void AddPrepareTimer(const int iTimeoutMs = 0);

  void AddAcceptTimer(const int iTimeoutMs = 0);

 public:
  ProposerState m_oProposerState;
  MsgCounter m_oMsgCounter;
  Learner * m_poLearner;

  bool m_bIsPreparing;
  bool m_bIsAccepting;

  IOLoop * m_poIOLoop;

  uint32_t m_iPrepareTimerID;
  int m_iLastPrepareTimeoutMs;
  uint32_t m_iAcceptTimerID;
  int m_iLastAcceptTimeoutMs;
  uint64_t m_llTimeoutInstanceID;

  bool m_bCanSkipPrepare;

  bool m_bWasRejectBySomeone;

  TimeStat m_oTimeStat;
};

}
