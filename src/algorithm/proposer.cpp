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

#include "proposer.h"
#include "learner.h"
#include "phxpaxos/sm.h"
#include "instance.h"

namespace phxpaxos {

ProposerState::ProposerState(const Config * poConfig) {
  m_poConfig = (Config *)poConfig;
  m_llProposalID = 1;
  Init();
}

ProposerState::~ProposerState() {
}

void ProposerState::Init() {
  m_llHighestOtherProposalID = 0;
  m_sValue.clear();
}

void ProposerState:: SetStartProposalID(const uint64_t llProposalID) {
  m_llProposalID = llProposalID;
}

void ProposerState::NewPrepare() {
  PLGHead("START ProposalID %lu HighestOther %lu MyNodeID %lu",
          m_llProposalID, m_llHighestOtherProposalID, m_poConfig->GetMyNodeID());

  uint64_t llMaxProposalID =
    m_llProposalID > m_llHighestOtherProposalID ? m_llProposalID : m_llHighestOtherProposalID;

  m_llProposalID = llMaxProposalID + 1;

  PLGHead("END New.ProposalID %lu", m_llProposalID);

}

void ProposerState::AddPreAcceptValue(
  const BallotNumber & oOtherPreAcceptBallot,
  const std::string & sOtherPreAcceptValue) {
  PLGDebug("OtherPreAcceptID %lu OtherPreAcceptNodeID %lu HighestOtherPreAcceptID %lu "
           "HighestOtherPreAcceptNodeID %lu OtherPreAcceptValue %zu",
           oOtherPreAcceptBallot.m_llProposalID, oOtherPreAcceptBallot.m_llNodeID,
           m_oHighestOtherPreAcceptBallot.m_llProposalID, m_oHighestOtherPreAcceptBallot.m_llNodeID,
           sOtherPreAcceptValue.size());

  if (oOtherPreAcceptBallot.isnull()) {
    return;
  }

  if (oOtherPreAcceptBallot > m_oHighestOtherPreAcceptBallot) {
    m_oHighestOtherPreAcceptBallot = oOtherPreAcceptBallot;
    m_sValue = sOtherPreAcceptValue;
  }
}

const uint64_t ProposerState::GetProposalID() {
  return m_llProposalID;
}

const std::string & ProposerState::GetValue() {
  return m_sValue;
}

void ProposerState::SetValue(const std::string & sValue) {
  m_sValue = sValue;
}

void ProposerState::SetOtherProposalID(const uint64_t llOtherProposalID) {
  if (llOtherProposalID > m_llHighestOtherProposalID) {
    m_llHighestOtherProposalID = llOtherProposalID;
  }
}

void ProposerState::ResetHighestOtherPreAcceptBallot() {
  m_oHighestOtherPreAcceptBallot.reset();
}

////////////////////////////////////////////////////////////////

Proposer::Proposer(
  const Config * poConfig,
  const MsgTransport * poMsgTransport,
  const Instance * poInstance,
  const Learner * poLearner,
  const IOLoop * poIOLoop)
  : Base(poConfig, poMsgTransport, poInstance), m_oProposerState(poConfig), m_oMsgCounter(poConfig) {
  m_poLearner = (Learner *)poLearner;
  m_poIOLoop = (IOLoop *)poIOLoop;

  m_bIsPreparing = false;
  m_bIsAccepting = false;

  m_bCanSkipPrepare = false;

  InitForNewPaxosInstance();

  m_iPrepareTimerID = 0;
  m_iAcceptTimerID = 0;
  m_llTimeoutInstanceID = 0;

  m_iLastPrepareTimeoutMs = m_poConfig->GetPrepareTimeoutMs();
  m_iLastAcceptTimeoutMs = m_poConfig->GetAcceptTimeoutMs();

  m_bWasRejectBySomeone = false;
}

Proposer::~Proposer() {
}

void Proposer::SetStartProposalID(const uint64_t llProposalID) {
  m_oProposerState.SetStartProposalID(llProposalID);
}

void Proposer::InitForNewPaxosInstance() {
  m_oMsgCounter.StartNewRound();
  m_oProposerState.Init();

  ExitPrepare();
  ExitAccept();
}

bool Proposer::IsWorking() {
  return m_bIsPreparing || m_bIsAccepting;
}

// 这里开始准备propose新的提议
int Proposer::NewValue(const std::string & sValue) {
  BP->GetProposerBP()->NewProposal(sValue);

  // 如果当前这个propose的状态里面，值还没有确定。
  // 那么就设置为调用者的值
  // 否则就是使用propose状态里面的旧值。
  if (m_oProposerState.GetValue().size() == 0) {
    m_oProposerState.SetValue(sValue);
  }

  // 虽然这里应该是走到Prepare的，但是这里有可能跳过。
  // 所以这里还是设置了Accept超时
  // prepare timeout
  m_iLastPrepareTimeoutMs = START_PREPARE_TIMEOUTMS;
  // accept timeout
  m_iLastAcceptTimeoutMs = START_ACCEPT_TIMEOUTMS;

  // 如果可以跳过prepare
  // 并且没有被其他的节点拒绝过
  // 那么就直接走到accept阶段
  // . 本节点之前已经执行过Prepare阶段，并且Prepare阶段的执行结果为Accept。
  // 要满瞳这个条件才可以跳过prepare
  if (m_bCanSkipPrepare && !m_bWasRejectBySomeone) {
    BP->GetProposerBP()->NewProposalSkipPrepare();

    PLGHead("skip prepare, directly start accept");
    Accept();
  } else {
    // 如果没有被人拒绝过，那么就没有必要自增编号
    //if not reject by someone, no need to increase ballot
    Prepare(m_bWasRejectBySomeone);
  }

  return 0;
}

void Proposer::ExitPrepare() {
  if (m_bIsPreparing) {
    m_bIsPreparing = false;

    m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
  }
}

void Proposer::ExitAccept() {
  if (m_bIsAccepting) {
    m_bIsAccepting = false;

    m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
  }
}

void Proposer::AddPrepareTimer(const int iTimeoutMs) {
  // 这里添加定时器
  // 选把以前的定时器删除掉
  if (m_iPrepareTimerID > 0) {
    m_poIOLoop->RemoveTimer(m_iPrepareTimerID);
  }

  // 如果超时的时间是大于0
  if (iTimeoutMs > 0) {
    // 那么添加这个定时器
    m_poIOLoop->AddTimer(
      iTimeoutMs,
      Timer_Proposer_Prepare_Timeout,
      m_iPrepareTimerID);
    return;
  }

  // 否则添加一个默认的时间
  m_poIOLoop->AddTimer(
    m_iLastPrepareTimeoutMs,
    Timer_Proposer_Prepare_Timeout,
    m_iPrepareTimerID);

  m_llTimeoutInstanceID = GetInstanceID();

  PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

  // 采用指数的方式来增加timeout时间
  m_iLastPrepareTimeoutMs *= 2;
  if (m_iLastPrepareTimeoutMs > MAX_PREPARE_TIMEOUTMS) {
    m_iLastPrepareTimeoutMs = MAX_PREPARE_TIMEOUTMS;
  }
}

void Proposer::AddAcceptTimer(const int iTimeoutMs) {
  if (m_iAcceptTimerID > 0) {
    m_poIOLoop->RemoveTimer(m_iAcceptTimerID);
  }

  if (iTimeoutMs > 0) {
    m_poIOLoop->AddTimer(
      iTimeoutMs,
      Timer_Proposer_Accept_Timeout,
      m_iAcceptTimerID);
    return;
  }

  m_poIOLoop->AddTimer(
    m_iLastAcceptTimeoutMs,
    Timer_Proposer_Accept_Timeout,
    m_iAcceptTimerID);

  m_llTimeoutInstanceID = GetInstanceID();

  PLGHead("timeoutms %d", m_iLastPrepareTimeoutMs);

  m_iLastAcceptTimeoutMs *= 2;
  if (m_iLastAcceptTimeoutMs > MAX_ACCEPT_TIMEOUTMS) {
    m_iLastAcceptTimeoutMs = MAX_ACCEPT_TIMEOUTMS;
  }
}

void Proposer::Prepare(const bool bNeedNewBallot) {
  PLGHead("START Now.InstanceID %lu MyNodeID %lu State.ProposalID %lu State.ValueLen %zu",
          GetInstanceID(), m_poConfig->GetMyNodeID(), m_oProposerState.GetProposalID(),
          m_oProposerState.GetValue().size());

  BP->GetProposerBP()->Prepare();
  // 这里进行时间的打点，返回值是距离上次打点的时间差。
  m_oTimeStat.Point();

  ExitAccept();
  m_bIsPreparing = true;
  m_bCanSkipPrepare = false;
  m_bWasRejectBySomeone = false;

  // 这里是把m_oProposerState里面的
  // m_oHighestOtherPreAcceptBallot
  // m_llProposalID nodeID都设置为0
  // struct ProposerState {
  //   uint64_t m_llProposalID;             // 后面自己要使用的编号
  //   uint64_t m_llHighestOtherProposalID; // 已知的acceptor的最大编号
  //   std::string m_sValue;
  //   BallotNumber m_oHighestOtherPreAcceptBallot;
  // }
  // 因为这里要开始新的prepare
  // 之前拿到的其他节点的结果都不能再使用了。
  // 所以直接清空。
  m_oProposerState.ResetHighestOtherPreAcceptBallot();
  if (bNeedNewBallot) {
    // uint64_t llMaxProposalID = std::max(m_llProposalID, m_llHighestOtherProposalID);
    // m_llProposalID = llMaxProposalID + 1;
    m_oProposerState.NewPrepare();
  }
  // 如果不生成新的balllot
  // 那么m_oProposerState.m_llProposalID就没有任何更改


  PaxosMsg oPaxosMsg;
  oPaxosMsg.set_msgtype(MsgType_PaxosPrepare);
  oPaxosMsg.set_instanceid(GetInstanceID());
  oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());

  // 接收到消息的各种状态直接清空
  // 比如记录了哪些投了赞成票
  // 哪些投了拒绝票，这些以前的信息都要清空掉
  m_oMsgCounter.StartNewRound();

  // 这里生成一个定时器
  AddPrepareTimer();

  PLGHead("END OK");
  // 开始广播消息给其他各个结点
  BroadcastMessage(oPaxosMsg);
}

// 注意：这里并没有收集所有的节点的反馈消息
// 只是收到了一个消息。所以需要判断三种情况：
// - 是否提议得到了通过?
// - 是否提议被拒绝了
// - 是否继续等待接收到后面的消息？
void Proposer::OnPrepareReply(const PaxosMsg & oPaxosMsg) {
  PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
          oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(),
          oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

  BP->GetProposerBP()->OnPrepareReply();

  // 还处在preparing队段不？
  // 当收到这个消息的时候，有可能已经走到下一步了
  // 因为消息来得实在是太慢了。
  // 所以这里直接丢弃这个消息。
  if (!m_bIsPreparing) {
    BP->GetProposerBP()->OnPrepareReplyButNotPreparing();
    //PLGErr("Not preparing, skip this msg");
    return;
  }

  // 如果id也不是想要等来的id
  // 丢弃之
  if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID()) {
    BP->GetProposerBP()->OnPrepareReplyNotSameProposalIDMsg();
    //PLGErr("ProposalID not same, skip this msg");
    return;
  }

  // 合法的消息
  // 接收之
  m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

  // 如果收到的不是拒绝信息
  // 也就是对方表示将对这个编号进行处理
  // 如果是拒信，那么拒信里面有那个node的最大提议号
  if (oPaxosMsg.rejectbypromiseid() == 0) {
    // 取出其中的Ballot信息
    BallotNumber oBallot(oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid());
    PLGDebug("[Promise] PreAcceptedID %lu PreAcceptedNodeID %lu ValueSize %zu",
             oPaxosMsg.preacceptid(), oPaxosMsg.preacceptnodeid(), oPaxosMsg.value().size());
    // 计数点头的人的数目
    m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
    // 添加预Accept的值
    // 记录将要达到一致的值
    // 如查有的话
    // 用来给下一步的Accept用
    m_oProposerState.AddPreAcceptValue(oBallot, oPaxosMsg.value());
  } else {
    // 不好意思，被拒了
    PLGDebug("[Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
    // 统计被拒绝的计数
    m_oMsgCounter.AddReject(oPaxosMsg.nodeid());
    m_bWasRejectBySomeone = true;
    // 记录对方的最大的proposal ID
    m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
    // 注意这里拒绝信息可以用来帮助选择下一次的投票编号
    // 这里回过头去看经典的算法
    // 在经典的算法中是没有收到拒信这一个说法的。
    // acceptor在收到一个比自己允诺的编号要小的ballot id的时候，是直接忽略。
    // 直接忽略是可以工作的
    // 假设有A, B, C三个人，
    // 那么当A已经走到200号，而B, C还在10号提议的时候。
    // 如果发起方提出11号，那么A -> X, [B,C] -> OK
    // 注意：此时如果[A, B, C]就11号提议进行投票，仍然不会出现冲突。
    // 这是因为，后面的议题，需要选用最大编号的议题。
    // 如果A在[11,200]号之间投了票，那么必然会选中这个议题
    // 如果A没有在[11, 200]号之前投票，那么也就是说在11号上也没有投过票
    // [B, C]在11号上进行投票，仍然是满足条件3的。
  }

  // 如果要通过，那么必须要大多数人投了同意票。
  // 如果在时间范围内收到了大多数人的投票。
  if (m_oMsgCounter.IsPassedOnThisRound()) {
    int iUseTimeMs = m_oTimeStat.Point();
    BP->GetProposerBP()->PreparePass(iUseTimeMs);
    PLGImp("[Pass] start accept, usetime %dms", iUseTimeMs);
    // 如果收到大多数人对这个提议号的认可
    m_bCanSkipPrepare = true;
    // 进入到accept阶段
    Accept();
  }
  // 如果悲剧了，反对的人占了大多数
  // 或者说，大家都收到了消息
  // 注意：如果赞同与拒绝形成了对半开，那么最后一个人的消息就需要等
  //      所以这里在退出这一轮paxos的时候，设置的条件是需要收到
  //      所有人的消息。
  else if (m_oMsgCounter.IsRejectedOnThisRound()
             || m_oMsgCounter.IsAllReceiveOnThisRound()) {
    BP->GetProposerBP()->PrepareNotPass();
    PLGImp("[Not Pass] wait 30ms and restart prepare");
    AddPrepareTimer(OtherUtils::FastRand() % 30 + 10);
  }

  // 如果既没有通过提议
  // 也没有反对的人占大多数
  // 也没有收到所有人的消息
  // 那么就需要在这里等着接收消息
  //  - 要么等到新消息来
  //  - 要么等到超时

  PLGHead("END");
}

void Proposer::OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg) {
  if (oPaxosMsg.rejectbypromiseid() != 0) {
    PLGDebug("[Expired Prepare Reply Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
    m_bWasRejectBySomeone = true;
    m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
  }
}

// 如果经过了第一阶段，那么这里开始走第二阶段，
// 发起accept请求
void Proposer::Accept() {
  PLGHead("START ProposalID %lu ValueSize %zu ValueLen %zu",
          m_oProposerState.GetProposalID(), m_oProposerState.GetValue().size(), m_oProposerState.GetValue().size());

  BP->GetProposerBP()->Accept();
  m_oTimeStat.Point();

  // 因为第一队段通过了，所以退出第一阶段。
  // 就是把isPreparing = false;
  // remove_prepare_timer
  ExitPrepare();
  // 表示正在accepting.
  m_bIsAccepting = true;

  /*开始构建accept消息*/
  PaxosMsg oPaxosMsg;
  // 消息的类型
  oPaxosMsg.set_msgtype(MsgType_PaxosAccept);
  // 当前的instance id
  oPaxosMsg.set_instanceid(GetInstanceID());
  // 设置自己这个节点
  oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  // 设置提议ID
  oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());
  // 设置前面得到的最大值
  oPaxosMsg.set_value(m_oProposerState.GetValue());
  // 设置校验值
  oPaxosMsg.set_lastchecksum(GetLastChecksum());

  // 开始新一轮的accept
  m_oMsgCounter.StartNewRound();

  AddAcceptTimer();

  PLGHead("END");

  BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_Final);
}

void Proposer::OnAcceptReply(const PaxosMsg & oPaxosMsg) {
  PLGHead("START Msg.ProposalID %lu State.ProposalID %lu Msg.from_nodeid %lu RejectByPromiseID %lu",
          oPaxosMsg.proposalid(), m_oProposerState.GetProposalID(),
          oPaxosMsg.nodeid(), oPaxosMsg.rejectbypromiseid());

  BP->GetProposerBP()->OnAcceptReply();

  // 是否还处在Accept阶段
  // 如果不是，那么这里需要直接就返回
  if (!m_bIsAccepting) {
    //PLGErr("Not proposing, skip this msg");
    BP->GetProposerBP()->OnAcceptReplyButNotAccepting();
    return;
  }

  // 提议的ID
  if (oPaxosMsg.proposalid() != m_oProposerState.GetProposalID()) {
    //PLGErr("ProposalID not same, skip this msg");
    BP->GetProposerBP()->OnAcceptReplyNotSameProposalIDMsg();
    return;
  }

  // 如果是这个消息的响应，那么添加到接收方里面。
  m_oMsgCounter.AddReceive(oPaxosMsg.nodeid());

  // 如果这个消息是一个赞成消息
  if (oPaxosMsg.rejectbypromiseid() == 0) {
    PLGDebug("[Accept]");
    m_oMsgCounter.AddPromiseOrAccept(oPaxosMsg.nodeid());
  }
  // 如果这个消息是一个拒绝消息
  else {
    PLGDebug("[Reject]");
    // 添加到拒绝队列中
    m_oMsgCounter.AddReject(oPaxosMsg.nodeid());
    // 设置拒约标记
    m_bWasRejectBySomeone = true;
    // 设置下一次最大的proposal id
    m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
  }

  // 看一下是否通过
  // 注意，这里只要是形成了大多数，那么就继续往下走
  if (m_oMsgCounter.IsPassedOnThisRound()) {
    int iUseTimeMs = m_oTimeStat.Point();
    BP->GetProposerBP()->AcceptPass(iUseTimeMs);
    PLGImp("[Pass] Start send learn, usetime %dms", iUseTimeMs);
    // 通过了，
    // is_accepting状态退出
    // 删除timer
    ExitAccept();
    m_poLearner->ProposerSendSuccess(GetInstanceID(), m_oProposerState.GetProposalID());
  }
  // 肯定无法通过了
  // 那么不用再等了
  else if (m_oMsgCounter.IsRejectedOnThisRound()
             || m_oMsgCounter.IsAllReceiveOnThisRound()) {
    BP->GetProposerBP()->AcceptNotPass();
    PLGImp("[Not pass] wait 30ms and Restart prepare");
    AddAcceptTimer(OtherUtils::FastRand() % 30 + 10);
  }

  else {
    /* no code here, just wait for next message*/
  }

  PLGHead("END");
}

void Proposer::OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg) {
  if (oPaxosMsg.rejectbypromiseid() != 0) {
    PLGDebug("[Expired Accept Reply Reject] RejectByPromiseID %lu", oPaxosMsg.rejectbypromiseid());
    m_bWasRejectBySomeone = true;
    m_oProposerState.SetOtherProposalID(oPaxosMsg.rejectbypromiseid());
  }
}

void Proposer::OnPrepareTimeout() {
  PLGHead("OK");

  // 如果在超时的这段时间里面 instanceid 已经发生了变化。那么这种超时也没有必要处理了。
  if (GetInstanceID() != m_llTimeoutInstanceID) {
    PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
           m_llTimeoutInstanceID, GetInstanceID());
    return;
  }

  BP->GetProposerBP()->PrepareTimeout();

  // 如果还处在当前的instance id那么处理之
  Prepare(m_bWasRejectBySomeone);
}

void Proposer::OnAcceptTimeout() {
  PLGHead("OK");

  if (GetInstanceID() != m_llTimeoutInstanceID) {
    PLGErr("TimeoutInstanceID %lu not same to NowInstanceID %lu, skip",
           m_llTimeoutInstanceID, GetInstanceID());
    return;
  }

  BP->GetProposerBP()->AcceptTimeout();

  Prepare(m_bWasRejectBySomeone);
}

void Proposer::CancelSkipPrepare() {
  m_bCanSkipPrepare = false;
}

}


