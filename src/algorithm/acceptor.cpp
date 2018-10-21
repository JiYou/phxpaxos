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

#include "acceptor.h"
#include "paxos_log.h"
#include "crc32.h"

namespace phxpaxos {

AcceptorState::AcceptorState(const Config * poConfig, const LogStorage * poLogStorage) :
  m_oPaxosLog(poLogStorage), m_iSyncTimes(0) {
  m_poConfig = (Config *)poConfig;
  Init();
}

AcceptorState::~AcceptorState() {
}

void AcceptorState::Init() {
  m_oAcceptedBallot.reset();

  m_sAcceptedValue = "";

  m_iChecksum = 0;
}

const BallotNumber & AcceptorState::GetPromiseBallot() const {
  return m_oPromiseBallot;
}

void AcceptorState::SetPromiseBallot(const BallotNumber & oPromiseBallot) {
  m_oPromiseBallot = oPromiseBallot;
}

const BallotNumber & AcceptorState::GetAcceptedBallot() const {
  return m_oAcceptedBallot;
}

void AcceptorState::SetAcceptedBallot(const BallotNumber & oAcceptedBallot) {
  m_oAcceptedBallot = oAcceptedBallot;
}

const std::string & AcceptorState::GetAcceptedValue() {
  return m_sAcceptedValue;
}

void AcceptorState::SetAcceptedValue(const std::string & sAcceptedValue) {
  m_sAcceptedValue = sAcceptedValue;
}

const uint32_t AcceptorState::GetChecksum() const {
  return m_iChecksum;
}

int AcceptorState::Persist(const uint64_t llInstanceID, const uint32_t iLastChecksum) {
  if (llInstanceID > 0 && iLastChecksum == 0) {
    m_iChecksum = 0;
  } else if (m_sAcceptedValue.size() > 0) {
    m_iChecksum = crc32(iLastChecksum, (const uint8_t *)m_sAcceptedValue.data(), m_sAcceptedValue.size(), CRC32SKIP);
  }

  AcceptorStateData oState;
  oState.set_instanceid(llInstanceID);
  oState.set_promiseid(m_oPromiseBallot.m_llProposalID);
  oState.set_promisenodeid(m_oPromiseBallot.m_llNodeID);
  oState.set_acceptedid(m_oAcceptedBallot.m_llProposalID);
  oState.set_acceptednodeid(m_oAcceptedBallot.m_llNodeID);
  oState.set_acceptedvalue(m_sAcceptedValue);
  oState.set_checksum(m_iChecksum);

  WriteOptions oWriteOptions;
  oWriteOptions.bSync = m_poConfig->LogSync();
  if (oWriteOptions.bSync) {
    m_iSyncTimes++;
    if (m_iSyncTimes > m_poConfig->SyncInterval()) {
      m_iSyncTimes = 0;
    } else {
      oWriteOptions.bSync = false;
    }
  }

  int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
  if (ret != 0) {
    return ret;
  }

  PLGImp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu "
         "AccectpedID %lu AcceptedNodeID %lu ValueLen %zu Checksum %u",
         m_poConfig->GetMyGroupIdx(), llInstanceID, m_oPromiseBallot.m_llProposalID,
         m_oPromiseBallot.m_llNodeID, m_oAcceptedBallot.m_llProposalID,
         m_oAcceptedBallot.m_llNodeID, m_sAcceptedValue.size(), m_iChecksum);

  return 0;
}

int AcceptorState::Load(uint64_t & llInstanceID) {
  int ret = m_oPaxosLog.GetMaxInstanceIDFromLog(m_poConfig->GetMyGroupIdx(), llInstanceID);
  if (ret != 0 && ret != 1) {
    PLGErr("Load max instance id fail, ret %d", ret);
    return ret;
  }

  if (ret == 1) {
    PLGErr("empty database");
    llInstanceID = 0;
    return 0;
  }

  AcceptorStateData oState;
  ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
  if (ret != 0) {
    return ret;
  }

  m_oPromiseBallot.m_llProposalID = oState.promiseid();
  m_oPromiseBallot.m_llNodeID = oState.promisenodeid();
  m_oAcceptedBallot.m_llProposalID = oState.acceptedid();
  m_oAcceptedBallot.m_llNodeID = oState.acceptednodeid();
  m_sAcceptedValue = oState.acceptedvalue();
  m_iChecksum = oState.checksum();

  PLGImp("GroupIdx %d InstanceID %lu PromiseID %lu PromiseNodeID %lu"
         " AccectpedID %lu AcceptedNodeID %lu ValueLen %zu Checksum %u",
         m_poConfig->GetMyGroupIdx(), llInstanceID, m_oPromiseBallot.m_llProposalID,
         m_oPromiseBallot.m_llNodeID, m_oAcceptedBallot.m_llProposalID,
         m_oAcceptedBallot.m_llNodeID, m_sAcceptedValue.size(), m_iChecksum);

  return 0;
}

/////////////////////////////////////////////////////////////////////////////////

Acceptor::Acceptor(
  const Config * poConfig,
  const MsgTransport * poMsgTransport,
  const Instance * poInstance,
  const LogStorage * poLogStorage)
  : Base(poConfig, poMsgTransport, poInstance), m_oAcceptorState(poConfig, poLogStorage) {
}

Acceptor::~Acceptor() {
}

int Acceptor::Init() {
  uint64_t llInstanceID = 0;
  int ret = m_oAcceptorState.Load(llInstanceID);
  if (ret != 0) {
    NLErr("Load State fail, ret %d", ret);
    return ret;
  }

  if (llInstanceID == 0) {
    PLGImp("Empty database");
  }

  SetInstanceID(llInstanceID);

  PLGImp("OK");

  return 0;
}

void Acceptor::InitForNewPaxosInstance() {
  m_oAcceptorState.Init();
}

AcceptorState * Acceptor::GetAcceptorState() {
  return &m_oAcceptorState;
}

int Acceptor::OnPrepare(const PaxosMsg & oPaxosMsg) {
  PLGHead("START Msg.InstanceID %lu Msg.from_nodeid %lu Msg.ProposalID %lu",
          oPaxosMsg.instanceid(), oPaxosMsg.nodeid(), oPaxosMsg.proposalid());

  BP->GetAcceptorBP()->OnPrepare();

  // 开始准备返回消息
  PaxosMsg oReplyPaxosMsg;
  // 设置当前已经进行到哪个instance id了
  oReplyPaxosMsg.set_instanceid(GetInstanceID());
  // 设置自己是哪个节点
  oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  // 设置原始消息中的proposal id
  // 这个是非常有必要的。假设当前节点处理特别慢。
  // 而其他节点都比这个节点快。
  // 那么，当这个节点的消息返回到Proposer节点的时候
  // 可以proposal id已经走到很后面了。
  // 这个时候Proposer就可以直接把这个消息扔掉
  oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
  // 消息的类型。
  // 用来在接收方分流
  // 也就是知道交给谁来处理
  oReplyPaxosMsg.set_msgtype(MsgType_PaxosPrepareReply);
  // 取得提议方的编号，id
  BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

  // 如果进来的编号 >= 我既将要要投票的编号
  // 注意编号的比较方式: 采用了论文里面的<proposalID, nodeID>
  // 唯一性比较大小的方式来处理
  if (oBallot >= m_oAcceptorState.GetPromiseBallot()) {
    PLGDebug("[Promise] State.PromiseID %lu State.PromiseNodeID %lu "
             "State.PreAcceptedID %lu State.PreAcceptedNodeID %lu",
             m_oAcceptorState.GetPromiseBallot().m_llProposalID,
             m_oAcceptorState.GetPromiseBallot().m_llNodeID,
             m_oAcceptorState.GetAcceptedBallot().m_llProposalID,
             m_oAcceptorState.GetAcceptedBallot().m_llNodeID);
    // 返回我上次投票的结果
    oReplyPaxosMsg.set_preacceptid(m_oAcceptorState.GetAcceptedBallot().m_llProposalID);
    oReplyPaxosMsg.set_preacceptnodeid(m_oAcceptorState.GetAcceptedBallot().m_llNodeID);

    // 设置一下值
    // 如果以前投过票
    if (m_oAcceptorState.GetAcceptedBallot().m_llProposalID > 0) {
      oReplyPaxosMsg.set_value(m_oAcceptorState.GetAcceptedValue());
    }
    // 设置第一阶段即将为某个编号投票
    m_oAcceptorState.SetPromiseBallot(oBallot);
    // 把这个状态持久化下来。
    // 写到paxos log里面
    int ret = m_oAcceptorState.Persist(GetInstanceID(), GetLastChecksum());
    if (ret != 0) {
      BP->GetAcceptorBP()->OnPreparePersistFail();
      PLGErr("Persist fail, Now.InstanceID %lu ret %d",
             GetInstanceID(), ret);
      return -1;
    }

    BP->GetAcceptorBP()->OnPreparePass();
  } else {
    BP->GetAcceptorBP()->OnPrepareReject();

    PLGDebug("[Reject] State.PromiseID %lu State.PromiseNodeID %lu",
             m_oAcceptorState.GetPromiseBallot().m_llProposalID,
             m_oAcceptorState.GetPromiseBallot().m_llNodeID);
    // 否则发拒信
    // 返回最大的proposal ID
    oReplyPaxosMsg.set_rejectbypromiseid(m_oAcceptorState.GetPromiseBallot().m_llProposalID);
  }

  nodeid_t iReplyNodeID = oPaxosMsg.nodeid();

  PLGHead("END Now.InstanceID %lu ReplyNodeID %lu",
          GetInstanceID(), oPaxosMsg.nodeid());;
  // 发送顺应消息
  SendMessage(iReplyNodeID, oReplyPaxosMsg);

  return 0;
}

void Acceptor::OnAccept(const PaxosMsg & oPaxosMsg) {
  PLGHead("START Msg.InstanceID %lu Msg.from_nodeid %lu Msg.ProposalID %lu Msg.ValueLen %zu",
          oPaxosMsg.instanceid(), oPaxosMsg.nodeid(), oPaxosMsg.proposalid(), oPaxosMsg.value().size());

  BP->GetAcceptorBP()->OnAccept();

  PaxosMsg oReplyPaxosMsg;
  oReplyPaxosMsg.set_instanceid(GetInstanceID());
  oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
  oReplyPaxosMsg.set_msgtype(MsgType_PaxosAcceptReply);

  BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

  if (oBallot >= m_oAcceptorState.GetPromiseBallot()) {
    PLGDebug("[Promise] State.PromiseID %lu State.PromiseNodeID %lu "
             "State.PreAcceptedID %lu State.PreAcceptedNodeID %lu",
             m_oAcceptorState.GetPromiseBallot().m_llProposalID,
             m_oAcceptorState.GetPromiseBallot().m_llNodeID,
             m_oAcceptorState.GetAcceptedBallot().m_llProposalID,
             m_oAcceptorState.GetAcceptedBallot().m_llNodeID);

    m_oAcceptorState.SetPromiseBallot(oBallot);
    m_oAcceptorState.SetAcceptedBallot(oBallot);
    m_oAcceptorState.SetAcceptedValue(oPaxosMsg.value());

    int ret = m_oAcceptorState.Persist(GetInstanceID(), GetLastChecksum());
    if (ret != 0) {
      BP->GetAcceptorBP()->OnAcceptPersistFail();

      PLGErr("Persist fail, Now.InstanceID %lu ret %d",
             GetInstanceID(), ret);

      return;
    }

    BP->GetAcceptorBP()->OnAcceptPass();
  } else {
    BP->GetAcceptorBP()->OnAcceptReject();

    PLGDebug("[Reject] State.PromiseID %lu State.PromiseNodeID %lu",
             m_oAcceptorState.GetPromiseBallot().m_llProposalID,
             m_oAcceptorState.GetPromiseBallot().m_llNodeID);

    oReplyPaxosMsg.set_rejectbypromiseid(m_oAcceptorState.GetPromiseBallot().m_llProposalID);
  }

  nodeid_t iReplyNodeID = oPaxosMsg.nodeid();

  PLGHead("END Now.InstanceID %lu ReplyNodeID %lu",
          GetInstanceID(), oPaxosMsg.nodeid());

  SendMessage(iReplyNodeID, oReplyPaxosMsg);
}

}


