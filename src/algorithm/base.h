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

#include "commdef.h"
#include "comm_include.h"
#include "config_include.h"
#include "msg_transport.h"

namespace phxpaxos {

#define GROUPIDXLEN (sizeof(int))
#define HEADLEN_LEN (sizeof(uint16_t))
#define CHECKSUM_LEN (sizeof(uint32_t))


class BallotNumber {
 public:
  BallotNumber() : m_llProposalID(0), m_llNodeID(nullnode) { }

  BallotNumber(const uint64_t llProposalID, const nodeid_t llNodeID) :
    m_llProposalID(llProposalID), m_llNodeID(llNodeID) { }

  ~BallotNumber() { }

  bool operator >= (const BallotNumber & other) const {
    if (m_llProposalID == other.m_llProposalID) {
      return m_llNodeID >= other.m_llNodeID;
    } else {
      return m_llProposalID >= other.m_llProposalID;
    }
  }

  bool operator != (const BallotNumber & other) const {
    return m_llProposalID != other.m_llProposalID
           || m_llNodeID != other.m_llNodeID;
  }

  bool operator == (const BallotNumber & other) const {
    return m_llProposalID == other.m_llProposalID
           && m_llNodeID == other.m_llNodeID;
  }

  bool operator > (const BallotNumber & other) const {
    if (m_llProposalID == other.m_llProposalID) {
      return m_llNodeID > other.m_llNodeID;
    } else {
      return m_llProposalID > other.m_llProposalID;
    }
  }

  const bool isnull() const {
    return m_llProposalID == 0;
  }

  void reset() {
    m_llProposalID = 0;
    m_llNodeID = 0;
  }

  uint64_t m_llProposalID;
  // node id就是一个ip:port分别占用两个32位
  // NodeInfo::MakeNodeID()
  nodeid_t m_llNodeID;
};

///////////////////////////////////////////////////////////

class Instance;

enum BroadcastMessage_Type {
  // 自己这个节点先运行
  BroadcastMessage_Type_RunSelf_First = 1,
  BroadcastMessage_Type_RunSelf_Final = 2,
  BroadcastMessage_Type_RunSelf_None = 3,
};

// 总的来说，这个类是各种Proposer/Acceptor/Learner的父类
// 主要负责的任务就是：负责处理
// Instance ID
// 各种message.
class Base {
 public:
  Base(const Config * poConfig, const MsgTransport * poMsgTransport, const Instance * poInstance);
  virtual ~Base();

 public:
  // 涉及到消息处理的地方，都是采用的是如下结构
  // 消息先统一放到一个Queu里面。然后再从Queue里面一个一个地消费。
  // 由于消息的执行是单线程的，并不是并发的。
  // 所以这里的这些操作也都是无锁的。

  // Instance ID的管理
  uint64_t GetInstanceID();
  void NewInstance();
  virtual void InitForNewPaxosInstance() = 0;
  void SetInstanceID(const uint64_t llInstanceID);

  // 解包与封包函数
  int PackMsg(const PaxosMsg & oPaxosMsg, std::string & sBuffer);
  int PackCheckpointMsg(const CheckpointMsg & oCheckpointMsg, std::string & sBuffer);

 public:
  const uint32_t GetLastChecksum() const;

  void PackBaseMsg(const std::string & sBodyBuffer, const int iCmd, std::string & sBuffer);

  static int UnPackBaseMsg(const std::string & sBuffer, Header & oHeader, size_t & iBodyStartPos, size_t & iBodyLen);

  void SetAsTestMode();

 protected:
  virtual int SendMessage(const nodeid_t iSendtoNodeID, const PaxosMsg & oPaxosMsg, const int iSendType = Message_SendType_UDP);

  virtual int BroadcastMessage(
    const PaxosMsg & oPaxosMsg,
    const int bRunSelfFirst = BroadcastMessage_Type_RunSelf_First,
    const int iSendType = Message_SendType_UDP);

  int BroadcastMessageToFollower(
    const PaxosMsg & oPaxosMsg,
    const int iSendType = Message_SendType_TCP);

  int BroadcastMessageToTempNode(
    const PaxosMsg & oPaxosMsg,
    const int iSendType = Message_SendType_UDP);

 protected:
  int SendMessage(const nodeid_t iSendtoNodeID, const CheckpointMsg & oCheckpointMsg,
                  const int iSendType = Message_SendType_TCP);

 protected:
  Config * m_poConfig;
  MsgTransport * m_poMsgTransport;
  Instance * m_poInstance;

 private:
  uint64_t m_llInstanceID;

  bool m_bIsTestMode;
};

}
