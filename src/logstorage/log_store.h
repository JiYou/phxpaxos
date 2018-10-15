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
#include <mutex>
#include "commdef.h"
#include "utils_include.h"
#include "commdef.h"
#include "comm_include.h"

namespace phxpaxos {

class Database;

#define FILEID_LEN (sizeof(int) + sizeof(int) + sizeof(uint32_t))

class LogStoreLogger {
 public:
  LogStoreLogger();
  ~LogStoreLogger();

  void Init(const std::string & sPath);
  void Log(const char * pcFormat, ...);

 private:
  int m_iLogFd;
};

// http://blog.fnil.net/blog/60348d92fdb346d044a1402408ebd4c8/
// 这一部分是非常核心的存储模块，参与者的状态信息、变更日志等都要写入磁盘并
// 且可能要求强制刷入存储磁盘避免系统崩溃等情况下数据丢失，
// 性能的很大一部分因素取决于这一块的实现。
// phxpaxos 使用了 LevelDB 做存储，但是做了几个优化:
// 1. LevelDB 存储的 value 只是一个 24 个字节的 fileid 索引
//   （有一个例外 minchosen 值），真正的状态数据存储在他们自己实现的
//    log_store 里, fileid 存储了
//       - 在 log_store 里的文件编号
//       - 写入 vfile 的offset
//       -  checksum 这三个字段信息。
// 这样做的原因是由于 leveldb 对于比较大的 value 的存取效率不是最优
// 通过间接一层存储，利用了 LevelDB 的 Key 的有序性和小 value 存储的
// 性能优势，加上定制的 log_store 的写入优化，达到最优组合。
// 2. log_store 按照文件编号固定增长，比如 1.f、2.f、3.f ……以此类推
//   （在日志目录的 vfile 目录下）。并且每个文件都是在创建的时候分配固定大小，
//    默认 100M（还有所谓 large buffer 模式分配的是 500M）。值的写入都是 
//    append 操作，写入完成后，将偏移量、校验和、当前文件 ID 再写入 LevelDB。
//    读取的时候就先从 LevelDB 得到这三个信息，然后去对应的 vfile 读取实际的值。
//    因为文件是固定大小分配，每次强制刷盘就不需要调用 fsync，而是采用 fdatasync，
//    这样就无需做两次刷盘，因为 fdatasync 不会去刷入文件的元信息
//    （比如大小、最后访问时间、最后修改时间等），而 fsync 是会的。
// 3。 注意到图中还有个 metafile，它存储了 log store 的当前的文件编号这个元信息
//     写满一个 vfile 就递增下这个计数，并更新到 metafile，为了保证不丢，
//     对它的每次修改都需要强制 fsync。
//     这个类似于leveldb的CURRENT文件，指向当前的manifest文件。
//     即然都用levelDB了，为什么把meta内容直接做成key/val放到leveldb中?
// 4. 这里能节省的IO可能并不理想，因为leveldb中每写一个item，就是用的fsync().
class LogStore {
 public:
  LogStore();
  ~LogStore();

  int Init(const std::string & sPath, const int iMyGroupIdx, Database * poDatabase);

  int Append(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sBuffer, std::string & sFileID);

  int Read(const std::string & sFileID, uint64_t & llInstanceID, std::string & sBuffer);

  int Del(const std::string & sFileID, const uint64_t llInstanceID);

  int ForceDel(const std::string & sFileID, const uint64_t llInstanceID);

  ////////////////////////////////////////////

  const bool IsValidFileID(const std::string & sFileID);

  ////////////////////////////////////////////

  int RebuildIndex(Database * poDatabase, int & iNowFileWriteOffset);

  int RebuildIndexForOneFile(const int iFileID, const int iOffset,
                             Database * poDatabase, int & iNowFileWriteOffset, uint64_t & llNowInstanceID);

 private:
  void GenFileID(const int iFileID, const int iOffset, const uint32_t iCheckSum, std::string & sFileID);

  void ParseFileID(const std::string & sFileID, int & iFileID, int & iOffset, uint32_t & iCheckSum);

  int IncreaseFileID();

  int OpenFile(const int iFileID, int & iFd);

  int DeleteFile(const int iFileID);

  int GetFileFD(const int iNeedWriteSize, int & iFd, int & iFileID, int & iOffset);

  int ExpandFile(int iFd, int & iFileSize);

 private:
  int m_iFd;
  int m_iMetaFd;
  int m_iFileID;
  std::string m_sPath;
  BytesBuffer m_oTmpBuffer;
  BytesBuffer m_oTmpAppendBuffer;

  std::mutex m_oMutex;
  std::mutex m_oReadMutex;

  int m_iDeletedMaxFileID;
  int m_iMyGroupIdx;

  int m_iNowFileSize;
  int m_iNowFileOffset;

 private:
  TimeStat m_oTimeStat;
  LogStoreLogger m_oFileLogger;
};

}
