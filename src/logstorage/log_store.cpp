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

#include "log_store.h"
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "crc32.h"
#include "comm_include.h"
#include "db.h"
#include "paxos_msg.pb.h"

namespace phxpaxos {

LogStore::LogStore() {
  m_iFd = -1;
  m_iMetaFd = -1;
  m_iFileID = -1;
  m_iDeletedMaxFileID = -1;
  m_iMyGroupIdx = -1;
  m_iNowFileSize = -1;
  m_iNowFileOffset = 0;
}

LogStore::~LogStore() {
  if (m_iFd != -1) {
    close(m_iFd);
  }

  if (m_iMetaFd != -1) {
    close(m_iMetaFd);
  }
}

// 总结一下Init()函数：
// 1. 创建sPath/vfile
// 2. 设置info logger文件句柄
// 3. 设置meta文件句柄，读出WAL LOG文件的序号，并且校验这个序号
// 4. RebuildIndex 就是查出当前读写到了什么位置。
// 5. RebuildIndex就是去拿最新的file id/offset,
//    当拿到之后，第5步会膨胀这个文件到指定大小，然后移动指针到这个offset
int LogStore::Init(const std::string & sPath, const int iMyGroupIdx, Database * poDatabase) {
  // 因为在DB初始化的时候，已经会去检查了
  // 所以这里直接设置group index.
  m_iMyGroupIdx = iMyGroupIdx;
  // 创建目录并且生成/vfile
  m_sPath = sPath + "/" + "vfile";
  if (access(m_sPath.c_str(), F_OK) == -1) {
    if (mkdir(m_sPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
      PLG1Err("Create dir fail, path %s", m_sPath.c_str());
      return -1;
    }
  }

  // 这里生成sPah/vfile/LOG文件路径，
  // 并且打开这个文件，生成相应的文件句柄
  m_oFileLogger.Init(m_sPath);

  // 元数据文件, 类似于leveldb中的current文件
  // 这个文件中的内容是当前哪个WAL LOG文件是有效的
  // 只不过leveldb中的current指的是manifest文件
  string sMetaFilePath = m_sPath + "/meta";
  // 这里设置meta file的路径，并且打开之。
  m_iMetaFd = open(sMetaFilePath.c_str(), O_CREAT | O_RDWR, S_IREAD | S_IWRITE);
  if (m_iMetaFd == -1) {
    PLG1Err("open meta file fail, filepath %s", sMetaFilePath.c_str());
    return -1;
  }

  // 移动到文件头
  // 读写位置移到文件开头时:lseek(int fildes, 0, SEEK_SET);
  // 读写位置移到文件尾时:lseek(int fildes, 0, SEEK_END);
  // 取得目前文件位置时:lseek(int fildes, 0, SEEK_CUR);
  off_t iSeekPos = lseek(m_iMetaFd, 0, SEEK_SET);
  if (iSeekPos == -1) {
    return -1;
  }
  // 读一个整数出来。
  ssize_t iReadLen = read(m_iMetaFd, &m_iFileID, sizeof(int));
  // 失败就从0开始
  // 读出WAL LOG文件当前的ID
  if (iReadLen != (ssize_t)sizeof(int)) {
    if (iReadLen == 0) {
      m_iFileID = 0;
    } else {
      PLG1Err("read meta info fail, readlen %zd", iReadLen);
      return -1;
    }
  }
  // 读出校验码，这里是针对fileID的校验码
  uint32_t iMetaChecksum = 0;
  iReadLen = read(m_iMetaFd, &iMetaChecksum, sizeof(uint32_t));
  if (iReadLen == (ssize_t)sizeof(uint32_t)) {
    uint32_t iCheckSum = crc32(0, (const uint8_t*)(&m_iFileID), sizeof(int));
    if (iCheckSum != iMetaChecksum) {
      PLG1Err("meta file checksum %u not same to cal checksum %u, fileid %d",
              iMetaChecksum, iCheckSum, m_iFileID);
      return -2;
    }
  }
  // 重建index
  // 这里应该是修正当前wal log文件的写入位置
  int ret = RebuildIndex(poDatabase, m_iNowFileOffset);
  if (ret != 0) {
    PLG1Err("rebuild index fail, ret %d", ret);
    return -1;
  }

  // m_iFileID指向最新的文件ID
  ret = OpenFile(m_iFileID, m_iFd);
  if (ret != 0) {
    return ret;
  }
  // 这里办法比较简单粗暴，就是直接往这个文件里面灌0
  // 写到 100M or 500M为止 
  ret = ExpandFile(m_iFd, m_iNowFileSize);
  if (ret != 0) {
    return ret;
  }

  // 移动到正确位置
  m_iNowFileOffset = lseek(m_iFd, m_iNowFileOffset, SEEK_SET);
  if (m_iNowFileOffset == -1) {
    PLG1Err("seek to now file offset %d fail", m_iNowFileOffset);
    return -1;
  }

  // 日志输出，不用管
  m_oFileLogger.Log("init write fileid %d now_w_offset %d filesize %d",
                    m_iFileID, m_iNowFileOffset, m_iNowFileSize);

  PLG1Head("ok, path %s fileid %d meta checksum %u nowfilesize %d nowfilewriteoffset %d",
           m_sPath.c_str(), m_iFileID, iMetaChecksum, m_iNowFileSize, m_iNowFileOffset);

  return 0;
}

int LogStore::ExpandFile(int iFd, int & iFileSize) {
  iFileSize = lseek(iFd, 0, SEEK_END);
  if (iFileSize == -1) {
    PLG1Err("lseek fail, ret %d", iFileSize);
    return -1;
  }

  if (iFileSize == 0) {
    //new file
    iFileSize = lseek(iFd, LOG_FILE_MAX_SIZE - 1, SEEK_SET);
    if (iFileSize != LOG_FILE_MAX_SIZE - 1) {
      return -1;
    }

    ssize_t iWriteLen = write(iFd, "\0", 1);
    if (iWriteLen != 1) {
      PLG1Err("write 1 bytes fail");
      return -1;
    }

    iFileSize = LOG_FILE_MAX_SIZE;
    int iOffset = lseek(iFd, 0, SEEK_SET);
    m_iNowFileOffset = 0;
    if (iOffset != 0) {
      return -1;
    }
  }

  return 0;
}

int LogStore::IncreaseFileID() {
  int iFileID = m_iFileID + 1;
  uint32_t iCheckSum = crc32(0, (const uint8_t*)(&iFileID), sizeof(int));

  off_t iSeekPos = lseek(m_iMetaFd, 0, SEEK_SET);
  if (iSeekPos == -1) {
    return -1;
  }

  size_t iWriteLen = write(m_iMetaFd, (char *)&iFileID, sizeof(int));
  if (iWriteLen != sizeof(int)) {
    PLG1Err("write meta fileid fail, writelen %zu", iWriteLen);
    return -1;
  }

  iWriteLen = write(m_iMetaFd, (char *)&iCheckSum, sizeof(uint32_t));
  if (iWriteLen != sizeof(uint32_t)) {
    PLG1Err("write meta checksum fail, writelen %zu", iWriteLen);
    return -1;
  }

  int ret = fsync(m_iMetaFd);
  if (ret != 0) {
    return -1;
  }

  m_iFileID++;

  return 0;
}

int LogStore::OpenFile(const int iFileID, int & iFd) {
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/%d.f", m_sPath.c_str(), iFileID);
  iFd = open(sFilePath, O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
  if (iFd == -1) {
    PLG1Err("open fail fail, filepath %s", sFilePath);
    return -1;
  }

  PLG1Imp("ok, path %s", sFilePath);
  return 0;
}

int LogStore::DeleteFile(const int iFileID) {
  if (m_iDeletedMaxFileID == -1) {
    if (iFileID - 2000 > 0) {
      m_iDeletedMaxFileID = iFileID - 2000;
    }
  }

  if (iFileID <= m_iDeletedMaxFileID) {
    PLG1Debug("file already deleted, fileid %d deletedmaxfileid %d", iFileID, m_iDeletedMaxFileID);
    return 0;
  }

  int ret = 0;
  for (int iDeleteFileID = m_iDeletedMaxFileID + 1; iDeleteFileID <= iFileID; iDeleteFileID++) {
    char sFilePath[512] = {0};
    snprintf(sFilePath, sizeof(sFilePath), "%s/%d.f", m_sPath.c_str(), iDeleteFileID);

    ret = access(sFilePath, F_OK);
    if (ret == -1) {
      PLG1Debug("file already deleted, filepath %s", sFilePath);
      m_iDeletedMaxFileID = iDeleteFileID;
      ret = 0;
      continue;
    }

    ret = remove(sFilePath);
    if (ret != 0) {
      PLG1Err("remove fail, filepath %s ret %d", sFilePath, ret);
      break;
    }

    m_iDeletedMaxFileID = iDeleteFileID;
    m_oFileLogger.Log("delete fileid %d", iDeleteFileID);
  }

  return ret;
}

int LogStore::GetFileFD(const int iNeedWriteSize, int & iFd, int & iFileID, int & iOffset) {
  if (m_iFd == -1) {
    PLG1Err("File aready broken, fileid %d", m_iFileID);
    return -1;
  }

  iOffset = lseek(m_iFd, m_iNowFileOffset, SEEK_SET);
  assert(iOffset != -1);

  if (iOffset + iNeedWriteSize > m_iNowFileSize) {
    close(m_iFd);
    m_iFd = -1;

    int ret = IncreaseFileID();
    if (ret != 0) {
      m_oFileLogger.Log("new file increase fileid fail, now fileid %d", m_iFileID);
      return ret;
    }

    ret = OpenFile(m_iFileID, m_iFd);
    if (ret != 0) {
      m_oFileLogger.Log("new file open file fail, now fileid %d", m_iFileID);
      return ret;
    }

    iOffset = lseek(m_iFd, 0, SEEK_END);
    if (iOffset != 0) {
      assert(iOffset != -1);

      m_oFileLogger.Log("new file but file aready exist, now fileid %d exist filesize %d",
                        m_iFileID, iOffset);

      PLG1Err("IncreaseFileID success, but file exist, data wrong, file size %d", iOffset);
      assert(false);
      return -1;
    }

    ret = ExpandFile(m_iFd, m_iNowFileSize);
    if (ret != 0) {
      PLG1Err("new file expand fail, fileid %d fd %d", m_iFileID, m_iFd);

      m_oFileLogger.Log("new file expand file fail, now fileid %d", m_iFileID);

      close(m_iFd);
      m_iFd = -1;
      return -1;
    }

    m_oFileLogger.Log("new file expand ok, fileid %d filesize %d", m_iFileID, m_iNowFileSize);
  }

  iFd = m_iFd;
  iFileID = m_iFileID;

  return 0;
}

int LogStore::Append(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sBuffer, std::string & sFileID) {
  m_oTimeStat.Point();
  std::lock_guard<std::mutex> oLock(m_oMutex);

  int iFd = -1;
  int iFileID = -1;
  int iOffset = -1;

  int iLen = sizeof(uint64_t) + sBuffer.size();
  int iTmpBufferLen = iLen + sizeof(int);

  int ret = GetFileFD(iTmpBufferLen, iFd, iFileID, iOffset);
  if (ret != 0) {
    return ret;
  }

  m_oTmpAppendBuffer.Ready(iTmpBufferLen);

  memcpy(m_oTmpAppendBuffer.GetPtr(), &iLen, sizeof(int));
  memcpy(m_oTmpAppendBuffer.GetPtr() + sizeof(int), &llInstanceID, sizeof(uint64_t));
  memcpy(m_oTmpAppendBuffer.GetPtr() + sizeof(int) + sizeof(uint64_t), sBuffer.c_str(), sBuffer.size());

  size_t iWriteLen = write(iFd, m_oTmpAppendBuffer.GetPtr(), iTmpBufferLen);

  if (iWriteLen != (size_t)iTmpBufferLen) {
    BP->GetLogStorageBP()->AppendDataFail();
    PLG1Err("writelen %d not equal to %d, buffersize %zu errno %d",
            iWriteLen, iTmpBufferLen, sBuffer.size(), errno);
    return -1;
  }

  if (oWriteOptions.bSync) {
    int fdatasync_ret = fdatasync(iFd);
    if (fdatasync_ret == -1) {
      PLG1Err("fdatasync fail, writelen %zu errno %d", iWriteLen, errno);
      return -1;
    }
  }

  m_iNowFileOffset += iWriteLen;

  int iUseTimeMs = m_oTimeStat.Point();
  BP->GetLogStorageBP()->AppendDataOK(iWriteLen, iUseTimeMs);

  uint32_t iCheckSum = crc32(0, (const uint8_t*)(m_oTmpAppendBuffer.GetPtr() + sizeof(int)), iTmpBufferLen - sizeof(int), CRC32SKIP);

  GenFileID(iFileID, iOffset, iCheckSum, sFileID);

  PLG1Imp("ok, offset %d fileid %d checksum %u instanceid %lu buffer size %zu usetime %dms sync %d",
          iOffset, iFileID, iCheckSum, llInstanceID, sBuffer.size(), iUseTimeMs, (int)oWriteOptions.bSync);

  return 0;
}

int LogStore::Read(const std::string & sFileID, uint64_t & llInstanceID, std::string & sBuffer) {
  int iFileID = -1;
  int iOffset = -1;
  uint32_t iCheckSum = 0;
  ParseFileID(sFileID, iFileID, iOffset, iCheckSum);

  int iFd = -1;
  int ret = OpenFile(iFileID, iFd);
  if (ret != 0) {
    return ret;
  }

  off_t iSeekPos = lseek(iFd, iOffset, SEEK_SET);
  if (iSeekPos == -1) {
    return -1;
  }

  int iLen = 0;
  ssize_t iReadLen = read(iFd, (char *)&iLen, sizeof(int));
  if (iReadLen != (ssize_t)sizeof(int)) {
    close(iFd);
    PLG1Err("readlen %zd not qual to %zu", iReadLen, sizeof(int));
    return -1;
  }

  std::lock_guard<std::mutex> oLock(m_oReadMutex);

  m_oTmpBuffer.Ready(iLen);
  iReadLen = read(iFd, m_oTmpBuffer.GetPtr(), iLen);
  if (iReadLen != iLen) {
    close(iFd);
    PLG1Err("readlen %zd not qual to %zu", iReadLen, iLen);
    return -1;
  }

  close(iFd);

  uint32_t iFileCheckSum = crc32(0, (const uint8_t *)m_oTmpBuffer.GetPtr(), iLen, CRC32SKIP);

  if (iFileCheckSum != iCheckSum) {
    BP->GetLogStorageBP()->GetFileChecksumNotEquel();
    PLG1Err("checksum not equal, filechecksum %u checksum %u", iFileCheckSum, iCheckSum);
    return -2;
  }

  memcpy(&llInstanceID, m_oTmpBuffer.GetPtr(), sizeof(uint64_t));
  sBuffer = string(m_oTmpBuffer.GetPtr() + sizeof(uint64_t), iLen - sizeof(uint64_t));

  PLG1Imp("ok, fileid %d offset %d instanceid %lu buffer size %zu",
          iFileID, iOffset, llInstanceID, sBuffer.size());

  return 0;
}

int LogStore::Del(const std::string & sFileID, const uint64_t llInstanceID) {
  int iFileID = -1;
  int iOffset = -1;
  uint32_t iCheckSum = 0;
  ParseFileID(sFileID, iFileID, iOffset, iCheckSum);

  if (iFileID > m_iFileID) {
    PLG1Err("del fileid %d large than useing fileid %d", iFileID, m_iFileID);
    return -2;
  }

  if (iFileID > 0) {
    return DeleteFile(iFileID - 1);
  }

  return 0;
}

int LogStore::ForceDel(const std::string & sFileID, const uint64_t llInstanceID) {
  int iFileID = -1;
  int iOffset = -1;
  uint32_t iCheckSum = 0;
  ParseFileID(sFileID, iFileID, iOffset, iCheckSum);

  if (iFileID != m_iFileID) {
    PLG1Err("del fileid %d not equal to fileid %d", iFileID, m_iFileID);
    return -2;
  }

  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/%d.f", m_sPath.c_str(), iFileID);

  printf("fileid %d offset %d\n", iFileID, iOffset);

  if (truncate(sFilePath, iOffset) != 0) {
    return -1;
  }

  return 0;
}


void LogStore::GenFileID(const int iFileID, const int iOffset, const uint32_t iCheckSum, std::string & sFileID) {
  char sTmp[sizeof(int) + sizeof(int) + sizeof(uint32_t)] = {0};
  memcpy(sTmp, (char *)&iFileID, sizeof(int));
  memcpy(sTmp + sizeof(int), (char *)&iOffset, sizeof(int));
  memcpy(sTmp + sizeof(int) + sizeof(int), (char *)&iCheckSum, sizeof(uint32_t));

  sFileID = std::string(sTmp, sizeof(int) + sizeof(int) + sizeof(uint32_t));
}

void LogStore::ParseFileID(const std::string & sFileID, int & iFileID, int & iOffset, uint32_t & iCheckSum) {
  memcpy(&iFileID, (void *)sFileID.c_str(), sizeof(int));
  memcpy(&iOffset, (void *)(sFileID.c_str() + sizeof(int)), sizeof(int));
  memcpy(&iCheckSum, (void *)(sFileID.c_str() + sizeof(int) + sizeof(int)), sizeof(uint32_t));

  PLG1Debug("fileid %d offset %d checksum %u", iFileID, iOffset, iCheckSum);
}

const bool LogStore::IsValidFileID(const std::string & sFileID) {
  if (sFileID.size() != FILEID_LEN) {
    return false;
  }

  return true;
}

//////////////////////////////////////////////////////////////////

int LogStore::RebuildIndex(Database * poDatabase, int & iNowFileWriteOffset) {
  string sLastFileID;

  uint64_t llNowInstanceID = 0;
  // 这里从leveldb中取出max instance ID
  // 以及这个max instance ID所对应的WAL LOG File ID
  // 把取出的结果放到
  // - sLastFileID -> 实际上这里包含fileID/offset,checksum三个值
  // - llNowInstanceID
  // 这两个变量里面
  int ret = poDatabase->GetMaxInstanceIDFileID(sLastFileID, llNowInstanceID);
  if (ret != 0) {
    return ret;
  }

  int iFileID = 0;
  int iOffset = 0;
  uint32_t iCheckSum = 0;

  // 从字符串中解析出file id, offset, checksum.
  if (sLastFileID.size() > 0) {
    ParseFileID(sLastFileID, iFileID, iOffset, iCheckSum);
  }

  // 如果解析出来的file id比meta文件中的id还要大
  // 那么这里应该是要报错
  if (iFileID > m_iFileID) {
    PLG1Err("LevelDB last fileid %d larger than meta now fileid %d, file error",
            iFileID, m_iFileID);
    return -2;
  }

  PLG1Head("START fileid %d offset %d checksum %u", iFileID, iOffset, iCheckSum);
  // 这里逐步向前递进
  // 找到相应的最新的offset.
  for (int iNowFileID = iFileID; ; iNowFileID++) {
    ret = RebuildIndexForOneFile(iNowFileID, iOffset, poDatabase, iNowFileWriteOffset, llNowInstanceID);
    if (ret != 0 && ret != 1) {
      break;
    } else if (ret == 1) {
      if (iNowFileID != 0 && iNowFileID != m_iFileID + 1) {
        PLG1Err("meta file wrong, nowfileid %d meta.nowfileid %d", iNowFileID, m_iFileID);
        return -1;
      }

      ret = 0;
      PLG1Imp("END rebuild ok, nowfileid %d", iNowFileID);
      break;
    }

    iOffset = 0;
  }

  return ret;
}

// truncate()和ftruncate（）函数导致一个名称为path
// 或者被文件描述符fd引用的常规文件被截断成一个大小精
// 为length字节的文件。如果先前的文件大于这个大小，
// 额外的数据丢失。如果先前的文件小于当前定义的大小，
// 那么，这个文件将会被扩展，扩展的部分将补以null,也就是‘\0’。
//  如果大小发生变化，那么这个st_ctime(访问时间)和st_mtime()
// 修改时间将会被更新。使用ftruncate()，这个文件必须被
// 打开用以写操作。使用truncate函数的文件必须能够被写。
int LogStore::RebuildIndexForOneFile(const int iFileID,
                                     const int iOffset,
                                     Database * poDatabase,
                                     int & iNowFileWriteOffset,
                                     uint64_t & llNowInstanceID) {
  // 这里根据文件的iFileID生成相应的路径
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/%d.f", m_sPath.c_str(), iFileID);
  // 看看文件是否存在
  int ret = access(sFilePath, F_OK);
  if (ret == -1) {
    PLG1Debug("file not exist, filepath %s", sFilePath);
    return 1;
  }

  int iFd = -1;
  // 打开文件
  ret = OpenFile(iFileID, iFd);
  if (ret != 0) {
    return ret;
  }
  // 看看文件的长度
  int iFileLen = lseek(iFd, 0, SEEK_END);
  // 如果文件的长度不对
  if (iFileLen == -1) {
    close(iFd);
    return -1;
  }
  // 移动到offset位置
  off_t iSeekPos = lseek(iFd, iOffset, SEEK_SET);
  if (iSeekPos == -1) {
    close(iFd);
    return -1;
  }
  // 当前的位置
  int iNowOffset = iOffset;
  // 是否需要被截断
  bool bNeedTruncate = false;

  while (true) {
    int iLen = 0;
    // 从offset开始读出一个int
    ssize_t iReadLen = read(iFd, (char *)&iLen, sizeof(int));
    // 如果值为0，那么表示读到了文件的尾巴
    if (iReadLen == 0) {
      PLG1Head("File End, fileid %d offset %d", iFileID, iNowOffset);
      iNowFileWriteOffset = iNowOffset;
      break;
    }
    // 如果读出来的长度与sizeof(int): 表示已经遇到末尾了
    // 那么读到末尾的可能性：
    // 1. 文件太小
    // 不一样，那么需要truncate
    // 这里可能会导致32位与64位不见兼容
    // 最好是明确指明使用int32_t或者int64_t
    if (iReadLen != (ssize_t)sizeof(int)) {
      bNeedTruncate = true;
      PLG1Err("readlen %zd not qual to %zu, need truncate", iReadLen, sizeof(int));
      break;
    }

    // 如果读出来的内容为0
    // 那么退出
    // iLen表示写入的数据的长度
    if (iLen == 0) {
      PLG1Head("File Data End, fileid %d offset %d", iFileID, iNowOffset);
      iNowFileWriteOffset = iNowOffset;
      break;
    }
    // 如果写入的数据的长度比文件的长度还要长，那么肯定是出错了
    if (iLen > iFileLen || iLen < (int)sizeof(uint64_t)) {
      PLG1Err("File data len wrong, data len %d filelen %d",
              iLen, iFileLen);
      ret = -1;
      break;
    }

    // 这里接着把iLen个byte的数据读出来
    m_oTmpBuffer.Ready(iLen);
    iReadLen = read(iFd, m_oTmpBuffer.GetPtr(), iLen);
    if (iReadLen != iLen) {
      bNeedTruncate = true;
      PLG1Err("readlen %zd not qual to %zu, need truncate", iReadLen, iLen);
      break;
    }

    // 从数据头里面取出intance id
    uint64_t llInstanceID = 0;
    memcpy(&llInstanceID, m_oTmpBuffer.GetPtr(), sizeof(uint64_t));

    // 如果取出的instance id与前面leveldb中读出来的instance id
    // 还小，那么肯定出问题了
    // 因为这里取的是最新的file ID
    // leveldb中记录的格式是：instance id -> <file_id, offset, checksum>
    // 那么偏移到这个位置开始读的时候
    // 肯定需要是读出来的instance id >= leveldb中读出业的instance id
    //InstanceID must be ascending order.
    if (llInstanceID < llNowInstanceID) {
      // 这个时候出的错应该是属于逻辑错误!!
      // 不truncate是为了重新trouble shooting?
      PLG1Err("File data wrong, read instanceid %lu smaller than now instanceid %lu",
              llInstanceID, llNowInstanceID);
      ret = -1;
      break;
    }
    // 当更新当前的instance id
    llNowInstanceID = llInstanceID;

    // 这里开始解析数据部分
    // 也就是说wal log文件里面存放的是
    // acceptor state data.
    AcceptorStateData oState;
    // 如果解析的数据是无效的，那么直接把后面的内容
    // truncate
    // 所以wal log与level db的wal log设计是不一样的
    // 在level db的设计中，只要某个条目不一样。
    // 那么只需要跳过这个record就可以了。 后面的record还是可以继续工作
    // 可能对于paxos来说，正确性的保证更加重要。所以这里直接扔掉/
    // 并且后面的内容，万一没有，还可以从别的地方学习。
    bool bBufferValid = oState.ParseFromArray(m_oTmpBuffer.GetPtr() + sizeof(uint64_t), iLen - sizeof(uint64_t));
    if (!bBufferValid) {
      m_iNowFileOffset = iNowOffset;
      PLG1Err("This instance's buffer wrong, can't parse to acceptState, instanceid %lu bufferlen %d nowoffset %d",
              llInstanceID, iLen - sizeof(uint64_t), iNowOffset);
      bNeedTruncate = true;
      break;
    }
    // 这里检查check sum
    uint32_t iFileCheckSum = crc32(0, (const uint8_t *)m_oTmpBuffer.GetPtr(), iLen, CRC32SKIP);

    string sFileID;
    // 这里重新生成<file id, offset, checksum> -> sFileID
    GenFileID(iFileID, iNowOffset, iFileCheckSum, sFileID);
    // 把<instnace id, sFileID> 这个key/value存放到leveldb里面。
    ret = poDatabase->RebuildOneIndex(llInstanceID, sFileID);
    // 如果出错，跳出去，但是并不会进行truncate.
    if (ret != 0) {
      break;
    }

    PLG1Imp("rebuild one index ok, fileid %d offset %d instanceid %lu checksum %u buffer size %zu",
            iFileID, iNowOffset, llInstanceID, iFileCheckSum, iLen - sizeof(uint64_t));
    // 操作成功，那么继续向前移动
    iNowOffset += sizeof(int) + iLen;
  } // end while (true);

  close(iFd);

  // 是否需要截断?
  // 如果需要，那么从这个位置开始，后面的内容都不要了。
  if (bNeedTruncate) {
    m_oFileLogger.Log("truncate fileid %d offset %d filesize %d",
                      iFileID, iNowOffset, iFileLen);
    if (truncate(sFilePath, iNowOffset) != 0) {
      PLG1Err("truncate fail, file path %s truncate to length %d errno %d",
              sFilePath, iNowOffset, errno);
      return -1;
    }
  }

  return ret;
}

//////////////////////////////////////////////////////////

LogStoreLogger::LogStoreLogger()
  : m_iLogFd(-1) {
}

LogStoreLogger::~LogStoreLogger() {
  if (m_iLogFd != -1) {
    close(m_iLogFd);
  }
}

void LogStoreLogger::Init(const std::string & sPath) {
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/LOG", sPath.c_str());
  m_iLogFd = open(sFilePath, O_CREAT | O_RDWR | O_APPEND, S_IWRITE | S_IREAD);
}

void LogStoreLogger::Log(const char * pcFormat, ...) {
  if (m_iLogFd == -1) {
    return;
  }

  uint64_t llNowTime = Time::GetTimestampMS();
  time_t tNowTimeSeconds = (time_t)(llNowTime / 1000);
  tm * local_time = localtime(&tNowTimeSeconds);
  char sTimePrefix[64] = {0};
  strftime(sTimePrefix, sizeof(sTimePrefix), "%Y-%m-%d %H:%M:%S", local_time);

  char sPrefix[128] = {0};
  snprintf(sPrefix, sizeof(sPrefix), "%s:%d ", sTimePrefix, (int)(llNowTime % 1000));
  string sNewFormat = string(sPrefix) + pcFormat + "\n";

  char sBuf[1024] = {0};
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), sNewFormat.c_str(), args);
  va_end(args);

  int iLen = strnlen(sBuf, sizeof(sBuf));
  ssize_t iWriteLen = write(m_iLogFd, sBuf, iLen);
  if (iWriteLen != iLen) {
    PLErr("fail, len %d writelen %d", iLen, iWriteLen);
  }
}

}


