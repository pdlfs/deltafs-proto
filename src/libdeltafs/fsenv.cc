/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "fsenv.h"

#include "pdlfs-common/leveldb/filenames.h"

#include "pdlfs-common/mutexlock.h"

#include <string.h>

namespace pdlfs {

FilesystemDbEnvWrapper::FilesystemDbEnvWrapper(
    const FilesystemDbOptions& options)
    : EnvWrapper(Env::GetUnBufferedIoEnv()), options_(options) {}

namespace {
template <typename T>
inline void CleanUpRepo(std::list<T*>* v) {
  typename std::list<T*>::iterator it = v->begin();
  for (; it != v->end(); ++it) {
    delete *it;
  }
  v->clear();
}

}  // namespace

FilesystemDbEnvWrapper::~FilesystemDbEnvWrapper() { Reset(); }

void FilesystemDbEnvWrapper::Reset() {
  MutexLock ml(&mu_);
  CleanUpRepo(&sequentialfile_repo_);
  CleanUpRepo(&randomaccessfile_repo_);
  CleanUpRepo(&writablefile_repo_);
}

namespace {
template <typename T>
inline uint64_t SumUpBytes(const std::list<T*>* v) {
  uint64_t result = 0;
  typename std::list<T*>::const_iterator it = v->begin();
  for (; it != v->end(); ++it) {
    result += (*it)->TotalBytes();
  }
  return result;
}

}  // namespace

size_t FilesystemDbEnvWrapper::TotalTableFilesOpenedForWrite() {
  MutexLock l(&mu_);
  return writablefile_repo_.size();
}

uint64_t FilesystemDbEnvWrapper::TotalDbBytesWritten() {
  MutexLock l(&mu_);
  return SumUpBytes(&writablefile_repo_);
}

size_t FilesystemDbEnvWrapper::TotalTableFilesOpenedForRead() {
  MutexLock l(&mu_);
  return randomaccessfile_repo_.size();
}

uint64_t FilesystemDbEnvWrapper::TotalDbBytesRead() {
  MutexLock l(&mu_);
  return SumUpBytes(&randomaccessfile_repo_);
}

void FilesystemDbEnvWrapper::SetDbLoc(const std::string& dbloc) {
  dbprefix_ = dbloc + "/";
}

namespace {
bool IsTableFile(const std::string& dbprefix, const char* filename) {
  uint64_t filenum;
  FileType type;
  if (strncmp(filename, dbprefix.c_str(), dbprefix.size()) == 0 &&
      ParseFileName(filename + dbprefix.size(), &filenum, &type)) {
    return type == kTableFile;
  } else {
    return false;
  }
}
}  // namespace

Status FilesystemDbEnvWrapper::NewSequentialFile(  ///
    const char* f, SequentialFile** r) {
  SequentialFile* file;
  Status s = target()->NewSequentialFile(f, &file);
  if (!s.ok()) {
    *r = NULL;
  } else if (options_.enable_io_monitoring && IsTableFile(dbprefix_, f)) {
    MutexLock ml(&mu_);
    SequentialFileStats* const stats = new SequentialFileStats;
    *r = new MonitoredSequentialFile(stats, file);
    sequentialfile_repo_.push_back(stats);
  } else {
    *r = file;
  }
  return s;
}

Status FilesystemDbEnvWrapper::NewRandomAccessFile(  ///
    const char* f, RandomAccessFile** r) {
  RandomAccessFile* file;
  Status s = target()->NewRandomAccessFile(f, &file);
  if (!s.ok()) {
    *r = NULL;
  } else if (options_.enable_io_monitoring && IsTableFile(dbprefix_, f)) {
    MutexLock ml(&mu_);
    RandomAccessFileStats* const stats = new RandomAccessFileStats;
    *r = new MonitoredRandomAccessFile(stats, file);
    randomaccessfile_repo_.push_back(stats);
  } else {
    *r = file;
  }
  return s;
}

Status FilesystemDbEnvWrapper::NewWritableFile(  ///
    const char* f, WritableFile** r) {
  WritableFile* file;
  Status s = target()->NewWritableFile(f, &file);
  if (!s.ok()) {
    *r = NULL;
  } else if (options_.enable_io_monitoring && IsTableFile(dbprefix_, f)) {
    MutexLock ml(&mu_);
    WritableFileStats* const stats = new WritableFileStats;
    *r = new pdlfs::MonitoredWritableFile(stats, file);
    writablefile_repo_.push_back(stats);
  } else {
    *r = file;
  }
  return s;
}

}  // namespace pdlfs
