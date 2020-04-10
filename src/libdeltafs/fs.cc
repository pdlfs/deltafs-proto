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
#include "fs.h"

#include "pdlfs-common/env.h"
#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/hash.h"
#include "pdlfs-common/mutexlock.h"

#include <sys/stat.h>

namespace pdlfs {

uint32_t Filesystem::PickupServer(const DirId& id) {
  char tmp[16];
  char* p = tmp;
  EncodeFixed64(p, id.dno);
  p += 8;
  EncodeFixed64(p, id.ino);
  p += 8;
  int rnd = DirIndex::RandomServer(Slice(tmp, p - tmp), 0);
  return rnd % options_.vsrvs;
}

namespace {

bool IsDirPartitionOk(const FilesystemOptions& options, const DirIndex& giga,
                      const Slice& name) {
  if (options.skip_partition_checks) {
    return true;
  } else {
    return (options.srvid == giga.SelectServer(name));
  }
}

// Return the owner of a given directory.
inline uint32_t uid(const LookupStat& dir) {  ///
  return dir.UserId();
}

// Return the group owner of a given directory.
inline uint32_t gid(const LookupStat& dir) {  ///
  return dir.GroupId();
}

bool IsDirWriteOk(const FilesystemOptions& options, const LookupStat& parent,
                  const User& who, const uint64_t ts) {
  const uint32_t mode = parent.DirMode();
  if (options.skip_perm_checks) {
    return true;
  } else if (who.uid == 0) {
    return true;
  } else if (parent.LeaseDue() < ts) {
    return false;
  } else if (who.uid == uid(parent) && (mode & S_IWUSR) == S_IWUSR) {
    return true;
  } else if (who.gid == gid(parent) && (mode & S_IWGRP) == S_IWGRP) {
    return true;
  } else if ((mode & S_IWOTH) == S_IWOTH) {
    return true;
  } else {
    return false;
  }
}

}  // namespace

Status Filesystem::Mkfle(  ///
    const User& who, const DirId& at, const Slice& name, uint32_t mode,
    const LookupStat& p, Stat* stat) {
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    uint64_t myino = ++inoq_;
    const uint32_t t = S_IFREG;
    mutex_.Unlock();
    s = Mknod1(who, at, name, myino, t, mode, p, dir, stat);
    mutex_.Lock();
    if (!s.ok()) {
      TryReuseIno(myino);
    }
    Release(dir);
  }
  return s;
}

Status Filesystem::Mkdir(  ///
    const User& who, const DirId& at, const Slice& name, uint32_t mode,
    const LookupStat& p, Stat* stat) {
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    uint64_t myino = ++inoq_;
    const uint32_t t = S_IFDIR;
    mutex_.Unlock();
    s = Mknod1(who, at, name, myino, t, mode, p, dir, stat);
    mutex_.Lock();
    if (!s.ok()) {
      TryReuseIno(myino);
    }
    Release(dir);
  }
  return s;
}

Status Filesystem::Mknod1(  ///
    const User& who, const DirId& at, const Slice& name, uint64_t myino,
    uint32_t type, uint32_t mode, const LookupStat& p, Dir* const dir,
    Stat* stat) {
  MutexLock lock(dir->mu);
  const uint64_t ts = CurrentMicros();
  if (!IsDirPartitionOk(options_, *dir->giga, name))
    return Status::AccessDenied("Wrong dir partition");
  if (!IsDirWriteOk(options_, p, who, ts))
    return Status::AccessDenied("No write perm");

  Status s;
  while (dir->busy) {
    dir->cv->Wait();
  }
  dir->busy = true;
  // Temporarily unlock for db accesses
  dir->mu->Unlock();
  if (!options_.skip_name_collision_checks) {
    s = mdb_->Get(at, name, stat);
    if (s.ok()) {
      s = Status::AlreadyExists(Slice());
    } else if (s.IsNotFound()) {
      s = Status::OK();
    }
  }
  if (s.ok()) {
    uint64_t mydno = options_.mydno;
    uint32_t mymo = type;
    mymo |= (mode & ACCESSPERMS);
    s = Put(who, at, name, mydno, myino, PickupServer(DirId(mydno, myino)),
            mymo, stat);
  }
  dir->mu->Lock();
  Unbusy(dir);
  return s;
}

Status Filesystem::Put(  ///
    const User& who, const DirId& at, const Slice& name, uint64_t mydno,
    uint64_t myino, uint32_t zsrv, uint32_t mymo, Stat* stat) {
  stat->SetDnodeNo(mydno);
  stat->SetInodeNo(myino);
  stat->SetZerothServer(zsrv);
  stat->SetFileMode(mymo);
  stat->SetFileSize(0);
  stat->SetUserId(who.uid);
  stat->SetGroupId(who.gid);
  stat->SetModifyTime(0);
  stat->SetChangeTime(0);
  stat->AssertAllSet();
  return mdb_->Set(at, name, *stat);
}

namespace {
Slice LRUKey(const DirId& id, char* const scratch) {
  char* p = scratch;
  EncodeFixed64(p, id.dno);
  p += 8;
  EncodeFixed64(p, id.ino);
  p += 8;
  return Slice(scratch, p - scratch);
}

uint32_t LRUHash(const Slice& k) { return Hash(k.data(), k.size(), 0); }

}  // namespace

void Filesystem::FreeDir(const Slice& key, Dir* dir) {
  assert(dir->key() == key);
  assert(!dir->in_use);
  delete dir->cv;
  delete dir->mu;
  free(dir);
}

void Filesystem::Release(Dir* dir) {
  mutex_.AssertHeld();
  assert(dir->in_use != 0);
  dir->in_use--;
  if (!dir->in_use) {
    diu_->Remove(dir->key(), dir->hash);
  }
  dlru_->Release(dir->lru_handle);
}

Status Filesystem::AcquireDir(const DirId& id, Dir** result) {
  mutex_.AssertHeld();
  char tmp[30];
  Slice key = LRUKey(id, tmp);
  const uint32_t hash = LRUHash(key);
  Status s;

  // Cursor position in the table is cached and can be reused in
  // subsequent table insertions.
  Dir** const pos = diu_->FindPointer(key, hash);
  Dir* dir = *pos;
  if (dir != NULL) {
    *result = dir;
    assert(dir->lru_handle->value == dir);
    // This could prevent an entry having
    // been evicted from the cache from being removed
    // from the memory.
    dlru_->Ref(dir->lru_handle);
    assert(dir->in_use != 0);
    dir->in_use++;
    return s;
  }

  DirHandl* h = dlru_->Lookup(key, hash);
  if (h != NULL) {
    dir = h->value;
    *result = dir;
    assert(dir->lru_handle == h);
    diu_->Inject(dir, pos);
    assert(dir->in_use == 0);
    dir->in_use = 1;
    return s;
  }

  dir = static_cast<Dir*>(malloc(sizeof(Dir) - 1 + key.size()));
  dir->dno = id.dno;
  dir->ino = id.ino;
  dir->key_length = key.size();
  memcpy(dir->key_data, key.data(), key.size());
  dir->hash = hash;
  dir->mu = new port::Mutex;
  dir->cv = new port::CondVar(dir->mu);
  dir->busy = 0;

  h = dlru_->Insert(key, hash, dir, 1, FreeDir);
  *result = dir;
  dir->lru_handle = h;
  diu_->Inject(dir, pos);
  dir->in_use = 1;
  return s;
}

}  // namespace pdlfs
