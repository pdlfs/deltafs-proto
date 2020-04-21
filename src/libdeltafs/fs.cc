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
  return DirIndex::RandomServer(Slice(tmp, p - tmp), 0);
}

Status Filesystem::Lokup(  ///
    const User& who, const LookupStat& p, const Slice& name, LookupStat* stat) {
  DirId at(p);
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    mutex_.Unlock();
    s = Lokup1(who, at, name, dir, p, stat);
    mutex_.Lock();
    Release(dir);
  }
  return s;
}

Status Filesystem::Lstat(  ///
    const User& who, const LookupStat& p, const Slice& name, Stat* stat) {
  DirId at(p);
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    mutex_.Unlock();
    s = Lstat1(who, at, name, dir, p, stat);
    mutex_.Lock();
    Release(dir);
  }
  return s;
}

// Note: *n is both input and output.
Status Filesystem::Mkfls(  ///
    const User& who, const LookupStat& parent, const Slice& namearr,
    uint32_t mode, uint32_t* n) {
  DirId at(parent);
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    inoq_ += *n;
    uint64_t myino = inoq_;  // The last ino for the batch
    uint64_t startino = myino - *n + 1;
    const uint32_t t = S_IFREG;
    mutex_.Unlock();
    s = Mknos1(who, at, namearr, startino, t, mode, parent, dir, n);
    mutex_.Lock();
    // Reuse inodes left by the batch
    if (!s.ok()) {
      uint64_t x = myino - startino + 1;
      TryReuseIno(myino, x - *n);
    }
    Release(dir);
  }
  return s;
}

Status Filesystem::Mkfle(  ///
    const User& who, const LookupStat& parent, const Slice& name, uint32_t mode,
    Stat* stat) {
  DirId at(parent);
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    uint64_t myino = ++inoq_;
    const uint32_t t = S_IFREG;
    mutex_.Unlock();
    s = Mknod1(who, at, name, myino, t, mode, parent, dir, stat);
    mutex_.Lock();
    if (!s.ok()) {
      TryReuseIno(myino);
    }
    Release(dir);
  }
  return s;
}

Status Filesystem::Mkdir(  ///
    const User& who, const LookupStat& parent, const Slice& name, uint32_t mode,
    Stat* stat) {
  DirId at(parent);
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    uint64_t myino = ++inoq_;
    const uint32_t t = S_IFDIR;
    mutex_.Unlock();
    s = Mknod1(who, at, name, myino, t, mode, parent, dir, stat);
    mutex_.Lock();
    if (!s.ok()) {
      TryReuseIno(myino);
    }
    Release(dir);
  }
  return s;
}

Status Filesystem::TEST_ProbeDir(const DirId& at) {
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    Release(dir);
  }
  return s;
}

uint32_t Filesystem::TEST_TotalDirsInMemory() {
  MutexLock lock(&mutex_);
  return dirs_->Size();
}

uint64_t Filesystem::TEST_LastIno() {
  MutexLock lock(&mutex_);
  return inoq_;
}

namespace {
bool IsDirPartitionOk(const FilesystemOptions& options, const DirIndex* giga,
                      const Slice& name) {
  if (options.skip_partition_checks) {
    return true;
  } else {
    return (options.srvid == giga->SelectServer(name));
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

// Check if a given user has the "w" permission beneath a parent
// directory based on a lease certificate provided by the user.
bool IsDirWriteOk(const FilesystemOptions& options, const LookupStat& parent,
                  const User& who) {
  const uint32_t mode = parent.DirMode();
  if (options.skip_perm_checks) {
    return true;
  } else if (who.uid == 0) {
    return true;
  } else if (who.uid == uid(parent) && (mode & S_IWUSR) == S_IWUSR) {
    return true;
  } else if (who.gid == gid(parent) && (mode & S_IWGRP) == S_IWGRP) {
    return true;
  } else {
    return ((mode & S_IWOTH) == S_IWOTH);
  }
}

// Check if a given user has the "x" permission beneath a parent
// directory based on a lease certificate provided by the user.
bool IsLookupOk(const FilesystemOptions& options, const LookupStat& parent,
                const User& who) {
  const uint32_t mode = parent.DirMode();
  if (options.skip_perm_checks) {
    return true;
  } else if (who.uid == 0) {
    return true;
  } else if (who.uid == uid(parent) && (mode & S_IXUSR) == S_IXUSR) {
    return true;
  } else if (who.gid == gid(parent) && (mode & S_IXGRP) == S_IXGRP) {
    return true;
  } else {
    return ((mode & S_IXOTH) == S_IXOTH);
  }
}

bool IsLeaseOk(  ///
    const FilesystemOptions& options, const LookupStat& parent,
    const uint64_t ts) {
  if (options.skip_lease_due_checks) {
    return true;
  } else {
    return (parent.LeaseDue() >= ts);
  }
}
}  // namespace

Status Filesystem::Lokup1(  ///
    const User& who, const DirId& at, const Slice& name, Dir* dir,
    const LookupStat& p, LookupStat* stat) {
  Stat tmp;
  Status s = Lstat1(who, at, name, dir, p, &tmp);
  if (s.ok()) {
    if (!S_ISDIR(tmp.FileMode())) {
      s = Status::DirExpected(Slice("Not a dir"));
    } else {
      stat->CopyFrom(tmp);
      stat->SetLeaseDue(-1);
      stat->AssertAllSet();
    }
  }
  return s;
}

Status Filesystem::Lstat1(  ///
    const User& who, const DirId& at, const Slice& name, Dir* dir,
    const LookupStat& p, Stat* stat) {
  if (!IsLeaseOk(options_, p, CurrentMicros()))
    return Status::AccessDenied("Lease has expired");
  if (!IsLookupOk(options_, p, who))
    return Status::AccessDenied("No dir x perm");
  if (!options_.skip_partition_checks) {
    MutexLock lock(dir->mu);
    Status s = MaybeFetchDir(dir);
    if (!s.ok()) {
      return s;
    }
    // XXX: obtain split lock here to secure directory split status
    if (!IsDirPartitionOk(options_, dir->giga, name))
      return Status::AccessDenied("Wrong dir partition");
  }

  // The following Get() operation goes unlocked with an assumption
  // that the directory partition it just checked won't be later
  // split --- the name is still "in" the partition so that
  // the read is still reading against the right server.
  // A directory partition is split when it grows large. When a
  // directory partition is split, it is divided into two halves.
  // One of the two halves stays in the server. The other is moved
  // into another server.
  // The read operation may read a key that belongs to the half
  // of partition that will be moved should directory splitting happen.
  // If the read operation is context switched out before it performs
  // the read and switched back after a directory split, it may miss
  // the key.
  // To ensure that the read operation can still read the key as if
  // the directory had not been split, we can have it read from a db
  // snapshot. If the name is "in" the half of partition that might
  // be moved out, the read reads from the snapshot. Otherwise, the
  // read still reads the latest db.
  // Db snapshots are installed by the operation that performs a split.
  // Db snapshots are reference counted. If an operation performing
  // a split finds a previously installed snapshot, it waits until
  // the last reference to the snapshot is released before proceeding.
  // An advantage of this approach is that when a directory
  // splitting happens, it effectively treats the half of partition
  // that needs to be moved as read only so that read operations can
  // still go during an entire split and only write operations against
  // the half of partition are blocked by the split.
  //
  // Another more straightforward way to do this is to have a read
  // operation install a status in the directory control block to
  // block any subsequent split from happening. At the same time,
  // any ongoing split will block reads from happening, like a read
  // write lock would do. We may call it a per-directory split lock.
  // The lock has 2 phases. In phase 1, all write operations against
  // the half of partition to be moved are blocked. The split
  // operation copies the half to another server. Read operations
  // are still allowed during this phase. In phase 2, both read and
  // write operations are blocked. The split operation bulk deletes
  // the half of partition that has been moved and install a new
  // directory index.
  return mdb_->Get(at, name, stat);
}

Status Filesystem::Mknos1(  ///
    const User& who, const DirId& at, const Slice& namearr, uint64_t startino,
    uint32_t type, uint32_t mode, const LookupStat& p, Dir* const dir,
    uint32_t* const n) {
  if (!IsLeaseOk(options_, p, CurrentMicros()))
    return Status::AccessDenied("Lease has expired");
  if (!IsDirWriteOk(options_, p, who))
    return Status::AccessDenied("No write perm");
  MutexLock lock(dir->mu);
  Status s = MaybeFetchDir(dir);
  if (!s.ok()) {
    return s;
  }
  dir->mu->AssertHeld();  // TODO: lock directory split status
  // Lock all name subsets...
  for (uint32_t i = 0; i < kWays; i++) {
    while (dir->busy[i]) {
      dir->cv->Wait();
    }
    dir->busy[i] = true;
  }
  dir->mu->Unlock();
  Stat tmp;
  size_t m = 0;
  Slice input = namearr;
  Slice name;
  while (m < (*n) && GetLengthPrefixedSlice(&input, &name)) {
    s = CheckAndPut(who, at, name, startino + m, type, mode, &tmp);
    if (!s.ok()) {
      break;
    }
    m++;
  }
  *n = m;
  dir->mu->Lock();
  for (uint32_t i = 0; i < kWays; i++) {
    dir->busy[i] = false;
  }
  dir->cv->SignalAll();
  return s;
}

Status Filesystem::Mknod1(  ///
    const User& who, const DirId& at, const Slice& name, uint64_t myino,
    uint32_t type, uint32_t mode, const LookupStat& p, Dir* const dir,
    Stat* const stat) {
  if (!IsLeaseOk(options_, p, CurrentMicros()))
    return Status::AccessDenied("Lease has expired");
  if (!IsDirWriteOk(options_, p, who))
    return Status::AccessDenied("No write perm");
  MutexLock lock(dir->mu);
  Status s = MaybeFetchDir(dir);
  if (!s.ok()) {
    return s;
  }
  // XXX: obtain directory split lock here to secure directory split status
  if (!IsDirPartitionOk(options_, dir->giga, name))
    return Status::AccessDenied("Wrong dir partition");
  // Lock the corresponding name subset in the partition for serialization...
  // The best performance is achieved when a different hash function is used
  // as the one used for directory splits
  uint32_t hash = Hash(name.data(), name.size(), 0);
  uint32_t i = hash & uint32_t(kWays - 1);
  // Wait for conflicting writes
  while (dir->busy[i]) dir->cv->Wait();
  dir->busy[i] = true;
  // Temporarily unlock for db operations
  dir->mu->Unlock();
  s = CheckAndPut(who, at, name, myino, type, mode, stat);
  dir->mu->Lock();
  dir->busy[i] = false;
  dir->cv->SignalAll();
  return s;
}

Status Filesystem::CheckAndPut(  ///
    const User& who, const DirId& at, const Slice& name, uint64_t myino,
    uint32_t type, uint32_t mode, Stat* stat) {
  Status s;
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

// Called when the last reference to a directory control block is removed.
void Filesystem::DeleteDir(const Slice& key, Dir* dir) {
  assert(dir->key() == key);
  Filesystem* const fs = dir->fs;
  fs->mutex_.AssertHeld();
  fs->dirs_->Remove(dir->key(), dir->hash);
  delete dir->giga_opts;
  delete dir->giga;
  delete dir->cv;
  delete dir->mu;
  free(dir);
}

// Remove an active reference to a directory control block. After removal, the
// control block may still be kept in memory by the LRU cache. If the control
// block has been evicted from the cache before, it will be deleted.
void Filesystem::Release(Dir* const dir) {
  mutex_.AssertHeld();
  dlru_->Release(dir->lru_handle);
}

// Fetch information from db if we haven't done so yet.
Status Filesystem::MaybeFetchDir(Dir* dir) {
  Status s;
  dir->mu->AssertHeld();
  if (dir->fetched) {
    return s;
  }

  dir->giga_opts = new DirIndexOptions;
  dir->giga_opts->num_virtual_servers = options_.vsrvs;
  dir->giga_opts->num_servers = options_.nsrvs;

  const uint32_t zsrv = PickupServer(dir->id);
  dir->giga = new DirIndex(zsrv, dir->giga_opts);
  dir->giga->SetAll();

  dir->fetched = true;
  return s;
}

// We serialize filesystem metadata operations at a per-directory basis. A
// control block is allocated for each directory to synchronize a variety of
// operations within that directory. An LRU cache of directory control blocks is
// kept in memory to avoid repeatedly allocating directory control blocks. A
// separate hash table is created to index all directory control blocks that
// are currently kept in memory including those that have been evicted from the
// cache but are still in use by some threads. When obtaining a control block,
// we first look it up at the cache. If we cannot find it, we continue the
// search at the table. If we still cannot find it, we create a new and insert
// it into the LRU cache and the hash table.
Status Filesystem::AcquireDir(const DirId& id, Dir** result) {
  mutex_.AssertHeld();
  char tmp[30];
  Slice key = LRUKey(id, tmp);
  const uint32_t hash = LRUHash(key);
  Status s;

  Dir* dir;
  // We start our search at the LRU cache. If we find it we are done.
  DirHandl* h = dlru_->Lookup(key, hash);
  if (h != NULL) {
    dir = h->value;
    *result = dir;
    assert(dir->lru_handle == h);
    return s;
  }

  // If we cannot find an entry in the cache, we continue our search at the
  // bigger hash table. We cache the cursor position returned by the table so
  // that we can reuse it in the subsequent table insertion.
  Dir** const pos = dirs_->FindPointer(key, hash);
  dir = *pos;
  if (dir != NULL) {
    *result = dir;
    assert(dir->lru_handle->value == dir);
    // If we hit an entry at the table, we increase the reference count of the
    // entry's corresponding handle in the LRU cache. If this entry has been
    // evicted from the cache, this will further defer its deletion from the
    // memory, which is our intention here. Should we reinsert it into the cache
    // though?
    dlru_->Ref(dir->lru_handle);
    return s;
  }

  // If we still cannot find the entry, we create it and insert it into the
  // cache and the table.
  dir = static_cast<Dir*>(malloc(sizeof(Dir) - 1 + key.size()));
  dir->key_length = key.size();
  memcpy(dir->key_data, key.data(), key.size());
  dir->hash = hash;
  dir->id = id;
  dir->mu = new port::Mutex;
  dir->cv = new port::CondVar(dir->mu);
  dir->fs = this;
  memset(&dir->busy[0], 0, sizeof(dir->busy));
  dir->giga = NULL;  // To be fetched from db later
  dir->giga_opts = NULL;
  dir->fetched = 0;
  dirs_->Inject(dir, pos);

  h = dlru_->Insert(key, hash, dir, 1, DeleteDir);
  dir->lru_handle = h;
  *result = dir;
  return s;
}

FilesystemOptions::FilesystemOptions()
    : dir_lru_size(4096),
      skip_partition_checks(false),
      skip_name_collision_checks(false),
      skip_lease_due_checks(false),
      skip_perm_checks(false),
      rdonly(false),
      vsrvs(1),
      nsrvs(1),
      srvid(0),
      mydno(0) {}

Filesystem::Filesystem(const FilesystemOptions& options)
    : inoq_(0), options_(options), mdb_(NULL) {
  dlru_ = new LRUCache<DirHandl>(options_.dir_lru_size);
  dirs_ = new HashTable<Dir>();
  mfac_ = new MDBFactory;
}

Filesystem::~Filesystem() {
  delete dlru_;
  assert(dirs_->Empty());
  delete dirs_;
  delete mdb_;  // Must be deleted before mfac_
  delete mfac_;
}

Status Filesystem::OpenFilesystem(const std::string& fsloc) {
  return mfac_->OpenMDB(fsloc, &mdb_);
}

}  // namespace pdlfs
