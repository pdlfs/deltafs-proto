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
#include "fscli.h"

#include "fs.h"
#include "fsdb.h"

#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/mutexlock.h"

#include <sys/stat.h>

namespace pdlfs {
namespace {
Status Nofs() {  ///
  return Status::AssertionFailed("No filesystem backend");
}
}  // namespace

FilesystemCli::UriMapper::~UriMapper() {}

Status FilesystemCli::TEST_Mkfle(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent, const Slice& fname,
    const Stat& stat, FilesystemDbStats* const stats) {
  Status status;
  if (fname.empty()) {
    status = Status::AssertionFailed("tgt is empty");
  } else {
    MutexLock lock(&mutex_);
    Partition* part;
    Dir* dir;
    int i;
    status = AcquireAndFetch(ctx, parent, fname, &dir, &i);
    if (status.ok()) {
      status = AcquirePartition(dir, i, &part);
      if (status.ok()) {
        mutex_.Unlock();
        status = fs_->TEST_Mkfle(ctx->who, parent, fname, stat, stats);
        mutex_.Lock();
        Release(part);
      }
      Release(dir);
    }
  }
  return status;
}

Status FilesystemCli::TEST_Mkfle(  ///
    FilesystemCliCtx* const ctx, const char* pathname, const Stat& stat,
    FilesystemDbStats* const stats) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, NULL, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    const LookupStat& p = *parent_dir->rep;
    status = TEST_Mkfle(ctx, p, tgt, stat, stats);
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::TEST_Lstat(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent, const Slice& fname,
    Stat* const stat, FilesystemDbStats* const stats) {
  Status status;
  if (fname.empty()) {
    status = Status::AssertionFailed("tgt is empty");
  } else {
    MutexLock lock(&mutex_);
    Partition* part;
    Dir* dir;
    int i;
    status = AcquireAndFetch(ctx, parent, fname, &dir, &i);
    if (status.ok()) {
      status = AcquirePartition(dir, i, &part);
      if (status.ok()) {
        mutex_.Unlock();
        status = fs_->TEST_Lstat(ctx->who, parent, fname, stat, stats);
        mutex_.Lock();
        Release(part);
      }
      Release(dir);
    }
  }
  return status;
}

Status FilesystemCli::TEST_Lstat(  ///
    FilesystemCliCtx* const ctx, const char* pathname, Stat* const stat,
    FilesystemDbStats* const stats) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, NULL, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    const LookupStat& p = *parent_dir->rep;
    status = TEST_Lstat(ctx, p, tgt, stat, stats);
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

// Relative root of a pathname
struct FilesystemCli::AT {
  // Look up stat of the parent directory of the relative root
  LookupStat parent_of_root;
  // Name of the relative root under its parent
  std::string name;
};

Status FilesystemCli::Atdir(  ///
    FilesystemCliCtx* const ctx, const AT* const at, const char* pathname,
    AT** result) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      AT* rv = new AT;
      rv->parent_of_root = *parent_dir->rep;
      rv->name = tgt.ToString();
      *result = rv;
    } else {  // Special case for root
      *result = NULL;
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

void FilesystemCli::Destroy(AT* at) { delete at; }

// Each batch instance is backed by a reference to a server-issued lease with
// batch creation permissions.
struct FilesystemCli::BAT {
  Lease* dir_lease;
};

// On success, the returned batch handle contains a reference to a special lease
// of the target directory and a reference to the internal batch context object
// associated with the lease.
Status FilesystemCli::BatchInit(  ///
    FilesystemCliCtx* const ctx, const AT* at, const char* pathname,
    BAT** result) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      Lease* dir_lease;
      // This should ideally be a special mkdir creating a new dir and
      // simultaneously locking the newly created dir. Any subsequent regular
      // lookup operation either finds a non-regular lease with a batch context
      // or fails to initialize a regular lease from server.
      status = Lokup(ctx, *parent_dir->rep, tgt, kBatchedCreats, &dir_lease);
      if (status.ok()) {
        assert(dir_lease->mode == kBatchedCreats && dir_lease->batch != NULL);
        BAT* const bat = new BAT;  // Opaque handle to the batch
        bat->dir_lease = dir_lease;
        *result = bat;
      }
    } else {  // Special handling for the root dir
      status = Status::NotSupported(Slice());
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::BatchInsert(BAT* bat, const char* name) {
  assert(bat->dir_lease != NULL);
  Lease* const lease = bat->dir_lease;
  assert(lease->batch != NULL);
  BatchedCreates* const bc = lease->batch;
  MutexLock lock(&bc->mu);
  if (bc->commit_status != 0) {
    return Status::AssertionFailed("Already committed");
  } else if (!bc->bg_status.ok()) {
    return bc->bg_status;
  }
  const int i = bc->dir->giga->SelectServer(name);
  bc->mu.Unlock();
  Status s =
      Mkfls1(bc->ctx, *lease->rep, name, bc->mode, false, i, &bc->wribufs[i]);
  bc->mu.Lock();
  if (!s.ok() && bc->bg_status.ok()) {
    bc->bg_status = s;
  }
  return s;
}

Status FilesystemCli::BatchCommit(BAT* bat) {
  assert(bat->dir_lease != NULL);
  Lease* const lease = bat->dir_lease;
  assert(lease->batch != NULL);
  BatchedCreates* const bc = lease->batch;
  MutexLock lock(&bc->mu);
  if (bc->commit_status == 1) {
    return Status::AssertionFailed("Batch is being committed");
  } else if (bc->commit_status == 2 || !bc->bg_status.ok()) {
    return bc->bg_status;
  }
  bc->commit_status = 1;
  bc->mu.Unlock();
  Status s;
  for (int i = 0; i < srvs_; i++) {
    s = Mkfls1(bc->ctx, *lease->rep, Slice(), bc->mode, true, i,
               &bc->wribufs[i]);
    if (!s.ok()) {
      break;
    }
  }
  bc->mu.Lock();
  bc->commit_status = 2;
  if (!s.ok() && bc->bg_status.ok()) {
    bc->bg_status = s;
  }
  return s;
}

Status FilesystemCli::Destroy(BAT* bat) {
  assert(bat->dir_lease != NULL);
  Lease* const lease = bat->dir_lease;
  assert(lease->batch != NULL);
  BatchedCreates* const bc = lease->batch;
  Partition* part = lease->part;
  part->mu->Lock();
  assert(bc->refs != 0);
  bc->refs--;
  const uint32_t r = bc->refs;
  if (!r) {
    // Non-regular leases are defined by their batch contexts. Once the context
    // is destroyed, the lease is effectively invalidated and therefore can be
    // deleted from the client to prevent locking out subsequent regular
    // directory accesses. Current implementation actually requires such
    // deletion so that the lease lookup code does not need to check the
    // presence of a batch context when obtaining a lease from the lease table
    // or the cache. This, however, only matters when it is possible for one
    // (for example, the lease LRU cache) to reference a lease without also
    // referencing a batch context.
    // XXX: We could remove the lease as early as the batch context is committed
    // XXX: We could skip caching non-regular leases and only put these special
    // leases in the lease table.
    part->cached_leases->Erase(lease->lru_handle);
    part->leases->Remove(lease);  // Eagerly remove the lease from the client
    lease->out = true;
    lease->batch = NULL;
  }
  part->cached_leases->Release(lease->lru_handle);
  part->mu->Unlock();
  {
    MutexLock lock(&mutex_);
    if (!r) Release(bc->dir);
    Release(part);
  }
  if (!r) {
    delete[] bc->wribufs;
    delete bc;
  }
  delete bat;
  return Status::OK();
}

// Each bulk instance is backed by a reference to a server-issued lease with
// bulk insertion permissions.
struct FilesystemCli::BULK {
  Lease* dir_lease;
};

Status FilesystemCli::BulkInit(  ///
    FilesystemCliCtx* const ctx, const AT* at, const char* pathname,
    BULK** result) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      Lease* dir_lease;
      // XXX: This should ideally be a special mkdir operation.
      status = Lokup(ctx, *parent_dir->rep, tgt, kBulkIn, &dir_lease);
      if (status.ok()) {
        assert(dir_lease->mode == kBulkIn && dir_lease->bk != NULL);
        BULK* bulk = new BULK;
        bulk->dir_lease = dir_lease;
        *result = bulk;
      }
    } else {  // Special handling for the root dir
      status = Status::NotSupported(Slice());
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::BulkInsert(BULK* hdl, const char* name) {
  assert(hdl->dir_lease != NULL);
  Lease* const lease = hdl->dir_lease;
  assert(lease->bk != NULL);
  BulkInserts* const bk = lease->bk;
  MutexLock lock(&bk->mu);
  if (bk->commit_status != 0) {
    return Status::AssertionFailed("Already committed");
  } else if (!bk->bg_status.ok()) {
    return bk->bg_status;
  }
  const int i = bk->dir->giga->SelectServer(name);
  bk->mu.Unlock();
  Status s = Bukin1(bk->ctx, *lease->rep, name, false, i, &bk->bulks[i]);
  bk->mu.Lock();
  if (!s.ok() && bk->bg_status.ok()) {
    bk->bg_status = s;
  }
  return s;
}

Status FilesystemCli::BulkCommit(BULK* hdl) {
  assert(hdl->dir_lease != NULL);
  Lease* const lease = hdl->dir_lease;
  assert(lease->bk != NULL);
  BulkInserts* const bk = lease->bk;
  MutexLock lock(&bk->mu);
  if (bk->commit_status == 1) {
    return Status::AssertionFailed("Bulk context is being committed");
  } else if (bk->commit_status == 2 || !bk->bg_status.ok()) {
    return bk->bg_status;
  }
  bk->commit_status = 1;
  bk->mu.Unlock();
  Status s;
  for (int i = 0; i < srvs_; i++) {
    s = Bukin1(bk->ctx, *lease->rep, Slice(), true, i, &bk->bulks[i]);
    if (!s.ok()) {
      break;
    }
  }
  bk->mu.Lock();
  bk->commit_status = 2;
  if (!s.ok() && bk->bg_status.ok()) {
    bk->bg_status = s;
  }
  return s;
}

Status FilesystemCli::Destroy(BULK* hdl) {
  assert(hdl->dir_lease != NULL);
  Lease* const lease = hdl->dir_lease;
  assert(lease->bk != NULL);
  BulkInserts* const bk = lease->bk;
  Partition* part = lease->part;
  part->mu->Lock();
  assert(bk->refs != 0);
  bk->refs--;
  const uint32_t r = bk->refs;
  if (!r && !lease->out) {
    part->cached_leases->Erase(lease->lru_handle);
    part->leases->Remove(lease);
    lease->out = true;
    // Dissociate the bulk context from the lease facilitating subsequent sanity
    // checking
    lease->bk = NULL;
  }
  part->cached_leases->Release(lease->lru_handle);
  part->mu->Unlock();
  {
    MutexLock lock(&mutex_);
    if (!r) Release(bk->dir);
    Release(part);
  }
  if (!r) {
    for (int i = 0; i < srvs_; i++) {
      delete bk->bulks[i].db;
    }
    delete[] bk->bulks;
    delete bk;
  }
  delete hdl;
  return Status::OK();
}

Status FilesystemCli::Mkfle(  ///
    FilesystemCliCtx* const ctx, const AT* const at, const char* pathname,
    const uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty() && !has_tailing_slashes) {
      status = Mkfle1(ctx, *parent_dir->rep, tgt, mode, stat);
    } else {
      status = Status::FileExpected("Path is dir");
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::Mkdir(  ///
    FilesystemCliCtx* const ctx, const AT* const at, const char* pathname,
    const uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      status = Mkdir1(ctx, *parent_dir->rep, tgt, mode, stat);
    } else {  // Special case: pathname is root
      status = Status::AlreadyExists(Slice());
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::Lstat(  ///
    FilesystemCliCtx* const ctx, const AT* const at, const char* pathname,
    Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(ctx, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      status = Lstat1(ctx, *parent_dir->rep, tgt, stat);
      if (has_tailing_slashes) {
        if (!S_ISDIR(stat->FileMode())) {
          status = Status::DirExpected("Not a dir");
        }
      }
    } else {  // Special case: pathname is root
      *stat = rtstat_;
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

// After a call, the caller must release *parent_dir when it is set. *parent_dir
// may be set even when an non-OK status is returned.
Status FilesystemCli::Resolu(  ///
    FilesystemCliCtx* const ctx, const AT* const at, const char* pathname,
    Lease** parent_dir, Slice* last_component,  ///
    bool* has_tailing_slashes) {
#define PATH_PREFIX(pathname, remaining_path) \
  Slice(pathname, remaining_path - pathname)
  const char* rp(NULL);  // Remaining path on errors
  Status status;
  // Relative root
  Lease* rr;
  if (at != NULL) {
    status = Lokup(ctx, at->parent_of_root, at->name, kRegular, &rr);
    if (!status.ok()) {
      return status;
    }
  } else {
    rr = &rtlease_;
  }
  status = Resolv(ctx, rr, pathname, parent_dir, last_component, &rp);
  if (status.IsDirExpected() && rp) {
    return Status::DirExpected(PATH_PREFIX(pathname, rp));
  } else if (status.IsNotFound() && rp) {
    return Status::NotFound(PATH_PREFIX(pathname, rp));
  } else if (!status.ok()) {
    return status;
  }

  const char* const p = last_component->data();
  if (p[last_component->size()] == '/')  ///
    *has_tailing_slashes = true;

  return status;
}

// After a call, the caller must release *parent_dir. One *parent_dir is
// returned regardless of the return status.
Status FilesystemCli::Resolv(  ///
    FilesystemCliCtx* const ctx, Lease* const relative_root,
    const char* pathname, Lease** parent_dir, Slice* last_component,
    const char** remaining_path) {
  assert(pathname);
  const char* p = pathname;
  const char* q;
  assert(p[0] == '/');
  Lease* tmp;
  Status status;
  Lease* current_parent = relative_root;
  Slice current_name;
  while (true) {
    // Jump forward to the next path splitter.
    // E.g., "/", "/a/b", "/aa/bb/cc/dd".
    //        ||     | |         |  |
    //        pq     p q         p  q
    for (q = p + 1; q[0]; q++) {
      if (q[0] == '/') {
        break;
      }
    }
    if (!q[0]) {  // End of path
      break;
    }
    // This skips empty names in the beginning of a path.
    // E.g., "///", "//a", "/////a/b/c".
    //         ||    ||        ||
    //         pq    pq        pq
    if (q - p - 1 == 0) {
      p = q;  // I.e., p++
      continue;
    }
    // Look ahead and skip repeated slashes. E.g., "//a//b", "/a/bb////cc".
    //                                               | | |      |  |   |
    //                                               p q c      p  q   c
    // This also gets rid of potential tailing slashes.
    // E.g., "/a/b/", "/a/b/c/////".
    //          | ||       | |    |
    //          p qc       p q    c
    const char* c = q + 1;
    for (; c[0]; c++) {
      if (c[0] != '/') {
        break;
      }
    }
    if (!c[0]) {  // End of path
      break;
    }
    current_name = Slice(p + 1, q - p - 1);
    p = c - 1;
    status = Lokup(ctx, *current_parent->rep, current_name, kRegular, &tmp);
    if (status.ok()) {
      Release(current_parent);
      current_parent = tmp;
    } else {
      break;
    }
  }
  if (status.ok()) {
    *last_component = Slice(p + 1, q - p - 1);
  } else {
    *last_component = current_name;
    *remaining_path = p;
  }

  *parent_dir = current_parent;
  return status;
}

// After a successful call, the returned lease (including its parent directory
// partition) must be released after use. If the lease has internal batch
// contexts, these internal contexts must also be unreferenced after use. On
// errors, no lease will be returned.
Status FilesystemCli::Lokup(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent, const Slice& name,
    LokupMode mode, Lease** stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;  // Index of the partition holding the name being looked up
  Status s = AcquireAndFetch(ctx, parent, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      // Lokup1() uses per-partition locking. Unlock here...
      mutex_.Unlock();
      s = Lokup1(ctx, parent, name, mode, part, stat);
      mutex_.Lock();
      // Increase partition reference before returning the lease to the caller
      if (s.ok()) {
        assert(*stat != &rtlease_);
        Ref(part);
      }
      Release(part);
    }
    Release(dir);
  }
  return s;
}

// Prepare the client context for bulk insertion.
Status FilesystemCli::CreateBulkContext(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent,
    BulkInserts** result) {
  MutexLock lock(&mutex_);
  Dir* dir;
  Status s = AcquireAndFetch(ctx, parent, Slice(), &dir, NULL);
  if (s.ok()) {
    BulkInserts* in = new BulkInserts;
    *result = in;
    in->commit_status = 0;
    in->refs = 0;  // To be incremented by the caller
    in->bulks = new BulkIn[srvs_];
    for (int i = 0; i < srvs_; i++) {
      in->bulks[i].db = NULL;
      char bkid[50];
      snprintf(bkid, sizeof(bkid), "/b%d-%llu-%llu-%d", ctx->bkid,
               static_cast<unsigned long long>(dir->id->dno),
               static_cast<unsigned long long>(dir->id->ino), i);
      in->bulks[i].dbloc = ctx->bkrt + bkid;
      Stat* const stat = &in->bulks[i].stat;
      stat->SetDnodeNo(0);
      stat->SetInodeNo(0);
      stat->SetChangeTime(0);
      stat->SetModifyTime(0);
      stat->SetUserId(ctx->who.uid);
      stat->SetGroupId(ctx->who.gid);
      stat->SetFileSize(0);
      stat->SetFileMode(S_IFREG | (0660 & ACCESSPERMS));
      stat->SetZerothServer(-1);
    }
    in->dir = dir;
    in->ctx = ctx;
  }
  return s;
}

// Prepare the client context for batch operations.
Status FilesystemCli::CreateBatch(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent,
    BatchedCreates** result) {
  MutexLock lock(&mutex_);
  Dir* dir;
  Status s = AcquireAndFetch(ctx, parent, Slice(), &dir, NULL);
  if (s.ok()) {
    BatchedCreates* bc = new BatchedCreates;
    *result = bc;
    // In future, we could allow each dir to define its own amount
    // of virtual servers.
    bc->mode = 0660;
    bc->commit_status = 0;
    bc->refs = 0;  // To be incremented by the caller
    bc->wribufs = new WriBuf[srvs_];
    bc->dir = dir;
    bc->ctx = ctx;
  }
  return s;
}

// After a successful call, the caller must release *result after use. On
// errors, no directory handle is returned.
// REQUIRES: mutex_ has been locked.
Status FilesystemCli::AcquireAndFetch(  ///
    FilesystemCliCtx* const ctx, const LookupStat& parent, const Slice& name,
    Dir** result, int* i) {
  mutex_.AssertHeld();
  DirId at(parent);
  Status s = AcquireDir(at, result);
  if (s.ok()) {
    mutex_.Unlock();  // Fetch1() uses per-dir locking
    s = Fetch1(ctx, parent, name, *result, i);
    mutex_.Lock();
    if (!s.ok()) {
      Release(*result);
    }
  }
  return s;
}

namespace {
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
bool IsDirWriteOk(const FilesystemCliOptions& options, const LookupStat& parent,
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
bool IsLookupOk(const FilesystemCliOptions& options, const LookupStat& parent,
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
}  // namespace

Status FilesystemCli::Fetch1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    Dir* const dir, int* const rv) {
  // If there is an ongoing dir index status change, wait until that change is
  // done before operating upon the index.
  MutexLock lock(dir->mu);
  Status s = FetchDir(p.ZerothServer(), dir);
  if (s.ok() && !name.empty()) {
    *rv = dir->giga->SelectServer(name);
  }
  return s;
}

// Look up a named directory beneath a specified parent directory. On success, a
// lease for the stat of the directory being looked up is returned. The returned
// lease must be released after use. Only valid leases will be returned. Expired
// leases are renewed before they are returned. Leases with a different mode
// than requested are immediately released and are not returned. Each returned
// lease requires adding a reference to its parent directory partition. Caller
// of this function must be holding an active reference to this parent partition
// when making this call and then transfer the reference to the returned lease
// immediately after receiving it following the call. When looking up under a
// non-regular lookup mode, each returned lease will additionally carry a
// reference to an internal batch context embedded within the lease. Such a
// reference must also be released after use.
Status FilesystemCli::Lokup1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    LokupMode mode, Partition* const part, Lease** stat) {
  if (!IsLookupOk(options_, p, ctx->who))  // Parental perm checks
    return Status::AccessDenied("No x perm");
  Lease* lease;
  MutexLock lock(part->mu);
  // The following hash is used for per-partition synchronization, for lookups
  // in the per-partition lease LRU cache, and for lookups in the per-partition
  // lease table.
  const uint32_t hash = Hash(name.data(), name.size(), 0);
  Status s = Lokup2(ctx, p, name, hash, mode, part, &lease);
  if (s.ok()) {
    if (lease->mode != mode) {
      part->cached_leases->Release(lease->lru_handle);
      s = Status::AccessDenied("Lease mode mismatch",
                               "Dir locked for regular operations, batched "
                               "creates, or bulk insertion");
    } else {
      // Pending partition reference increment
      switch (mode) {
        case kBulkIn:
          assert(lease->bk != NULL);
          lease->bk->refs++;
          break;
        case kBatchedCreats:
          assert(lease->batch != NULL);
          lease->batch->refs++;
          break;
        default:
          break;
      }

      *stat = lease;
    }
  }

  return s;
}

Status FilesystemCli::Bukin1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const bool force_flush, const int i, BulkIn* const buk) {
  Status s;
  MutexLock lock(&buk->mu);
  if (!buk->db) {
    BukDb::DestroyDb(buk->dbloc, ctx->bkenv);
    buk->db = new BukDb(ctx->bkoptions, ctx->bkenv);
    s = buk->db->Open(buk->dbloc);
  }
  if (s.ok() && !name.empty()) {
    s = buk->db->Put(DirId(p), name, buk->stat, &buk->stats);
  }
  if (s.ok() && force_flush) {
    s = buk->db->Flush();
    if (s.ok()) {
      delete buk->db;
      buk->db = NULL;
      s = Bukin2(ctx, p, buk->dbloc, i);
    }
  }
  return s;
}

Status FilesystemCli::Mkfls1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t mode, const bool force_flush, const int i,
    WriBuf* const buf) {
  Status s;
  MutexLock lock(&buf->mu);  // Shall we use double buffering?
  if (force_flush || buf->n >= options_.batch_size) {
    s = Mkfls2(ctx, p, buf->namearr, buf->n, mode, i);
    if (s.ok()) {
      buf->namearr.resize(0);
      buf->n = 0;
    }
  }
  if (s.ok() && !name.empty()) {
    PutLengthPrefixedSlice(&buf->namearr, name);
    buf->n++;
  }
  return s;
}

Status FilesystemCli::Mkfle1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t mode, Stat* const stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(ctx, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Mkfle2() is serialized by server; unlock here...
      s = Mkfle2(ctx, p, name, mode, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::Mkdir1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t mode, Stat* const stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(ctx, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Mkdir2() is serialized by server; unlock here...
      s = Mkdir2(ctx, p, name, mode, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::Lstat1(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    Stat* const stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(ctx, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Lstat2() is serialized by server; unlock here...
      s = Lstat2(ctx, p, name, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

// Look for a lease. Dynamically instantiate a new lease when none can be found
// locally or the one we find has already expired. When dynamically
// instantiating a lease, the specified lookup mode will be checked and the
// lease will be initialized accordingly. Otherwise, the mode is not checked and
// the returned lease may have a different mode. A caller should check whether
// the returned lease has a desired mode. In either case, a caller should
// release the returned lease after use. If the returned lease has an internal
// batch context, a reference may need to be added to such an internal context.
// REQUIRES: part->mu has been locked.
Status FilesystemCli::Lokup2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t hash, LokupMode mode, Partition* const part,
    Lease** const stat) {
  part->mu->AssertHeld();
  Lease* lease;
  Status s;
  LRUCache<LeaseHandl>* const lru = part->cached_leases;
  HashTable<Lease>* const ht = part->leases;
  // Quickly check if we have already had the lease...
  LeaseHandl* h = lru->Lookup(name, hash);
  if (h != NULL) {  // It's a hit!
    lease = h->value;
    if (lease->rep->LeaseDue() < CurrentMicros()) {
      ht->Remove(lease);  // Lease expired; remove it from the partition
      lru->Erase(h);
      lease->out = true;
      lru->Release(h);
    } else {
      // Directly return the cached lease
      assert(lease->lru_handle == h);
      *stat = lease;
      return s;
    }
  } else {  // Try the bigger lease table...
    lease = *ht->FindPointer(name, hash);
    if (lease == NULL) {
      // Do nothing
    } else if (lease->rep->LeaseDue() < CurrentMicros()) {
      ht->Remove(lease);
      lease->out = true;
    } else {
      assert(lease->lru_handle->value == lease);
      lru->Ref(lease->lru_handle);
      *stat = lease;
      return s;
    }
  }

  *stat = NULL;

  // Wait for concurrent lookups. After they finish, check again in case some
  // other thread has done the work for us while we are waiting...
  uint32_t const i = hash & uint32_t(kWays - 1);
  while (part->busy[i]) part->cv->Wait();
  part->busy[i] = true;
  h = lru->Lookup(name, hash);
  if (h != NULL) {
    lease = h->value;
    if (lease->rep->LeaseDue() < CurrentMicros()) {
      ht->Remove(lease);  // Lease expired; remove it from the partition
      lru->Erase(h);
      lease->out = true;
      lru->Release(h);
    } else {
      assert(lease->lru_handle == h);
      *stat = lease;
    }
  } else {  // Try the bigger lease table...
    lease = *ht->FindPointer(name, hash);
    if (lease == NULL) {
      // Do nothing
    } else if (lease->rep->LeaseDue() < CurrentMicros()) {
      ht->Remove(lease);
      lease->out = true;
    } else {
      assert(lease->lru_handle->value == lease);
      lru->Ref(lease->lru_handle);
      *stat = lease;
    }
  }

  if (*stat == NULL) {
    // Temporarily unlock for potentially costly lookups...
    part->mu->Unlock();
    LookupStat* tmp = new LookupStat;
    if (fs_ != NULL) {
      s = fs_->Lokup(ctx->who, p, name, tmp);
    } else if (rpc_ != NULL) {
      LokupOptions opts;
      opts.parent = &p;
      opts.name = name;
      opts.me = ctx->who;
      LokupRet ret;
      ret.stat = tmp;
      rpc::If* const stub = PrepareStub(ctx, part->index);
      s = rpc::LokupCli(stub)(opts, &ret);
    } else {
      s = Nofs();
    }

    BatchedCreates* tmpbat = NULL;
    BulkInserts* tmpin = NULL;
    if (s.ok()) {
      switch (mode) {
        case kBulkIn:
          s = CreateBulkContext(ctx, *tmp, &tmpin);
          break;
        case kBatchedCreats:
          s = CreateBatch(ctx, *tmp, &tmpbat);
          break;
        default:
          break;
      }
    }

    // Lock again for finishing up...
    part->mu->Lock();
    if (s.ok()) {
      lease = static_cast<Lease*>(malloc(sizeof(Lease) - 1 + name.size()));
      lease->key_length = name.size();
      memcpy(lease->key_data, name.data(), name.size());
      lease->mode = mode;
      lease->hash = hash;
      lease->part = part;
      lease->out = false;
      lease->batch = tmpbat;
      lease->bk = tmpin;
      lease->rep = tmp;
      Lease* old = ht->Insert(lease);
#ifndef NDEBUG
      // Because we synchronize and check before each insert, there shall be no
      // conflict.
      assert(old == NULL);
#else
      (void)old;
#endif
      h = lru->Insert(name, hash, lease, 1, DeleteLease);
      lease->lru_handle = h;
      if (lease->rep->LeaseDue() == 0) {
        // Lease cannot be cached; we remove it from the lease table forcing
        // a new lease to be instantiated the next time the directory is
        // accessed.
        ht->Remove(lease);
        lease->out = true;
        lru->Erase(h);
      }

      *stat = lease;
    } else {
      delete tmp;
    }
  }

  part->busy[i] = false;
  part->cv->SignalAll();
  return s;
}

Status FilesystemCli::Bukin2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const std::string& bkdir,
    const int i) {
  if (!IsDirWriteOk(options_, p, ctx->who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Bukin(ctx->who, p, bkdir);
  } else if (rpc_ != NULL) {
    BukinOptions opts;
    opts.parent = &p;
    opts.dir = bkdir;
    opts.me = ctx->who;
    BukinRet ret;
    rpc::If* const stub = PrepareStub(ctx, i);
    s = rpc::BukinCli(stub)(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Mkfls2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& namearr,
    uint32_t n, const uint32_t mode, const int i) {
  if (!IsDirWriteOk(options_, p, ctx->who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkfls(ctx->who, p, namearr, mode, &n);
  } else if (rpc_ != NULL) {
    MkflsOptions opts;
    opts.parent = &p;
    opts.namearr = namearr;
    opts.mode = mode;
    opts.n = n;
    opts.me = ctx->who;
    MkflsRet ret;
    rpc::If* const stub = PrepareStub(ctx, i);
    s = rpc::MkflsCli(stub)(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Mkfle2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t mode, const int i, Stat* const stat) {
  if (!IsDirWriteOk(options_, p, ctx->who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkfle(ctx->who, p, name, mode, stat);
  } else if (rpc_ != NULL) {
    MkfleOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.mode = mode;
    opts.me = ctx->who;
    MkfleRet ret;
    ret.stat = stat;
    rpc::If* const stub = PrepareStub(ctx, i);
    s = rpc::MkfleCli(stub)(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Mkdir2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const uint32_t mode, const int i, Stat* const stat) {
  if (!IsDirWriteOk(options_, p, ctx->who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkdir(ctx->who, p, name, mode, stat);
  } else if (rpc_ != NULL) {
    MkdirOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.mode = mode;
    opts.me = ctx->who;
    MkdirRet ret;
    ret.stat = stat;
    rpc::If* const stub = PrepareStub(ctx, i);
    s = rpc::MkdirCli(stub)(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Lstat2(  ///
    FilesystemCliCtx* const ctx, const LookupStat& p, const Slice& name,
    const int i, Stat* const stat) {
  if (!IsLookupOk(options_, p, ctx->who))  // Avoid unnecessary server rpc
    return Status::AccessDenied("No x perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Lstat(ctx->who, p, name, stat);
  } else if (rpc_ != NULL) {
    LstatOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.me = ctx->who;
    LstatRet ret;
    ret.stat = stat;
    rpc::If* const stub = PrepareStub(ctx, i);
    s = rpc::LstatCli(stub)(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

rpc::If* FilesystemCli::PrepareStub(  ///
    FilesystemCliCtx* const ctx, const int srv_idx) {
  assert(srv_idx < srvs_);
  if (!ctx->stubs_) {
    ctx->stubs_ = new rpc::If*[srvs_ * ports_per_srv_];
    memset(ctx->stubs_, 0, sizeof(rpc::If*) * srvs_ * ports_per_srv_);
    ctx->n_ = srvs_ * ports_per_srv_;
  }
  int port_idx = 0;
  if (ports_per_srv_ > 1) {
    port_idx = int(ctx->rnd_.Next()) % ports_per_srv_;
  }
  int i = srv_idx * ports_per_srv_ + port_idx;
  if (!ctx->stubs_[i]) {
    ctx->stubs_[i] = rpc_->OpenStubFor(uri_mapper_->GetUri(srv_idx, port_idx));
  }
  return ctx->stubs_[i];
}

// This function is called when the last reference to a lease of a parent
// directory is released. It deletes the lease by freeing its memory and
// removing its record from its parent lease table. Future lookups to the
// directory will result in new fs or rpc lookups and new leases.
void FilesystemCli::DeleteLease(const Slice& key, Lease* lease) {
  assert(lease->key() == key);
  // Any batch context should have been dissociated by now
  assert(lease->bk == NULL && lease->batch == NULL);
  Partition* const part = lease->part;
  part->mu->AssertHeld();
  if (!lease->out)  // Skip if lease has already been removed from the table
    part->leases->Remove(lease);
  delete lease->rep;
  free(lease);
}

// Remove a reference to a lease. Also remove a reference to the lease's parent
// partition. The lease will be deleted when the last reference to it is
// removed.
void FilesystemCli::Release(Lease* lease) {
  if (lease == &rtlease_) return;  // Root lease is static...
  Partition* const part = lease->part;
  part->mu->Lock();
  part->cached_leases->Release(lease->lru_handle);
  part->mu->Unlock();
  MutexLock lock(&mutex_);
  Release(part);
}

uint32_t FilesystemCli::TEST_TotalLeasesAtPartition(const DirId& at, int ix) {
  uint32_t rv(0);
  Partition* part;
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    s = AcquirePartition(dir, ix, &part);
    if (s.ok()) {
      // part->mu->Lock() unnecessary assuming reading one 32-bit
      // integer is an atomic operation
      rv = part->leases->Size();
      Release(part);
    }
    Release(dir);
  }
  return rv;
}

Status FilesystemCli::TEST_ProbePartition(const DirId& at, int ix) {
  Partition* part;
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    s = AcquirePartition(dir, ix, &part);
    if (s.ok()) {
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::TEST_ProbeDir(const DirId& at) {
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    Release(dir);
  }
  return s;
}

uint32_t FilesystemCli::TEST_TotalPartitionsInMemory() {
  MutexLock lock(&mutex_);
  return pars_->Size();
}

uint32_t FilesystemCli::TEST_TotalDirsInMemory() {
  MutexLock lock(&mutex_);
  return dirs_->Size();
}

namespace {
Slice LRUKey(const DirId& id, int partition_no, char* const scratch) {
  char* p = scratch;
  EncodeFixed64(p, id.dno);
  p += 8;
  EncodeFixed64(p, id.ino);
  p += 8;
  EncodeFixed32(p, partition_no);
  p += 4;
  return Slice(scratch, p - scratch);
}

Slice DirKey(const DirId& id, char* const scratch) {
  char* p = scratch;
  EncodeFixed64(p, id.dno);
  p += 8;
  EncodeFixed64(p, id.ino);
  p += 8;
  return Slice(scratch, p - scratch);
}

uint32_t HashKey(const Slice& k) { return Hash(k.data(), k.size(), 0); }

template <typename E>
void LIST_Remove(E* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

template <typename E>
void LIST_Append(E* e, E* list) {
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

}  // namespace

void FilesystemCli::Release(Dir* dir) {
  mutex_.AssertHeld();
  assert(dir->refs != 0);
  dir->refs--;
  if (!dir->refs) {
    dirs_->Remove(dir->key(), dir->hash);
    LIST_Remove(dir);
    delete dir->id;
    delete dir->giga_opts;
    delete dir->giga;
    delete dir->mu;
    free(dir);
  }
}

// REQUIRES: dir->mu has been locked.
Status FilesystemCli::FetchDir(uint32_t zeroth_server, Dir* dir) {
  Status s;
  dir->mu->AssertHeld();
  if (dir->fetched) {
    return s;
  }

  dir->giga_opts = new DirIndexOptions;
  dir->giga_opts->num_virtual_servers = srvs_;
  dir->giga_opts->num_servers = srvs_;

  const uint32_t zsrv = Filesystem::PickupServer(*dir->id);
  dir->giga = new DirIndex(zsrv, dir->giga_opts);
  dir->giga->SetAll();

  dir->fetched = true;
  return s;
}

// REQUIRES: mutex_ has been locked.
Status FilesystemCli::AcquireDir(const DirId& id, Dir** result) {
  mutex_.AssertHeld();
  char tmp[30];
  Slice key = DirKey(id, tmp);
  const uint32_t hash = HashKey(key);
  Status s;

  // Check if we have already cached it
  Dir** const pos = dirs_->FindPointer(key, hash);
  Dir* dir = *pos;
  if (dir != NULL) {
    *result = dir;
    assert(*dir->id == id);
    dir->refs++;
    return s;
  }

  // If we cannot find the entry, we create it...
  dir = static_cast<Dir*>(malloc(sizeof(Dir) - 1 + key.size()));
  dir->id = new DirId(id);
  dir->key_length = key.size();
  memcpy(dir->key_data, key.data(), key.size());
  dir->hash = hash;
  dir->fscli = this;
  dir->mu = new port::Mutex;
  dir->giga = NULL;  // To be fetched later
  dir->giga_opts = NULL;
  dir->fetched = 0;

  *result = dir;
  LIST_Append(dir, &dirlist_);
  dirs_->Inject(dir, pos);
  dir->refs = 1;
  return s;
}

// Delete a directory partition control block.
void FilesystemCli::DeletePartition(const Slice& key, Partition* part) {
  assert(part->key() == key);
  FilesystemCli* const cli = part->dir->fscli;
  cli->mutex_.AssertHeld();
  cli->pars_->Remove(key, part->hash);
  cli->mutex_.Unlock();
  part->mu->Lock();
  delete part->cached_leases;
  assert(part->leases->Empty());
  delete part->leases;
  part->mu->Unlock();
  delete part->cv;
  delete part->mu;
  cli->mutex_.Lock();
  cli->Release(part->dir);
  free(part);
}

// Remove a reference to a specified directory partition control block. Delete
// it when the last reference is removed.
void FilesystemCli::Release(Partition* const part) {
  mutex_.AssertHeld();
  plru_->Release(part->lru_handle);
}

// Add a reference to a directory partition.
void FilesystemCli::Ref(Partition* part) {
  mutex_.AssertHeld();
  plru_->Ref(part->lru_handle);
}

// Obtain the control block for a specific directory partition.
Status FilesystemCli::AcquirePartition(Dir* dir, int ix, Partition** result) {
  mutex_.AssertHeld();
  char tmp[30];
  Slice key = LRUKey(*dir->id, ix, tmp);
  const uint32_t hash = HashKey(key);
  Status s;

  Partition* part;
  // Try the LRU cache first...
  PartHandl* h = plru_->Lookup(key, hash);
  if (h != NULL) {
    part = h->value;
    assert(part->lru_handle == h);
    *result = part;
    return s;
  }

  // If we cannot find an entry from the cache, we continue our search at the
  // bigger hash table. We cache the cursor position returned by the table so
  // that we can reuse it in a later table insertion.
  Partition** const pos = pars_->FindPointer(key, hash);
  part = *pos;
  if (part != NULL) {
    *result = part;
    assert(part->lru_handle->value == part);
    // Should we reinsert it into the cache?
    plru_->Ref(part->lru_handle);
    return s;
  }

  // If we still cannot find it, we create it...
  part = static_cast<Partition*>(malloc(sizeof(Partition) - 1 + key.size()));
  part->key_length = key.size();
  memcpy(part->key_data, key.data(), key.size());
  part->hash = hash;
  part->index = ix;
  part->cached_leases =
      new LRUCache<LeaseHandl>(options_.per_partition_lease_lru_size);
  part->leases = new HashTable<Lease>;
  part->mu = new port::Mutex;
  part->cv = new port::CondVar(part->mu);
  memset(&part->busy[0], 0, kWays);
  pars_->Inject(part, pos);
  part->dir = dir;
  dir->refs++;

  h = plru_->Insert(key, hash, part, 1, DeletePartition);
  part->lru_handle = h;
  *result = part;
  return s;
}

void FilesystemCli::FormatRoot() {
  rtstat_.SetDnodeNo(0);
  rtstat_.SetInodeNo(0);
  rtstat_.SetZerothServer(0);
  rtstat_.SetFileMode(0777);
  rtstat_.SetUserId(0);
  rtstat_.SetGroupId(0);
  rtstat_.SetFileSize(0);
  rtstat_.SetChangeTime(0);
  rtstat_.SetModifyTime(0);
  rtstat_.AssertAllSet();
}

FilesystemCli::FilesystemCli(const FilesystemCliOptions& options)
    : dirs_(NULL),
      plru_(NULL),
      pars_(NULL),
      options_(options),
      fs_(NULL),
      uri_mapper_(NULL),
      ports_per_srv_(1),
      srvs_(1),
      rpc_(NULL) {
  dirs_ = new HashTable<Dir>;
  dirlist_.next = &dirlist_;
  dirlist_.prev = &dirlist_;
  plru_ = new LRUCache<PartHandl>(options_.partition_lru_size);
  pars_ = new HashTable<Partition>;

  FormatRoot();

  rtlokupstat_.CopyFrom(rtstat_);
  rtlokupstat_.SetLeaseDue(-1);
  rtlease_.rep = &rtlokupstat_;
  rtlease_.batch = NULL;
}

FilesystemCliOptions::FilesystemCliOptions()
    : per_partition_lease_lru_size(4096),
      partition_lru_size(4096),
      batch_size(16),
      skip_perm_checks(false) {}

void FilesystemCli::RegisterFsSrvUris(  ///
    RPC* rpc, const UriMapper* uri_mapper, int srvs, int ports_per_srv) {
  rpc_ = rpc;
  uri_mapper_ = uri_mapper;
  ports_per_srv_ = ports_per_srv;
  srvs_ = srvs;
}

void FilesystemCli::SetLocalFs(Filesystem* fs) {
  fs_ = fs;  // This is a weak reference; fs_ is not owned by us
}

FilesystemCli::~FilesystemCli() {
  mutex_.Lock();
  delete plru_;
  assert(pars_->Empty());
  delete pars_;
  assert(dirlist_.next == &dirlist_);
  assert(dirlist_.prev == &dirlist_);
  assert(dirs_->Empty());
  delete dirs_;
  mutex_.Unlock();
}

}  // namespace pdlfs
