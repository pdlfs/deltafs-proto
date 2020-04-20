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

#include "pdlfs-common/gigaplus.h"
#include "pdlfs-common/mutexlock.h"

#include <sys/stat.h>

namespace pdlfs {
namespace {
Status Nofs() {  ///
  return Status::Disconnected("No fs manager");
}
}  // namespace
// Relative root of a pathname
struct FilesystemCli::AT {
  // Look up stat of the parent directory of the relative root
  LookupStat parent_of_root;
  // Name of the relative root under its parent
  std::string name;
};

Status FilesystemCli::Atdir(  ///
    const User& who, const AT* const at, const char* const pathname,
    AT** result) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      AT* rv = new AT;
      rv->parent_of_root = *parent_dir->value;
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

// Each batch instance is a reference to a server-issued lease with bulk
// insertion capabilities.
struct FilesystemCli::BATCH {
  Lease* dir_lease;
};

// On success, the returned batch handle contains a reference to the target
// dir's lease from server and a reference to the internal batch object
// associated with the lease.
Status FilesystemCli::BatchStart(  ///
    const User& who, const AT* const at, const char* const pathname,
    BATCH** result) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      Lease* dir_lease;
      // This should ideally be a special mkdir creating a new dir and
      // simultaneously locking the newly created dir. Any subsequent regular
      // lookup operation either finds a non-regular lease with a batch context
      // or fails to initialize a regular lease from server.
      status = Lokup(who, *parent_dir->value, tgt, kBatchedCreats, &dir_lease);
      if (status.ok()) {
        assert(dir_lease->batch != NULL);
        BATCH* bat = new BATCH;  // Opaque handle to the batch
        bat->dir_lease = dir_lease;
        *result = bat;
      }
    } else {  // Special case for root
      status = Status::NotSupported(Slice());
    }
  }
  if (parent_dir) {
    Release(parent_dir);
  }
  return status;
}

Status FilesystemCli::BatchEnd(BATCH* bat) {
  Lease* lease = bat->dir_lease;
  Partition* const part = lease->part;
  BatchedCreates* const bc = lease->batch;
  part->mu->Lock();
  assert(bc->refs != 0);
  bc->refs--;
  uint32_t r = bc->refs;
  if (!r) {
    // Non-regular leases are marked by their batch contexts.
    // Once the context is deleted, the lease itself is invalidated and must be
    // removed from the cache.
    part->cached_leases->Erase(lease->lru_handle);
    part->leases->Remove(lease);
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

Status FilesystemCli::Mkfle(  ///
    const User& who, const AT* const at, const char* const pathname,
    uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty() && !has_tailing_slashes) {
      if (parent_dir->batch != NULL) {
        status = Status::AccessDenied("Dir locked for batch file creates");
      } else {
        status = Mkfle1(who, *parent_dir->value, tgt, mode, stat);
      }
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
    const User& who, const AT* const at, const char* const pathname,
    uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      if (parent_dir->batch != NULL) {
        status = Status::AccessDenied("Dir locked for batch file creates");
      } else {
        status = Mkdir1(who, *parent_dir->value, tgt, mode, stat);
      }
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
    const User& who, const AT* const at, const char* const pathname,
    Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir(NULL);
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (status.ok()) {
    if (!tgt.empty()) {
      if (parent_dir->batch != NULL) {
        status = Status::AccessDenied("Dir locked for batch file creates");
      } else {
        status = Lstat1(who, *parent_dir->value, tgt, stat);
        if (has_tailing_slashes) {
          if (!S_ISDIR(stat->FileMode())) {
            status = Status::DirExpected("Not a dir");
          }
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
    const User& who, const AT* const at, const char* const pathname,
    Lease** parent_dir, Slice* last_component,  ///
    bool* has_tailing_slashes) {
#define PATH_PREFIX(pathname, remaining_path) \
  Slice(pathname, remaining_path - pathname)
  const char* rp(NULL);  // Remaining path on errors
  Status status;
  // Relative root
  Lease* rr;
  if (at != NULL) {
    status = Lokup(who, at->parent_of_root, at->name, kRegular, &rr);
    if (!status.ok()) {
      return status;
    }
  } else {
    rr = &rtlease_;
  }
  status = Resolv(who, rr, pathname, parent_dir, last_component, &rp);
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
    const User& who, Lease* const relative_root, const char* const pathname,
    Lease** parent_dir, Slice* last_component,  ///
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
    status = Lokup(who, *current_parent->value, current_name, kRegular, &tmp);
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

// After a successful call, the caller must release *stat after use. On errors,
// no lease is returned.
Status FilesystemCli::Lokup(  ///
    const User& who, const LookupStat& parent, const Slice& name,
    LokupMode mode, Lease** stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;  // Index of the partition holding the name being looked up
  Status s = AcquireAndFetch(who, parent, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      // Lokup1() uses per-partition locking. Unlock here...
      mutex_.Unlock();
      s = Lokup1(who, parent, name, mode, part, stat);
      mutex_.Lock();
      if (s.ok()) {  // Increase partition ref before returning the lease
        assert(*stat != &rtlease_);
        Ref(part);
      }
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::CreateBatch(  ///
    const User& who, const LookupStat& parent, BatchedCreates** result) {
  MutexLock lock(&mutex_);
  Dir* dir;
  Status s = AcquireAndFetch(who, parent, Slice(), &dir, NULL);
  if (s.ok()) {
    BatchedCreates* bc = new BatchedCreates;
    *result = bc;
    // In future, we could allow each dir to define its own amount
    // of virtual servers.
    int n = options_.nsrvs;
    bc->refs = 0;  // To be increased by the caller
    bc->wribufs = new WriBuf[n];
    bc->dir = dir;
  }
  return s;
}

// After a successful call, the caller must release *result after use. On
// errors, no directory handle is returned.
// REQUIRES: mutex_ has been locked.
Status FilesystemCli::AcquireAndFetch(  ///
    const User& who, const LookupStat& parent, const Slice& name, Dir** result,
    int* i) {
  mutex_.AssertHeld();
  DirId at(parent);
  Status s = AcquireDir(at, result);
  if (s.ok()) {
    mutex_.Unlock();  // Fetch1() uses per-dir locking
    s = Fetch1(who, parent, name, *result, i);
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
};  // namespace

Status FilesystemCli::Fetch1(  ///
    const User& who, const LookupStat& p, const Slice& name, Dir* dir,
    int* rv) {
  // If there is an ongoing dir index status change, wait until that change is
  // done before using the index.
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
// leases are renewed before they are returned. Each returned lease holds a
// reference to its parent directory partition. Caller of this function must
// increase the partition's reference count after receiving a lease.
Status FilesystemCli::Lokup1(  ///
    const User& who, const LookupStat& p, const Slice& name, LokupMode mode,
    Partition* part, Lease** stat) {
  if (!IsLookupOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No x perm");
  Lease* lease;
  MutexLock lock(part->mu);
  // The following hash is used for per-partition synchronization, for lookups
  // in the per-partition lease LRU cache, and for lookups in the per-partition
  // lease table.
  const uint32_t hash = Hash(name.data(), name.size(), 0);
  Status s = Lokup2(who, p, name, hash, mode, part, &lease);
  if (s.ok()) {
    *stat = lease;
    assert(lease->part == part);  // Pending partition reference increment
    if (mode == kBatchedCreats) {
      assert(lease->batch != NULL);
      lease->batch->refs++;
    }
  }

  return s;
}

Status FilesystemCli::Mkfle1(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    Stat* stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(who, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Mkfle2() uses server-side locking. Unlock here...
      s = Mkfle2(who, p, name, mode, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::Mkdir1(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    Stat* stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(who, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Mkdir2() uses server-side locking. Unlock here...
      s = Mkdir2(who, p, name, mode, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

Status FilesystemCli::Lstat1(  ///
    const User& who, const LookupStat& p, const Slice& name, Stat* stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;
  Status s = AcquireAndFetch(who, p, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Lstat2() uses server-side locking. Unlock here...
      s = Lstat2(who, p, name, i, stat);
      mutex_.Lock();
      Release(part);
    }
    Release(dir);
  }
  return s;
}

// part->mu has been locked.
Status FilesystemCli::Lokup2(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t hash,
    LokupMode mode, Partition* part, Lease** stat) {
  part->mu->AssertHeld();
  Lease* lease;
  Status s;
  LRUCache<LeaseHandl>* const lru = part->cached_leases;
  HashTable<Lease>* const ht = part->leases;
  // Quickly check if we have it already...
  LeaseHandl* h = lru->Lookup(name, hash);
  if (h != NULL) {  // It's a hit!
    lease = h->value;
    if (lease->value->LeaseDue() < CurrentMicros()) {
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
    } else if (lease->value->LeaseDue() < CurrentMicros()) {
      ht->Remove(lease);
      lease->out = true;
    } else {
      assert(lease->lru_handle->value == lease);
      lru->Ref(lease->lru_handle);
      *stat = lease;
    }
  }

  *stat = NULL;

  // Wait for concurrent conflicting name lookups or changes and check again in
  // case some other thread has done the work for us while we are waiting...
  uint32_t const i = hash & uint32_t(kWays - 1);
  while (part->busy[i]) part->cv->Wait();
  part->busy[i] = true;
  h = lru->Lookup(name, hash);
  if (h != NULL) {
    lease = h->value;
    if (lease->value->LeaseDue() < CurrentMicros()) {
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
    } else if (lease->value->LeaseDue() < CurrentMicros()) {
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
      s = fs_->Lokup(who, p, name, tmp);
    } else if (stub_ != NULL) {
      assert(part->index < options_.nsrvs);
      LokupOptions opts;
      opts.parent = &p;
      opts.name = name;
      opts.me = who;
      LokupRet ret;
      ret.stat = tmp;
      s = rpc::LokupCli(stub_[part->index])(opts, &ret);
    } else {
      s = Nofs();
    }

    BatchedCreates* tmpbat = NULL;
    if (s.ok()) {
      if (mode == kBatchedCreats) {
        s = CreateBatch(who, *tmp, &tmpbat);
      }
    }

    // Lock again for finishing up...
    part->mu->Lock();
    if (s.ok()) {
      lease = static_cast<Lease*>(malloc(sizeof(Lease) - 1 + name.size()));
      lease->key_length = name.size();
      memcpy(lease->key_data, name.data(), name.size());
      lease->hash = hash;
      lease->part = part;
      lease->out = false;
      lease->batch = tmpbat;
      lease->value = tmp;
      Lease* old = ht->Insert(lease);
      assert(old == NULL);
      (void)old;

      h = lru->Insert(name, hash, lease, 1, DeleteLease);
      lease->lru_handle = h;
      if (lease->value->LeaseDue() == 0) {
        // Lease cannot be cached; remove it from the partition
        ht->Remove(lease);
        lru->Erase(h);
        lease->out = true;
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

Status FilesystemCli::Mkfle2(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    int i, Stat* stat) {
  if (!IsDirWriteOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkfle(who, p, name, mode, stat);
  } else if (stub_ != NULL) {
    assert(i < options_.nsrvs);
    MkfleOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.mode = mode;
    opts.me = who;
    MkfleRet ret;
    ret.stat = stat;
    s = rpc::MkfleCli(stub_[i])(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Mkdir2(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    int i, Stat* stat) {
  if (!IsDirWriteOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkdir(who, p, name, mode, stat);
  } else if (stub_ != NULL) {
    assert(i < options_.nsrvs);
    MkdirOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.mode = mode;
    opts.me = who;
    MkdirRet ret;
    ret.stat = stat;
    s = rpc::MkdirCli(stub_[i])(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

Status FilesystemCli::Lstat2(  ///
    const User& who, const LookupStat& p, const Slice& name, int i,
    Stat* stat) {
  if (!IsLookupOk(options_, p, who))  // Avoid unnecessary server rpc
    return Status::AccessDenied("No x perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Lstat(who, p, name, stat);
  } else if (stub_ != NULL) {
    assert(i < options_.nsrvs);
    LstatOptions opts;
    opts.parent = &p;
    opts.name = name;
    opts.me = who;
    LstatRet ret;
    ret.stat = stat;
    s = rpc::LstatCli(stub_[i])(opts, &ret);
  } else {
    s = Nofs();
  }

  return s;
}

// Delete a lease from memory.
void FilesystemCli::DeleteLease(const Slice& key, Lease* lease) {
  assert(lease->key() == key);
  Partition* const part = lease->part;
  part->mu->AssertHeld();
  if (!lease->out)  // Skip if we have already done so
    part->leases->Remove(lease);
  // Any batch context should already be closed by now
  assert(lease->batch == NULL);
  delete lease->value;
  free(lease);
}

// Remove an active reference to a lease potentially causing it to be deleted
// from memory. Also remove a reference to the lease's parent partition
// potentially causing the partition to be deleted from memory too.
void FilesystemCli::Release(Lease* lease) {
  if (lease == &rtlease_) return;  // Root lease is static...
  Partition* const part = lease->part;
  part->mu->Lock();
  part->cached_leases->Release(lease->lru_handle);
  part->mu->Unlock();
  MutexLock lock(&mutex_);
  Release(part);
}

Status FilesystemCli::TEST_ProbePartition(const DirId& at, int ix) {
  Partition* part;
  Dir* dir;
  MutexLock lock(&mutex_);
  Status s = AcquireDir(at, &dir);
  if (s.ok()) {
    assert(dir->id == at);
    s = AcquirePartition(dir, ix, &part);
    if (s.ok()) {
      assert(part->index == ix);
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
    assert(dir->id == at);
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
  dir->giga_opts->num_virtual_servers = options_.vsrvs;
  dir->giga_opts->num_servers = options_.nsrvs;

  const uint32_t zsrv = Filesystem::PickupServer(dir->id);
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
    assert(dir->id == id);
    dir->refs++;
    return s;
  }

  // If we cannot find the entry, we create it...
  dir = static_cast<Dir*>(malloc(sizeof(Dir) - 1 + key.size()));
  dir->id = id;
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

// Delete a partition from memory.
void FilesystemCli::DeletePartition(const Slice& key, Partition* part) {
  assert(part->key() == key);
  FilesystemCli* const cli = part->dir->fscli;
  cli->mutex_.AssertHeld();
  cli->pars_->Remove(key, part->hash);
  delete part->cached_leases;
  assert(part->leases->Empty());
  delete part->leases;
  delete part->cv;
  delete part->mu;
  cli->Release(part->dir);
  free(part);
}

// Remove an active reference to a directory partition.
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
  Slice key = LRUKey(dir->id, ix, tmp);
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
      stub_(NULL),
      fs_(NULL),
      rpc_(NULL) {
  dirs_ = new HashTable<Dir>;
  dirlist_.next = &dirlist_;
  dirlist_.prev = &dirlist_;
  plru_ = new LRUCache<PartHandl>(options_.partition_lru_size);
  pars_ = new HashTable<Partition>;

  FormatRoot();

  rtlokupstat_.CopyFrom(rtstat_);
  rtlokupstat_.SetLeaseDue(-1);
  rtlease_.value = &rtlokupstat_;
  rtlease_.batch = NULL;
}

FilesystemCliOptions::FilesystemCliOptions()
    : per_partition_lease_lru_size(4096),
      partition_lru_size(4096),
      batch_size(16),
      skip_perm_checks(false),
      vsrvs(1),
      nsrvs(1) {}

Status FilesystemCli::OpenFilesystemCli(  ///
    const FilesystemOptions& options, const std::string& fsloc) {
  Filesystem* fs = new Filesystem(options);
  Status s = fs->OpenFilesystem(fsloc);
  if (s.ok()) {
    fs_ = fs;
  } else {
    delete fs;
  }
  return s;
}

Status FilesystemCli::Open(RPC* rpc, const std::string* uri) {
  stub_ = new rpc::If*[options_.nsrvs];
  for (int i = 0; i < options_.nsrvs; i++) {
    stub_[i] = rpc->OpenStubFor(uri[i]);
  }
  rpc_ = rpc;
  return Status::OK();
}

FilesystemCli::~FilesystemCli() {
  delete plru_;
  assert(pars_->Empty());
  delete pars_;
  assert(dirlist_.next == &dirlist_);
  assert(dirlist_.prev == &dirlist_);
  assert(dirs_->Empty());
  delete dirs_;
  if (stub_) {
    for (int i = 0; i < options_.nsrvs; i++) {
      delete stub_[i];
    }
  }
  delete[] stub_;
  delete rpc_;
  delete fs_;
}

}  // namespace pdlfs
