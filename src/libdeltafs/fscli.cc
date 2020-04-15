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

// Relative root of a pathname
struct FilesystemCli::AT {
  // Look up stat of the parent directory of the relative root
  LookupStat parent_of_root;
  // Name of the relative root under its parent
  std::string name;
};

Status FilesystemCli::Mkfle(  ///
    const User& who, const AT* const at, const char* const pathname,
    uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir;
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (!status.ok()) {
    return status;
  }

  if (has_tailing_slashes) {
    status = Status::FileExpected("Path refers to a dir");
  } else if (!tgt.empty()) {
    status = Mkfle1(who, *parent_dir->value, tgt, mode, stat);
  }

  Release(parent_dir);
  return status;
}

Status FilesystemCli::Mkdir(  ///
    const User& who, const AT* const at, const char* const pathname,
    uint32_t mode, Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir;
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (!status.ok()) {
    return status;
  }

  if (!tgt.empty()) {
    status = Mkdir1(who, *parent_dir->value, tgt, mode, stat);
  } else {  // Special case; pathname is root
    status = Status::AlreadyExists(Slice());
  }

  Release(parent_dir);
  return status;
}

Status FilesystemCli::Lstat(  ///
    const User& who, const AT* const at, const char* const pathname,
    Stat* const stat) {
  bool has_tailing_slashes(false);
  Lease* parent_dir;
  Slice tgt;
  Status status =
      Resolu(who, at, pathname, &parent_dir, &tgt, &has_tailing_slashes);
  if (!status.ok()) {
    return status;
  }

  if (!tgt.empty()) {
    status = Lstat1(who, *parent_dir->value, tgt, stat);
  } else {  // Special case; pathname is root
    *stat = rtstat_;
  }

  Release(parent_dir);
  return status;
}

Status FilesystemCli::Resolu(  ///
    const User& who, const AT* at, const char* const pathname,
    Lease** parent_dir, Slice* last_component,  ///
    bool* has_tailing_slashes) {
#define PATH_PREFIX(pathname, remaining_path) \
  Slice(pathname, remaining_path - pathname)
  const char* rp(NULL);  // Remaining path on errors
  // Relative root
  Lease* rr;
  if (at != NULL) {
    return Status::NotSupported(Slice());
  } else {
    rr = &rtlease_;
  }
  Status status = Resolv(who, rr, pathname, parent_dir, last_component, &rp);
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
    status = Lokup(who, *current_parent->value, current_name, &tmp);
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

Status FilesystemCli::Lokup(  ///
    const User& who, const LookupStat& parent, const Slice& name,
    Lease** stat) {
  MutexLock lock(&mutex_);
  Dir* dir;
  int i;  // Index for the partition holding the name
  Status s = AcquireAndFetch(who, parent, name, &dir, &i);
  if (s.ok()) {
    Partition* part;
    s = AcquirePartition(dir, i, &part);
    if (s.ok()) {
      mutex_.Unlock();  // Lokup1() uses per-partition locking
      s = Lokup1(who, parent, name, part, stat);
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

Status FilesystemCli::AcquireAndFetch(  ///
    const User& who, const LookupStat& parent, const Slice& name, Dir** result,
    int* i) {
  mutex_.AssertHeld();
  DirId at(parent);
  Status s = AcquireDir(at, result);
  if (s.ok()) {
    mutex_.Unlock();  // Fetch1() uses per-directory locking
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
  MutexLock lock(dir->mu);  // May be changed to using cv...
  Status s = FetchDir(p.ZerothServer(), dir);
  if (s.ok()) {
    *rv = dir->giga->SelectServer(name);
  }
  return s;
}

// Look up a named directory under a parent directory. On success, a lease for
// the stat of the directory being looked up is returned. The returned lease
// must be released after use. Only valid leases will be returned. Expired
// leases are renewed before they are returned. Each returned lease holds
// a reference to its parent partition. Caller of this function must
// increase the partition's reference count when a lease is returned.
Status FilesystemCli::Lokup1(  ///
    const User& who, const LookupStat& p, const Slice& name, Partition* part,
    Lease** stat) {
  if (!IsLookupOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No x perm");
  MutexLock lock(part->mu);
  // Determine subpartition; also serves as the hash
  // for the LRU lease cache
  uint32_t hash = Hash(name.data(), name.size(), 0);
  uint32_t i = hash & uint32_t(kWays - 1);

  Status s;
  LRUCache<Lease>* const lru = part->cached_leases;
  // Quickly check if we have it already...
  Lease* l = lru->Lookup(name, hash);
  if (l != NULL) {
    if (l->value->LeaseDue() < CurrentMicros()) {
      lru->Erase(name, hash);
      lru->Release(l);
    } else {
      // Directly return the cached lease...
      assert(l->part == part);  // Pending reference increment
      *stat = l;
      return s;
    }
  }

  *stat = NULL;

  // Wait for concurrent conflicting name lookups and check again in case some
  // other thread has done the work for us while we are waiting...
  while (part->busy[i]) part->cv->Wait();
  part->busy[i] = true;
  l = lru->Lookup(name, hash);
  if (l != NULL) {
    if (l->value->LeaseDue() < CurrentMicros()) {
      lru->Erase(name, hash);
      lru->Release(l);
    } else {
      assert(l->part == part);  // Pending reference increment
      *stat = l;
    }
  }

  if (*stat == NULL) {
    // Temporarily unlock for potentially costly lookups...
    part->mu->Unlock();
    LookupStat* tmp = new LookupStat;
    if (fs_ != NULL) {
      s = fs_->Lokup(who, p, name, tmp);
    } else {
      ///
    }

    // Lock again for finishing up ...
    part->mu->Lock();
    if (s.ok()) {
      l = lru->Insert(name, hash, tmp, 1, DeleteLookupStat);
      l->part = part;  // Pending reference increment
      *stat = l;
      if (tmp->LeaseDue() == 0) {  // The lease is non-cacheable
        lru->Erase(name, hash);
      }
    } else {
      delete tmp;
    }
  }

  part->busy[i] = false;
  part->cv->SignalAll();
  return s;
}

Status FilesystemCli::Mkfle1(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    Stat* stat) {
  if (!IsDirWriteOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    fs_->Mkfle(who, p, name, mode, stat);
  } else {
    ///
  }
  return s;
}

Status FilesystemCli::Mkdir1(  ///
    const User& who, const LookupStat& p, const Slice& name, uint32_t mode,
    Stat* stat) {
  if (!IsDirWriteOk(options_, p, who))  // Parental perm checks
    return Status::AccessDenied("No write perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Mkdir(who, p, name, mode, stat);
  } else {
    ///
  }
  return s;
}

Status FilesystemCli::Lstat1(  ///
    const User& who, const LookupStat& p, const Slice& name, Stat* stat) {
  if (!IsLookupOk(options_, p, who))  // Avoid unnecessary server rpc
    return Status::AccessDenied("No x perm");
  Status s;
  if (fs_ != NULL) {
    s = fs_->Lstat(who, p, name, stat);
  } else {
    ///
  }
  return s;
}

void FilesystemCli::DeleteLookupStat(const Slice& key, LookupStat* stat) {
  delete stat;
}

void FilesystemCli::Release(Lease* lease) {
  if (lease == &rtlease_) return;  // Root lease is static...
  Partition* const part = lease->part;
  part->mu->Lock();
  part->cached_leases->Release(lease);
  part->mu->Unlock();
  MutexLock lock(&mutex_);
  Release(part);
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

  LIST_Append(dir, &dirlist_);
  dirs_->Inject(dir, pos);
  dir->refs = 1;
  return s;
}

void FilesystemCli::DeletePartition(const Slice& key, Partition* part) {
  assert(part->key() == key);
  delete part->cached_leases;
  delete part->cv;
  delete part->mu;
  FilesystemCli* cli = part->dir->fscli;
  MutexLock lock(&cli->mutex_);
  cli->Release(part->dir);
  free(part);
}

void FilesystemCli::Release(Partition* part) {
  mutex_.AssertHeld();
  assert(part->in_use != 0);
  part->in_use--;
  if (!part->in_use) {
    piu_->Remove(part->key(), part->hash);
  }
  plru_->Release(part->lru_handle);
}

void FilesystemCli::Ref(Partition* part) {
  mutex_.AssertHeld();
  assert(part->in_use != 0);
  part->in_use++;
  plru_->Ref(part->lru_handle);
}

Status FilesystemCli::AcquirePartition(Dir* dir, int ix, Partition** result) {
  mutex_.AssertHeld();
  char tmp[30];
  Slice key = LRUKey(dir->id, ix, tmp);
  const uint32_t hash = HashKey(key);
  Status s;

  // Search the table first...
  Partition** const pos = piu_->FindPointer(key, hash);
  Partition* part = *pos;
  if (part != NULL) {
    *result = part;
    assert(part->lru_handle->value == part);
    plru_->Ref(part->lru_handle);
    assert(part->in_use != 0);
    part->in_use++;
    return s;
  }

  // Try the LRU cache...
  PartHandl* h = plru_->Lookup(key, hash);
  if (h != NULL) {
    part = h->value;
    *result = part;
    assert(part->lru_handle == h);
    piu_->Inject(part, pos);
    assert(part->in_use == 0);
    part->in_use = 1;
    return s;
  }

  // If we still cannot find it, we create it...
  part = static_cast<Partition*>(malloc(sizeof(Partition) - 1 + key.size()));
  part->index = ix;
  part->key_length = key.size();
  memcpy(dir->key_data, key.data(), key.size());
  part->hash = hash;
  part->cached_leases =
      new LRUCache<Lease>(options_.per_partition_lease_lru_size);
  part->mu = new port::Mutex;
  part->cv = new port::CondVar(part->mu);
  memset(&part->busy[0], 0, kWays);
  part->dir = dir;
  dir->refs++;

  h = plru_->Insert(key, hash, part, 1, DeletePartition);
  *result = part;
  part->lru_handle = h;
  piu_->Inject(part, pos);
  part->in_use = 1;
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
      piu_(NULL),
      options_(options),
      stub_(NULL),
      fs_(NULL),
      rpc_(NULL) {
  dirs_ = new HashTable<Dir>;
  dirlist_.next = &dirlist_;
  dirlist_.prev = &dirlist_;
  plru_ = new LRUCache<PartHandl>(options_.partition_lru_size);
  piu_ = new HashTable<Partition>;

  FormatRoot();

  rtlokupstat_.CopyFrom(rtstat_);
  rtlease_.value = &rtlokupstat_;
}

FilesystemCliOptions::FilesystemCliOptions()
    : per_partition_lease_lru_size(4096),
      partition_lru_size(4096),
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

FilesystemCli::~FilesystemCli() {
  delete plru_;
  delete piu_;
  delete dirs_;
  delete stub_;
  delete rpc_;
  delete fs_;
}

}  // namespace pdlfs
