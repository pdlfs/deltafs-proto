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
#pragma once

#include "fscom.h"

#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/random.h"
#include "pdlfs-common/rpc.h"

namespace pdlfs {

struct FilesystemDbStats;
struct DirIndexOptions;
struct DirId;

class FilesystemCli;
class Filesystem;
class DirIndex;

// Client context to make filesystem calls.
class FilesystemCliCtx {
 public:
  explicit FilesystemCliCtx(int seed = 301) : rnd_(seed), stubs_(NULL), n_(0) {}

  ~FilesystemCliCtx() {
    if (stubs_) {
      for (int i = 0; i < n_; i++) {
        delete stubs_[i];
      }
    }
    delete[] stubs_;
  }

  User who;

 private:
  Random rnd_;
  friend class FilesystemCli;
  rpc::If** stubs_;
  int n_;
};

struct FilesystemCliOptions {
  FilesystemCliOptions();
  size_t per_partition_lease_lru_size;
  size_t partition_lru_size;
  size_t batch_size;
  bool skip_perm_checks;
};

// A filesystem client may either talk to a local metadata manager via the
// FilesystemIf interface or talk to a remote filesystem server through rpc.
class FilesystemCli {
 public:
  explicit FilesystemCli(const FilesystemCliOptions& options);
  class UriMapper {
   public:
    UriMapper() {}
    virtual ~UriMapper();

    virtual std::string GetUri(int srv_idx, int port_idx) const = 0;

   private:
    void operator=(const UriMapper& other);
    UriMapper(const UriMapper&);
  };
  void RegisterFsSrvUris(RPC* rpc, const UriMapper* uri_mapper, int srvs,
                         int ports_per_srv = 1);
  void SetLocalFs(Filesystem* fs);
  ~FilesystemCli();

  // Reference to a resolved parent directory serving as a relative root for
  // pathnames
  struct AT;
  Status Atdir(FilesystemCliCtx* ctx, const AT* at, const char* pathname, AT**);
  void Destroy(AT* at);

  Status Mkfle(FilesystemCliCtx* ctx, const AT* at, const char* pathname,
               uint32_t mode, Stat* stat);
  Status Mkdir(FilesystemCliCtx* ctx, const AT* at, const char* pathname,
               uint32_t mode, Stat* stat);
  Status Lstat(FilesystemCliCtx* ctx, const AT* at, const char* pathname,
               Stat* stat);

  // Reference to a batch of create operations buffered at the client under a
  // server-issued parent dir lease
  struct BAT;
  Status BatchStart(FilesystemCliCtx* ctx, const AT* at, const char* pathname,
                    BAT** bat);
  Status BatchInsert(BAT* bat, const char* name);
  Status BatchCommit(BAT* bat);
  Status BatchEnd(BAT* bat);

  Status TEST_Mkfle(FilesystemCliCtx* ctx, const LookupStat& parent,
                    const Slice& fname, const Stat& stat,
                    FilesystemDbStats* stats);
  Status TEST_Mkfle(FilesystemCliCtx* ctx, const char* pathname,
                    const Stat& stat, FilesystemDbStats* stats);
  Status TEST_Lstat(FilesystemCliCtx* ctx, const LookupStat& parent,
                    const Slice& fname, Stat* stat, FilesystemDbStats* stats);
  Status TEST_Lstat(FilesystemCliCtx* ctx, const char* pathname, Stat* stat,
                    FilesystemDbStats* stats);
  uint32_t TEST_TotalLeasesAtPartition(const DirId& dir_id, int ix);
  Status TEST_ProbePartition(const DirId& dir_id, int ix);
  uint32_t TEST_TotalPartitionsInMemory();
  Status TEST_ProbeDir(const DirId& dir_id);
  uint32_t TEST_TotalDirsInMemory();

 private:
  struct WriBuf;
  struct BatchedCreates;
  struct Lease;
  struct Partition;
  struct Dir;

  // Resolve a filesystem path down to the last component of the path. Return
  // the name of the last component and a lease on its parent directory on
  // success. In addition, return whether the specified path has tailing
  // slashes. This method is a wrapper function over "Resolv", and should be
  // called instead of it. When the input filesystem path points to the root
  // directory, the root directory itself is returned as the parent directory
  // and the name of the last component of the path is set to empty.
  Status Resolu(FilesystemCliCtx* ctx, const AT* at, const char* pathname,
                Lease** parent_dir, Slice* last_component,
                bool* has_tailing_slashes);
  // Resolve a filesystem path down to the last component of the path. On
  // success, return the name of the last component and a lease on its parent
  // directory. Return a non-OK status on error. Path following (not including)
  // the erroneous directory is returned as well to assist debugging.
  Status Resolv(FilesystemCliCtx* ctx, Lease* relative_root,
                const char* pathname, Lease** parent_dir, Slice* last_component,
                const char** remaining_path);

  Status Lokup(FilesystemCliCtx* ctx, const LookupStat& parent,
               const Slice& name, LokupMode mode, Lease** stat);
  Status CreateBatch(FilesystemCliCtx* ctx, const LookupStat& parent,
                     BatchedCreates**);
  Status AcquireAndFetch(FilesystemCliCtx* ctx, const LookupStat& parent,
                         const Slice& name, Dir**, int* idx);

  Status Fetch1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, Dir* dir, int*);
  Status Lokup1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, LokupMode mode, Partition* part,
                Lease** stat);
  Status Mkfls1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t mode, bool force_flush, int srv_idx,
                WriBuf* buf);
  Status Mkfle1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t mode, Stat* stat);
  Status Mkdir1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t mode, Stat* stat);
  Status Lstat1(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, Stat* stat);

  Status Lokup2(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t hash, LokupMode mode,
                Partition* part, Lease** stat);
  Status Mkfls2(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& namearr, uint32_t n, uint32_t mode, int srv_idx);
  Status Mkfle2(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t mode, int srv_idx, Stat* stat);
  Status Mkdir2(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, uint32_t mode, int srv_idx, Stat* stat);
  Status Lstat2(FilesystemCliCtx* ctx, const LookupStat& parent,
                const Slice& name, int srv_idx, Stat* stat);

  rpc::If* PrepareStub(FilesystemCliCtx* ctx, int srv_idx);

  // No copying allowed
  void operator=(const FilesystemCli& cli);
  FilesystemCli(const FilesystemCli&);

  struct WriBuf {
    std::string namearr;
    port::Mutex mu;
    uint32_t n;
  };
  struct BatchedCreates {
    FilesystemCliCtx* ctx;
    uint32_t mode;
    uint32_t refs;
    port::Mutex mu;
    // 0 if not committed, 1 if being committed, or 2 if committed
    unsigned char commit_status;
    Status bg_status;
    WriBuf* wribufs;
    Dir* dir;
  };
  typedef LRUEntry<Lease> LeaseHandl;
  // A lease to a pathname lookup stat. Struct doubles as a hash table entry.
  struct Lease {
    LeaseHandl* lru_handle;
    LookupStat* rep;
    BatchedCreates* batch;
    // Each non-LRU reference to the lease additionally requires a reference to
    // the parent partition. This prevents the parent partition from being
    // removed from the memory.
    Partition* part;
    Lease* next_hash;
    size_t key_length;
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    bool out;
    char key_data[1];  // Beginning of key

    Slice key() const {  // Return key of the lease.
      return Slice(key_data, key_length);
    }

    ///
  };
  static void DeleteLease(const Slice& key, Lease* lease);
  void Release(Lease* lease);

  // State below is protected by mutex_
  port::Mutex mutex_;
  // Per-directory control block. Each directory consists of one or more
  // partitions. Per-directory giga status is serialized here.
  // Struct doubles as an hash table entry.
  struct Dir {
    DirId* id;
    DirIndexOptions* giga_opts;
    DirIndex* giga;
    Dir* next_hash;
    Dir* next;
    Dir* prev;
    FilesystemCli* fscli;
    port::Mutex* mu;
    size_t key_length;
    uint32_t refs;  // Total number of refs (system + active)
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    unsigned char fetched;
    char key_data[1];  // Beginning of key

    Slice key() const {  // Return the key of the dir
      return Slice(key_data, key_length);
    }

    ///
  };
  // Obtain the control block for a specified directory.
  Status AcquireDir(const DirId& id, Dir**);
  // Fetch dir info from server.
  Status FetchDir(uint32_t zeroth_server, Dir* dir);
  // Release a reference to the dir.
  void Release(Dir* dir);
  // All directories currently kept in memory.
  HashTable<Dir>* dirs_;
  Dir dirlist_;  // Dummy head of the linked list

  typedef LRUEntry<Partition> PartHandl;
  enum { kWays = 8 };  // Must be a power of 2
  // Per-partition directory control block. Pathname lookups within a single
  // directory partition are serialized here.
  // Struct doubles as an hash table entry.
  struct Partition {
    PartHandl* lru_handle;
    Dir* dir;
    LRUCache<LeaseHandl>* cached_leases;
    HashTable<Lease>* leases;
    port::Mutex* mu;
    port::CondVar* cv;
    Partition* next_hash;
    size_t key_length;
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    int index;
    unsigned char busy[kWays];  // True if a dir subpartition is busy
    char key_data[1];           // Beginning of key

    Slice key() const {  // Return key of the partition
      return Slice(key_data, key_length);
    }

    ///
  };
  // We keep an LRU cache of directory partitions in memory so that we can reuse
  // them when a recent directory partition is accessed.
  LRUCache<PartHandl>* plru_;
  static void DeletePartition(const Slice& key, Partition* partition);
  // Obtain the control block for a specific directory partition.
  Status AcquirePartition(Dir* dir, int index, Partition**);
  // Add a reference to a specific directory partition preventing it from being
  // deleted from memory.
  void Ref(Partition* partition);
  // Release a reference to a specified directory partition.
  void Release(Partition* partition);
  // All directory partition control blocks currently kept in memory. These
  // include both the control blocks currently referenced by the LRU cache and
  // the control blocks that have been evicted from the cache but still have
  // remaining references.
  HashTable<Partition>* pars_;

  void FormatRoot();
  // Constant after client open
  Stat rtstat_;
  LookupStat rtlokupstat_;
  Lease rtlease_;
  FilesystemCliOptions options_;
  // If not NULL, the cli runs in a serverless mode
  Filesystem* fs_;  // This is a weak reference; fs_ is not owned by us
  // The following is set when running in the
  // traditional client-server mode
  const UriMapper* uri_mapper_;  // Not owned by us
  int ports_per_srv_;
  int srvs_;
  RPC* rpc_;  // Not owned by us
};

}  // namespace pdlfs
