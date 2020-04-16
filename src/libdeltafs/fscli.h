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

#include "fscomm.h"

#include "pdlfs-common/fsdbx.h"
#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"
#include "pdlfs-common/rpc.h"

namespace pdlfs {

struct FilesystemOptions;
struct DirIndexOptions;

struct FilesystemCliOptions {
  FilesystemCliOptions();
  size_t per_partition_lease_lru_size;
  size_t partition_lru_size;
  bool skip_perm_checks;
  // Total number of virtual servers
  int vsrvs;
  // Number of servers
  int nsrvs;
};

class DirIndex;

// A filesystem client may either talk to a local metadata manager via the
// FilesystemIf interface or talk to a remote filesystem server through rpc.
class FilesystemCli {
 public:
  explicit FilesystemCli(const FilesystemCliOptions& options);
  ~FilesystemCli();

  Status OpenFilesystemCli(const FilesystemOptions& options,
                           const std::string& fsloc);

  Status Open(RPC* rpc, const std::string* uri);

  struct AT;  // Relative root of a pathname

  Status Atdir(const User& who, const AT* at, const char* pathname, AT**);
  Status Mkfle(const User& who, const AT* at, const char* pathname,
               uint32_t mode, Stat* stat);
  Status Mkdir(const User& who, const AT* at, const char* pathname,
               uint32_t mode, Stat* stat);
  Status Lstat(const User& who, const AT* at, const char* pathname, Stat* stat);

  void Destroy(AT* at);

  Status TEST_ProbeDir(const DirId& id);

 private:
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
  Status Resolu(const User& who, const AT* at, const char* pathname,
                Lease** parent_dir, Slice* last_component,
                bool* has_tailing_slashes);
  // Resolve a filesystem path down to the last component of the path. On
  // success, return the name of the last component and a lease on its parent
  // directory. Return a non-OK status on error. Path following (not including)
  // the erroneous directory is returned as well to assist debugging.
  Status Resolv(const User& who, Lease* relative_root, const char* pathname,
                Lease** parent_dir, Slice* last_component,
                const char** remaining_path);
  Status Lokup(const User& who, const LookupStat& parent, const Slice& name,
               Lease** stat);
  Status AcquireAndFetch(const User& who, const LookupStat& parent,
                         const Slice& name, Dir**, int*);

  Status Fetch1(const User& who, const LookupStat& parent, const Slice& name,
                Dir* dir, int*);
  Status Lokup1(const User& who, const LookupStat& parent, const Slice& name,
                Partition* part, Lease** stat);

  Status Mkfle1(const User& who, const LookupStat& parent, const Slice& name,
                uint32_t mode, Stat* stat);
  Status Mkdir1(const User& who, const LookupStat& parent, const Slice& name,
                uint32_t mode, Stat* stat);
  Status Lstat1(const User& who, const LookupStat& parent, const Slice& name,
                Stat* stat);

  Status Mkfle2(const User& who, const LookupStat& parent, const Slice& name,
                uint32_t mode, int i, Stat* stat);
  Status Mkdir2(const User& who, const LookupStat& parent, const Slice& name,
                uint32_t mode, int i, Stat* stat);
  Status Lstat2(const User& who, const LookupStat& parent, const Slice& name,
                int i, Stat* stat);

  // No copying allowed
  void operator=(const FilesystemCli& cli);
  FilesystemCli(const FilesystemCli&);

  // A lease to a pathname lookup stat. Also serves as an LRU cache entry.
  struct Lease {
    LookupStat* value;
    void (*deleter)(const Slice&, LookupStat* value);
    Lease* next_hash;
    Lease* next;
    Lease* prev;
    Partition* part;
    size_t charge;
    size_t key_length;
    uint32_t refs;
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    bool in_cache;  // False when evicted
    char key_data[1];  // Beginning of key

    Slice key() const {  // Return key of the lease.
      return Slice(key_data, key_length);
    }

    ///
  };
  static void DeleteLookupStat(const Slice& key, LookupStat* stat);
  void Release(Lease* lease);

  // State below is protected by mutex_
  port::Mutex mutex_;
  // Per-directory control block. Each directory consists of one or more
  // partitions. Per-directory giga status is serialized here.
  // Simultaneously serves as an hash table entry.
  struct Dir {
    DirId id;
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
  // All directories cached at the client
  HashTable<Dir>* dirs_;
  Dir dirlist_;  // Dummy head of the linked list

  typedef LRUEntry<Partition> PartHandl;
  enum { kWays = 8 };  // Must be a power of 2
  // Per-partition directory control block. Pathname lookups within a single
  // directory partition are serialized here.
  // Simultaneously serves as an hash table entry.
  struct Partition {
    PartHandl* lru_handle;
    Dir* dir;
    LRUCache<Lease>* cached_leases;
    port::Mutex* mu;
    port::CondVar* cv;
    Partition* next_hash;
    size_t key_length;
    uint32_t in_use;  //  Number of active uses
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    int index;
    unsigned char busy[kWays];  // True if a dir subpartition is busy
    char key_data[1];           // Beginning of key

    Slice key() const {  // Return key of the partition
      return Slice(key_data, key_length);
    }

    ///
  };
  // We keep an LRU cache of directory partitions in memory so that we don't
  // need to create a new one every time a directory partition is accessed.
  LRUCache<PartHandl>* plru_;
  static void DeletePartition(const Slice& key, Partition* partition);
  Status AcquirePartition(Dir* dir, int index, Partition**);
  void Ref(Partition* partition);
  void Release(Partition* partition);
  HashTable<Partition>* piu_;  // Partition in use.

  void FormatRoot();
  // Constant after client open
  Stat rtstat_;
  LookupStat rtlokupstat_;
  Lease rtlease_;
  FilesystemCliOptions options_;
  rpc::If** stub_;
  FilesystemIf* fs_;
  RPC* rpc_;
};

}  // namespace pdlfs
