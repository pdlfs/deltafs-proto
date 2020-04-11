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

#include "fsdb.h"

#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

struct DirIndexOptions;
class DirIndex;

// Options for controlling the filesystem.
struct FilesystemOptions {
  FilesystemOptions();
  size_t dir_lru_size;
  bool skip_partition_checks;
  bool skip_name_collision_checks;
  bool skip_perm_checks;
  bool rdonly;
  // Total number of virtual servers
  int vsrvs;
  // Number of servers
  int nsrvs;
  // Server id.
  int srvid;
  // My dnode no.
  int mydno;
};
// User id information.
struct User {
  uint32_t uid;
  uint32_t gid;
};

class Filesystem {
 public:
  explicit Filesystem(const FilesystemOptions& options);
  ~Filesystem();

  Status OpenFilesystem(const std::string& fsloc);

  Status Mkfle(const User& who, const LookupStat& parent, const Slice& name,
               uint32_t mode, Stat* stat);
  Status Mkdir(const User& who, const LookupStat& parent, const Slice& name,
               uint32_t mode, Stat* stat);
  Status Lokup(const User& who, const LookupStat& parent, const Slice& name,
               LookupStat* stat);
  Status Lstat(const User& who, const LookupStat& parent, const Slice& name,
               Stat* stat);

  // Deterministically assign a zeroth server to
  // a given directory id.
  int PickupServer(const DirId& id);

  // Fake a directory access.
  Status TEST_ProbeDir(const DirId& id);

 private:
  struct Dir;

  Status Mknod1(const User& who, const DirId& at, const Slice& name,
                uint64_t ino, uint32_t type, uint32_t mode,
                const LookupStat& parent, Dir* dir, Stat* stat);
  Status Lokup1(const User& who, const DirId& at, const Slice& name, Dir* dir,
                const LookupStat& parent, LookupStat* stat);
  Status Lstat1(const User& who, const DirId& at, const Slice& name, Dir* dir,
                const LookupStat& parent, Stat* stat);

  Status Put(const User& who, const DirId& at, const Slice& name, uint64_t dno,
             uint64_t ino, uint32_t zsrv, uint32_t mode, Stat* stat);

  // No copying allowed
  void operator=(const Filesystem& fs);
  Filesystem(const Filesystem&);

  port::Mutex mutex_;
  uint64_t inoq_;  // The last inode no.
  void TryReuseIno(uint64_t ino) {
    mutex_.AssertHeld();
    if (ino == inoq_) {
      --inoq_;
    }
  }

  typedef LRUEntry<Dir> DirHandl;
  // Directory control blocks.
  // Also serves as entries in an Hash Table.
  enum { kWays = 8 };  // Must be a power of 2
  struct Dir {
    DirHandl* lru_handle;
    DirIndexOptions* giga_opts;
    DirIndex* giga;
    Dir* next_hash;
    port::CondVar* cv;
    port::Mutex* mu;
    uint64_t dno;
    uint64_t ino;
    size_t key_length;
    uint32_t in_use;  // Number of active uses
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    unsigned char fetched;
    unsigned char busy[kWays];  // Has ongoing namespace or stat changes
    char key_data[1];           // Beginning of key

    Slice key() const {  // Return the key of the dir.
      return Slice(key_data, key_length);
    }

    ///
  };
  // An LRU cache of directory control blocks is kept in memory. A certain
  // number of control blocks may be cached in memory. When the maximum is
  // reached, cache eviction will start.
  LRUCache<DirHandl>* dlru_;
  static void FreeDir(const Slice& key, Dir* dir);
  // Obtain the control block for a given directory.
  Status AcquireDir(const DirId&, Dir**);
  // Fetch information on a directory from db.
  Status MaybeFetchDir(Dir* dir);
  // Release an active reference to a directory control block.
  void Release(Dir*);
  // It is possible for a control block to be evicted from the LRU cache while
  // the block itself is still being used by some threads. This would cause a
  // caller seeking the control block to believe that there is no such block in
  // memory and go create a new control block causing two control blocks of a
  // single directory to appear in memory. To resolve this problem, we use a
  // separate table to hold all directory control blocks that are currently
  // being used. A caller checks the table before it checks the LRU cache. If
  // the caller finds the control block it seeks from the table, it directly
  // adds a reference to the control block and no more cache queries will be
  // needed. We only keep a certain number of control blocks in the table. If
  // the maximum is reached, subsequent filesystem operations may be rejected
  // until one or more slots in the table reappear.
  HashTable<Dir>* diu_;  // I.e., Directory in use.

  FilesystemOptions options_;

  MDB* mdb_;
  DB* db_;
};

}  // namespace pdlfs
