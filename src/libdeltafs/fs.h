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

#include "fsapi.h"

#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/lru.h"
#include "pdlfs-common/port.h"

#if __cplusplus >= 201103L
#define OVERRIDE override
#else
#define OVERRIDE
#endif
namespace pdlfs {

struct FilesystemDir;
struct FilesystemDbStats;
struct DirIndexOptions;
struct DirId;
struct User;

class FilesystemDb;
class DirIndex;

// Options for controlling the filesystem.
struct FilesystemOptions {
  FilesystemOptions();
  size_t dir_lru_size;
  bool skip_partition_checks;
  bool skip_name_collision_checks;
  bool skip_lease_due_checks;
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

class Filesystem : public FilesystemIf {
 public:
  explicit Filesystem(const FilesystemOptions& options);
  virtual ~Filesystem();

  virtual Status Bukin(const User& who, const LookupStat& parent,
                       const std::string& table_dir) OVERRIDE;
  virtual Status Mkfls(const User& who, const LookupStat& parent,
                       const Slice& namearr, uint32_t mode,
                       uint32_t* n) OVERRIDE;
  virtual Status Mkfle(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) OVERRIDE;
  virtual Status Mkdir(const User& who, const LookupStat& parent,
                       const Slice& name, uint32_t mode, Stat* stat) OVERRIDE;
  virtual Status Lokup(const User& who, const LookupStat& parent,
                       const Slice& name, LookupStat* stat) OVERRIDE;
  virtual Status Lstat(const User& who, const LookupStat& parent,
                       const Slice& name, Stat* stat) OVERRIDE;

  void SetDb(FilesystemDb* db);
  // Deterministically calculate a zeroth server based on a specified directory
  // id.
  static uint32_t PickupServer(const DirId& id);

  Status TEST_Lstat(const User& who, const LookupStat& parent,
                    const Slice& name, Stat* stat, FilesystemDbStats* stats);
  Status TEST_Mkfle(const User& who, const LookupStat& parent,
                    const Slice& name, const Stat& stat,
                    FilesystemDbStats* stats);
  FilesystemDir* TEST_ProbeDir(const DirId& dir_id);
  const FilesystemDbStats& TEST_FetchDbStats(FilesystemDir* dir);
  void TEST_Release(FilesystemDir* dir);
  uint32_t TEST_TotalDirsInMemory();
  uint64_t TEST_LastIno();  // Return the last ino assigned

 private:
  struct Dir;

  Status Bukin1(const User& who, const DirId& at, const LookupStat& parent,
                Dir* dir, const std::string& table_dir);
  Status Mknos1(const User& who, const DirId& at, const Slice& namearr,
                uint64_t startino, const LookupStat& parent, Dir* dir,
                Stat* stat, uint32_t* n, FilesystemDbStats* stats);
  Status Mknod1(const User& who, const DirId& at, const Slice& name,
                const LookupStat& parent, Dir* dir, const Stat& stat,
                FilesystemDbStats* stats);
  Status Lokup1(const User& who, const DirId& at, const Slice& name,
                const LookupStat& parent, Dir* dir, LookupStat* stat,
                FilesystemDbStats* stats);
  Status Lstat1(const User& who, const DirId& at, const Slice& name,
                const LookupStat& parent, Dir* dir, Stat* stat,
                FilesystemDbStats* stats);
  Status CheckAndPut(const DirId& at, const Slice& name, const Stat& stat,
                     FilesystemDbStats* stats);

  // No copying allowed
  void operator=(const Filesystem& fs);
  Filesystem(const Filesystem&);

  port::Mutex mutex_;
  uint64_t inoq_;  // The last inode no.
  // If the last ino ever assigned is still ino, reduce it by n
  void TryReuseIno(uint64_t ino, size_t n = 1) {
    mutex_.AssertHeld();
    if (ino == inoq_) {
      inoq_ -= n;
    }
  }

  typedef LRUEntry<Dir> DirHandl;
  enum { kWays = 8 };  // Must be a power of 2
  // Per-directory control block.
  // Struct simultaneously serves as an hash table entry.
  struct Dir {
    DirId* id;
    DirHandl* lru_handle;
    DirIndexOptions* giga_opts;
    DirIndex* giga;
    Dir* next_hash;
    FilesystemDbStats* stats;
    Filesystem* fs;
    port::CondVar* cv;
    port::Mutex* mu;
    size_t key_length;
    uint32_t hash;  // Hash of key(); used for fast partitioning and comparisons
    unsigned char fetched;
    unsigned char busy[kWays];  // True if a name subset is busy
    char key_data[1];           // Beginning of key

    Slice key() const {  // Return the key of the dir
      return Slice(key_data, key_length);
    }

    ///
  };
  // An LRU cache of directory control blocks is kept in memory. A certain
  // number of control blocks may be cached in memory. When the maximum is
  // reached, cache eviction will start.
  LRUCache<DirHandl>* dlru_;
  static void DeleteDir(const Slice& key, Dir* dir);
  // Obtain the control block for a specific directory.
  Status AcquireDir(const DirId&, Dir**);
  // Fetch dir from db.
  Status MaybeFetchDir(Dir* dir);
  // Release a reference to a specified directory control block.
  void Release(Dir*);
  // It is possible for a control block to be evicted from the LRU cache while
  // the block itself is still being used by some threads. This would cause a
  // caller seeking the control block to believe that there is no such block in
  // memory and go create a new control block causing two control blocks of a
  // single directory to appear in memory. To resolve this problem, we use a
  // separate table to index all directory control blocks that exist in memory.
  // A caller is expected to check the table whenever it gets a miss from the
  // cache. If the caller finds the control block it seeks, it adds a new
  // reference to the control block (and potentially re-inserts the control
  // block to the cache). We only keep a certain number of control blocks in
  // memory. If the maximum is reached, subsequent filesystem operations may be
  // rejected until slots reappear.
  HashTable<Dir>* dirs_;

  // Constant after server opening
  FilesystemOptions options_;
  FilesystemDb* db_;  // db_ is not owned by us
};

}  // namespace pdlfs
#undef OVERRIDE
