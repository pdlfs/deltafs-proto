/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */
#pragma once

#include "pdlfs-common/coding.h"
#include "pdlfs-common/env.h"
#include "pdlfs-common/hashmap.h"
#include "pdlfs-common/log_reader.h"
#include "pdlfs-common/log_writer.h"
#include "pdlfs-common/ofs.h"
#include "pdlfs-common/osd.h"
#include "pdlfs-common/port.h"

namespace pdlfs {

class FileSet {
 public:
  // Values are unsigned
  enum RecordType {
    kNoOp = 0x00,  // Paddings that should be ignored

    // Undo required during recovery
    kTryNewFile = 0x01,
    // Redo required during recovery
    kTryDelFile = 0x02,

    // Operation committed
    kNewFile = 0xf1,
    kDelFile = 0xf2
  };

  FileSet(const MountOptions& options, const Slice& name, bool sync_on_close)
      : paranoid_checks(options.paranoid_checks),
        read_only(options.read_only),
        create_if_missing(options.create_if_missing),
        error_if_exists(options.error_if_exists),
        sync_on_close(sync_on_close),
        sync(options.sync),
        name(name.ToString()),
        xfile(NULL),
        xlog(NULL) {}

  ~FileSet() {
    delete xlog;
    if (xfile != NULL) {
      Status s;
      if (sync_on_close) s = xfile->Sync();
      if (s.ok()) xfile->Close();
      delete xfile;
    }
  }

  Status TryNewFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kTryNewFile, fname));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  Status NewFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kNewFile, fname));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        files.Insert(fname);
      }
      return s;
    }
  }

  Status TryDeleteFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kTryDelFile, fname));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      if (s.ok()) {
        files.Erase(fname);
      }
      return s;
    }
  }

  Status DeleteFile(const Slice& fname) {
    if (xlog == NULL) {
      return Status::ReadOnly(Slice());
    } else {
      assert(!read_only);
      Status s = xlog->AddRecord(LogRecord(kDelFile, fname));
      if (s.ok() && sync) {
        s = xfile->Sync();
      }
      return s;
    }
  }

  // File set options
  // Constant after construction
  bool paranoid_checks;
  bool read_only;
  bool create_if_missing;
  bool error_if_exists;
  bool sync_on_close;
  bool sync;

  std::string name;  // Internal name of the file set
  HashSet files;     // Children files

  // File set logging
  static std::string LogRecord(  ///
      RecordType type, const Slice& fname1, const Slice& fname2 = Slice());
  WritableFile* xfile;  // The file backing the write-ahead log
  typedef log::Writer Log;
  Log* xlog;  // Write-ahead logger

 private:
  // No copying allowed
  void operator=(const FileSet& other);
  FileSet(const FileSet&);
};

class Ofs::Impl {
 public:
  Impl(const OfsOptions& options, Osd* osd) : options_(options), osd_(osd) {
    if (options_.info_log == NULL) {
      options_.info_log = Logger::Default();
    }
  }

  ~Impl() {
    // All file sets should have be unmounted
    // at this point
    assert(mtable_.Empty());
  }

  bool HasFileSet(const Slice& mntptr);
  Status LinkFileSet(const Slice& mntptr, FileSet* fset);
  Status UnlinkFileSet(const Slice& mntptr, bool deletion);
  Status ListFileSet(const Slice& mntptr, std::vector<std::string>* names);
  Status SynFileSet(const Slice& mntptr);

  bool HasFile(const ResolvedPath& fp);
  Status GetFile(const ResolvedPath& fp, std::string* data);
  Status PutFile(const ResolvedPath& fp, const Slice& data);
  Status FileSize(const ResolvedPath& fp, uint64_t* size);
  Status DeleteFile(const ResolvedPath& fp);
  Status NewSequentialFile(const ResolvedPath& fp, SequentialFile** result);
  Status NewRandomAccessFile(const ResolvedPath& fp, RandomAccessFile** result);
  Status NewWritableFile(const ResolvedPath& fp, WritableFile** result);
  std::string TEST_GetObjectName(const ResolvedPath& fp);
  Status CopyFile(const ResolvedPath& sp, const ResolvedPath& dp);

 private:
  port::Mutex mutex_;
  HashMap<FileSet> mtable_;

  static std::string OfsName(const FileSet*, const Slice& name);
  typedef ResolvedPath OfsPath;

  // No copying allowed
  void operator=(const Impl&);
  Impl(const Impl&);

  friend class Ofs;
  OfsOptions options_;
  Osd* osd_;
};

inline void PutOp(  ///
    std::string* dst, FileSet::RecordType type, const Slice& fname1,
    const Slice& fname2 = Slice()) {
  dst->push_back(static_cast<unsigned char>(type));
  PutLengthPrefixedSlice(dst, fname1);
  PutLengthPrefixedSlice(dst, fname2);
}

// Each record is formatted as defined below:
//   timestamp: uint64_t
//   num_ops: uint32_t
//  For each op:
//   op_type: uint8_t
//   fname1_len: varint32_t
//   fname1: char[n]
//   fname2_len: varint32_t
//   fname2: char[n]
inline std::string FileSet::LogRecord(  ///
    FileSet::RecordType type, const Slice& fname1, const Slice& fname2) {
  std::string rec;
  size_t max_record_size = 8 + 4 + 1 + 5 + 5;
  max_record_size += fname1.size();
  max_record_size += fname2.size();
  rec.reserve(max_record_size);
  PutFixed64(&rec, CurrentMicros());
  PutFixed32(&rec, 1);
  PutOp(&rec, type, fname1, fname2);
  return rec;
}

}  // namespace pdlfs
