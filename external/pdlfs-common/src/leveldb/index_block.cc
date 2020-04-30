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

/*
 * Copyright (c) 2013 Facebook, Inc. All rights reserved.
 * This source code is dual-licensed under the GPLv2 and Apache 2.0
 * License that can both be found at https://github.com/facebook/rocksdb.
 * One may select either of the two licenses.
 */
#include "pdlfs-common/leveldb/index_block.h"
#include "pdlfs-common/leveldb/block.h"
#include "pdlfs-common/leveldb/block_builder.h"
#include "pdlfs-common/leveldb/format.h"
#include "pdlfs-common/leveldb/internal_types.h"
#include "pdlfs-common/leveldb/iterator.h"
#include "pdlfs-common/leveldb/options.h"

#include "pdlfs-common/coding.h"
#include "pdlfs-common/ect.h"

namespace pdlfs {

IndexBuilder::~IndexBuilder() {}

IndexReader::~IndexReader() {}

class DefaultIndexBuilder : public IndexBuilder {
 public:
  DefaultIndexBuilder(const Options* options)
      : index_block_builder_(options->index_block_restart_interval,
                             options->comparator) {}

  virtual void AddIndexEntry(std::string* last_key, const Slice* next_key,
                             const BlockHandle& block_handle) {
    const Comparator* const comparator = index_block_builder_.comparator();
    if (next_key != NULL) {
      comparator->FindShortestSeparator(last_key, *next_key);
    } else {
      comparator->FindShortSuccessor(last_key);
    }

    std::string encoding;
    block_handle.EncodeTo(&encoding);
    index_block_builder_.Add(*last_key, encoding);
  }

  virtual Slice Finish() { return index_block_builder_.Finish(); }

  virtual size_t CurrentSizeEstimate() const {
    return index_block_builder_.CurrentSizeEstimate();
  }

  virtual Status ChangeOptions(const Options* options) {
    index_block_builder_.ChangeRestartInterval(
        options->index_block_restart_interval);
    return Status::OK();
  }

  virtual void OnKeyAdded(const Slice& key) {
    // empty
  }

 private:
  BlockBuilder index_block_builder_;
};

class DefaultIndexReader : public IndexReader {
 public:
  DefaultIndexReader(const BlockContents& contents, const Options* options)
      : cmp_(options->comparator), block_(contents) {}

  virtual size_t ApproximateMemoryUsage() const { return block_.size(); }

  virtual Iterator* NewIterator() { return block_.NewIterator(cmp_); }

 private:
  const Comparator* cmp_;
  Block block_;
};

IndexBuilder* IndexBuilder::Create(const Options* options) {
  return new DefaultIndexBuilder(options);
}

IndexReader* IndexReader::Create(const BlockContents& contents,
                                 const Options* options) {
  return new DefaultIndexReader(contents, options);
}

}  // namespace pdlfs
