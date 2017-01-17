#include <exception>
#include <iostream>
#include <fstream>
#include <csignal>
#include "util/coding.h"
#include "env_nvm.h"
#include <liblightnvm.h>

#include <execinfo.h>
#ifndef NVM_TRACE
#define NVM_TRACE 1
void nvm_trace_pr(void) {
  void *array[1024];
  size_t size;
  char **strings;
  size_t i;

  size = backtrace(array, 1024);
  strings = backtrace_symbols(array, size);

  printf("Got %zd stack frames.\n", size);

  for (i = 0; i < size; i++) {
    printf("%s\n", strings[i]);
  }

  free(strings);
}
#endif

int count = 0;

namespace rocksdb {

NvmFile::NvmFile(
  EnvNVM* env, const FPathInfo& info, const std::string mpath
) :
  env_(env), refs_(), info_(info), fsize_(),
  align_nbytes_(), stripe_nbytes_(), blk_nbytes_(), blks_() {
  NVM_DBG(this, "");

  const struct nvm_geo *geo = nvm_dev_get_geo(env_->store_->GetDev());

  align_nbytes_ = geo->nplanes * geo->nsectors * geo->sector_nbytes;
  stripe_nbytes_ = align_nbytes_ * geo->nchannels * geo->nluns;
  blk_nbytes_ = stripe_nbytes_ * geo->nblocks * geo->npages;

  std::deque<std::vector<struct nvm_addr>> meta_vblks;

  Status s = env_->posix_->FileExists(mpath);
  if (s.ok()) {                                 // Load from meta
    std::string dev_path;
    s = env_->rmeta(mpath, fsize_, dev_path, meta_vblks);
    if (!s.ok()) {
      throw std::runtime_error("Failed reading meta");
    }
    if (dev_path.compare(env_->store_->GetDevPath())) {
      throw std::runtime_error("Invalid device");
    }
  }

  for (size_t i = 0; i < meta_vblks.size(); ++i) {
    struct nvm_vblk *blk = nvm_vblk_alloc(
      env_->store_->GetDev(),
      &meta_vblks[i][0],
      meta_vblks[i].size()
    );
    if (!blk) {
      throw std::runtime_error("Failed allocating vblk");
    }
    blks_.push_back(blk);
  }

  buf_file_offset_ = fsize_;
  buf_nbytes_ = 0;
  buf_nbytes_max_ = blk_nbytes_;
  buf_ = (char*)nvm_buf_alloc(geo, buf_nbytes_max_);
  if (!buf_) {
    throw std::runtime_error("Failed allocating buffer");
  }
}

NvmFile::~NvmFile(void) {
  NVM_DBG(this, "");

  for (auto &blk : blks_) {
    if (blk) {
      nvm_vblk_free(blk);
    }
  }

  free(buf_);
}

bool NvmFile::IsNamed(const std::string& fname) const {
  NVM_DBG(this, "fname(" << fname << ")");

  return !info_.fname().compare(fname);
}

const std::string& NvmFile::GetFname(void) const {
  NVM_DBG(this, "return(" << info_.fname() << ")");

  return info_.fname();
}

const std::string& NvmFile::GetDpath(void) const {
  NVM_DBG(this, "return(" << info_.dpath() << ")");

  return info_.dpath();
}

void NvmFile::Rename(const std::string& fname) {
  NVM_DBG(this, "fname(" << fname << ")");

  info_.fname(fname);
}

size_t NvmFile::GetRequiredBufferAlignment(void) const {
  NVM_DBG(this, "returning(" << align_nbytes_ << ")");

  return align_nbytes_;
}

void NvmFile::Ref(void) {
  MutexLock lock(&refs_mutex_);

  NVM_DBG(this, "refs_(" << refs_ << ")");

  ++refs_;

  NVM_DBG(this, "refs_(" << refs_ << ")");
}

void NvmFile::Unref(void) {
  NVM_DBG(this, "");

  bool do_delete = false;

  {
    MutexLock lock(&refs_mutex_);
    --refs_;
    if (refs_ < 0) {
      do_delete = true;
    }
  }

  if (do_delete) {
    NVM_DBG(this, "deleting!");
    delete this;
  }
}

std::string NvmFile::txt(void) const {
  std::stringstream ss;
  ss << "fname(" << info_.fname() << ") ";
  return ss.str();
}

// Used by WritableFile
bool NvmFile::UseDirectIO(void) const {
  NVM_DBG(this, "return(" << std::boolalpha << c_UseDirectIO << ")");

  return c_UseDirectIO;
}

// Used by WritableFile
bool NvmFile::UseOSBuffer(void) const {
  NVM_DBG(this, "return(" << std::boolalpha << c_UseOSBuffer << ")");

  return c_UseOSBuffer;
}

// Used by WritableFile
bool NvmFile::IsSyncThreadSafe(void) const {
  NVM_DBG(this, "hard-coded return");

  return true;
}

//
// Implemented using the address of the NvmFile instance. This seem to satisfy
// the requirements: it lives as long as need be, is unique and probably won't
// have the same address after termination.
//
// Although... depending on what is meant by prefix... they probably could be.
//
//
// Used by RandomAccessFile, WritableFile
size_t NvmFile::GetUniqueId(char* id, size_t max_size) const {
  NVM_DBG(this, "");

  if (max_size < (kMaxVarint64Length*3)) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, (uint64_t)this);
  rid = EncodeVarint64(rid, (uint64_t)this);
  rid = EncodeVarint64(rid, (uint64_t)this);
  assert(rid >= id);

  return static_cast<size_t>(rid - id);
}

// Used by WritableFile
uint64_t NvmFile::GetFileSize(void) const {
  NVM_DBG(this, "return(" << fsize_ << ")");

  return fsize_;
}

// Used by WritableFile
void NvmFile::PrepareWrite(size_t offset, size_t len) {
  NVM_DBG(this, "offset(" << offset << "), len(" << len << ") ignoring...");
}

// Used by WritableFile
Status NvmFile::Allocate(uint64_t offset, uint64_t len) {
  NVM_DBG(this, "offset(" << offset << "), len(" << len << ") ignoring");

  return Status::OK();
}

// Used by WritableFile
Status NvmFile::Sync(void) {
  NVM_DBG(this, "writing file meta to default env...");

  return Flush();
}

// Used by WritableFile
Status NvmFile::Fsync(void) {
  NVM_DBG(this, "writing file meta to default env...");

  return Flush();
}

// Used by WritableFile
Status NvmFile::RangeSync(uint64_t offset, uint64_t nbytes) {
  NVM_DBG(
    this,
    "offset(" << offset << "), nbytes(" << nbytes << ")"
  );

  return Flush();
}

// Used by WritableFile
Status NvmFile::Close(void) {
  NVM_DBG(this, "ignoring...");

  return Status::OK();
}

// Deletes any buffers covering the range [offset; offset+length].
//
// Used by SequentialFile, RandomAccessFile, WritableFile
Status NvmFile::InvalidateCache(size_t offset, size_t length) {
  NVM_DBG(this, "");

  return Status::OK();
}

// Used by WritableFile
Status NvmFile::Append(const Slice& slice) {
  NVM_DBG(this, "");

  return PositionedAppend(slice, fsize_);
}

// Used by WritableFile
Status NvmFile::PositionedAppend(const Slice& slice, uint64_t offset) {
  NVM_DBG(this, "offset(" << offset << ")-aligned(" << !(offset % align_nbytes_) << ")");
  NVM_DBG(this, "fsize_(" << fsize_ << ")-aligned(" << !(fsize_ % align_nbytes_) << ")");
  NVM_DBG(this, "buf_nbytes_(" << buf_nbytes_ << ")-aligned(" << !(buf_nbytes_ % align_nbytes_) << ")");
  NVM_DBG(this, "slice-size(" << slice.size() << ")-aligned(" << !(slice.size() % align_nbytes_) << ")");

  if (offset < (fsize_ - buf_nbytes_)) {
    NVM_DBG(this, "ERR: offset(" << offset << ") <  fsize_(" << fsize_ << ") - buf_nbytes_(" << buf_nbytes_ << ")");
    return Status::IOError("Writing to flushed offset");
  }

  if (offset > fsize_) {
    NVM_DBG(this, "ERR: offset(" << offset << ") > fsize_(" << fsize_ << ")");
    return Status::IOError("Out of bounds");
  }

  const size_t data_nbytes = slice.size();
  const char* data = slice.data();

  size_t nbytes_remaining = data_nbytes;
  size_t nbytes_written = 0;

  buf_nbytes_ = offset - (fsize_ - buf_nbytes_);        // Reset buffer start
  fsize_ = offset;                                      // Reset file size

  NVM_DBG(this, "1. fsize_(" << fsize_ << "), buf_nbytes_(" << buf_nbytes_ << ")");

  while(nbytes_remaining > 0) {
    size_t avail = buf_nbytes_max_ - buf_nbytes_;
    size_t nbytes = std::min(nbytes_remaining, avail);

    memcpy(buf_ + buf_nbytes_, data + nbytes_written, nbytes);

    nbytes_remaining -= nbytes;
    nbytes_written += nbytes;

    buf_nbytes_ += nbytes;
    fsize_ += nbytes;

    NVM_DBG(this, "2. fsize_(" << fsize_ << "), buf_nbytes_(" << buf_nbytes_ << ")");

    if (nbytes_remaining && (!Flush().ok())) {
      return Status::IOError("Flushing to media failed.");
    }
  }
  NVM_DBG(this, "3. fsize_(" << fsize_ << "), buf_nbytes_(" << buf_nbytes_ << ")");

  return Status::OK();
}

Status NvmFile::wmeta(void) {
  NVM_DBG(this, "");

  std::string mpath = info_.fpath() + kNvmMetaExt;

  return env_->wmeta(mpath, fsize_, env_->store_->GetDevPath(), blks_);
}

// Used by WritableFile

Status NvmFile::Flush(void) {
  return Flush(true);
}

Status NvmFile::Flush(bool all_but_last) {
  NVM_DBG(this, "all_buf_last(" << all_but_last << ")");

  if (!buf_nbytes_) {
    NVM_DBG(this, "Nothing to flush (case 1)");
    return Status::OK();
  }

  if (all_but_last && buf_nbytes_ <= align_nbytes_) {
    NVM_DBG(this, "Nothing to flush (case 2)");
    return Status::OK();
  }

  if (fsize_ < buf_nbytes_) {
    NVM_DBG(this, "This should not happen!");
    return Status::IOError("This should not happen!");
  }

  size_t flush_tbytes = all_but_last ? buf_nbytes_ - align_nbytes_ : buf_nbytes_;

  NVM_DBG(this, "buf_nbytes_(" << buf_nbytes_ << "), flush_tbytes(" << flush_tbytes << ")");

  // TODO: Implement this properly
  size_t offset = fsize_ - buf_nbytes_;
  size_t blk_idx = offset / blk_nbytes_;

  if (!blks_[blk_idx]) {
    struct nvm_vblk *blk = env_->store_->get();

    if (!blk) {
      NVM_DBG(this, "failed allocating block blk_idx(" << blk_idx << ")");
      return Status::IOError("Failed reserving NVM (ENOMEM)");
    }

    blks_[blk_idx] = blk;
  }

  nvm_vblk_pr(blks_[blk_idx]);
  buf_nbytes_ -= flush_tbytes;

  if (nvm_vblk_write(blks_[blk_idx], buf_, flush_tbytes) < 0) {
    perror("What is this snitzel!?");
    NVM_DBG(this, "More snitzels");
    return Status::IOError("More snitzels");
  }

  /*
  size_t blk_idx = offset / blk_nbytes_;
  size_t blk_off = offset % blk_nbytes_;

  size_t nbufs = buffers_.size();
  if (nbufs && all_but_last) {
    --nbufs;
  }

  size_t blks_needed = (nbufs + blk_nbufs_ -1) / blk_nbufs_;
  NVM_DBG(this, "blks_needed(" << blks_needed << ")");

  // Ensure that have enough blocks reserved for flushing the file content
  for (size_t i = blks_.size(); i < blks_needed; ++i) {
    struct nvm_vblk *blk = env_->store_->get();

    if (!blk) {
      NVM_DBG(this, "failed allocating block i(" << i << ")");
      return Status::IOError("Failed reserving NVM (ENOMEM)");
    }

    blks_.push_back(blk);
  }

  // Write to media and free buffers
  for (size_t buf_idx = 0; buf_idx < nbufs; ++buf_idx) {
    if (!buffers_[buf_idx])
      continue;

    size_t blk_idx = buf_idx / blk_nbufs_;
    size_t blk_off = buf_idx % blk_nbufs_;

    for (size_t buf_pg_off = 0; buf_pg_off < buf_nvpgs_; ++buf_pg_off) {
      NVM_DBG(this, "buf_idx(" << buf_idx
              << "), blk_idx(" << blk_idx
              << "), blk_off(" << blk_off
              << "), buf_pg_off(" << buf_pg_off << ")");

      int nfail = 0;
      while(nfail < wretry_) {
        ssize_t err;
        err = nvm_sblk_write(
          blks_[blk_idx],
          buffers_[buf_idx] + (buf_pg_off * vpg_nbytes_),
          1
        );
        if (!err)
          break;

        ++nfail;
        NVM_DBG(this, "failed write, err(" << err << ")");
      }

      if (nfail == wretry_) {
        NVM_DBG(this, "failures (write) exceeded retry");
        return Status::IOError("failures (write) exceeded retry");
      }
    }

    free(buffers_[buf_idx]);    // Remove buffered data
    buffers_[buf_idx] = NULL;

    buf_nflushed_ = buf_idx+1;
  }
  */
  return wmeta();
}

// Used by WritableFile
Status NvmFile::Truncate(uint64_t size) {
  NVM_DBG(this, "fsize_(" << fsize_ << ")" <<
                ", size(" << size << ")" <<
                ", nvblocks(" << blks_.size() << ")");

  size_t blks_nreq = (size + blk_nbytes_ -1) / blk_nbytes_;

  // Release blocks that are no longer required
  for(size_t blk_idx = blks_nreq; blk_idx < blks_.size(); ++blk_idx) {
    NVM_DBG(this, "blk_idx(" << blk_idx << ")");

    if (!blks_[blk_idx]) {
      NVM_DBG(this, "nothing here... skipping...");
      continue;
    }

    env_->store_->put(blks_[blk_idx]);
    blks_[blk_idx] = NULL;
  }

  fsize_ = size;

  // TODO: Adjust buffer accordingly

  /*
  size_t bufs_required = (size + align_nbytes_ - 1) / align_nbytes_;

  // De-allocate buffers that are no longer required
  for(size_t buf_idx = bufs_required; buf_idx < buffers_.size(); ++buf_idx) {
    NVM_DBG(this, "buf_idx(" << buf_idx << ")");
    free(buffers_[buf_idx]);
    buffers_[buf_idx] = NULL;
  }

  // Release blocks that are no longer required
  for(size_t blk_idx = blks_required; blk_idx < blks_.size(); ++blk_idx) {
    NVM_DBG(this, "blk_idx(" << blk_idx << ")");

    if (!blks_[blk_idx]) {
      NVM_DBG(this, "nothing here... skipping...");
      continue;
    }

    env_->store_->put(blks_[blk_idx]);
    blks_[blk_idx] = NULL;
  }

  */
  return wmeta();
}

Status NvmFile::pad_last_block(void) {
  NVM_DBG(this, "...");

  Status s = Flush(false);      // Flush out...
  if (!s.ok()) {
    NVM_DBG(this, "padding: Schnitzels hit the fan...");
    return s;
  }

  if (!fsize_) {
    NVM_DBG(this, "padding: empty file, skipping...");
    return Status::OK();
  }

  const size_t blk_idx = fsize_ / blk_nbytes_;

  if (!blks_[blk_idx]) {
    return Status::IOError("FAILED: No vblk to pad!?");
  }

  ssize_t err = nvm_vblk_pad(blks_[blk_idx]);
  if (err < 0)
    return Status::IOError("FAILED: nvm_vblk_pad(...)");

  return Status::OK();
}

// Used by SequentialFile, RandomAccessFile
Status NvmFile::Read(
  uint64_t offset, size_t n, Slice* result, char* scratch
) const {
  NVM_DBG(this, "offset(" << offset << "), n(" << n << "), fsize(" << fsize_ << ")");

  // n is the MAX number of bytes to read, since it is the since of the scratch
  // memory. However, there might be n, less than n, or more than n bytes in the
  // file starting from offset.  So we need to know how many bytes we actually
  // need to read..
  const uint64_t nbytes_from_offset = fsize_ - std::min(fsize_, offset);
  if (n > nbytes_from_offset) {
    n = nbytes_from_offset;
  }
  if (n == 0) {
    NVM_DBG(this, "no buffers...");
    *result = Slice();
    return Status::OK();
  }
  // Now we know that: '0 < n <= nbytes_from_offset'

  /*
  // TODO: Compute blk and offset within block when reading from multiple blocks
  uint64_t blk_idx = offset / blk_nbytes_;
  uint64_t blk_offset = offset % blk_nbytes_;

  ssize_t ret = nvm_sblk_read(blks_[blk_id], scratch, n, blk_offset);

  if (ret < 0) {
    NVM_DBG(this, "FAILED: nvm_sblk_read");
    return Status::IOError("FAILED: nvm_sblk_read");
  }

  *result = Slice(scratch, ret);
  */

  return Status::OK();
}

}       // namespace rocksdb

