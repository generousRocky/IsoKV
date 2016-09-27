#include <iostream>
#include <fstream>
#include <csignal>

#include "util/coding.h"
#include "env_nvm.h"

int count = 0;

namespace rocksdb {

static size_t kNvmVBlockSize = 16777216;
static size_t kNvmVPageSize = 16384;
static size_t kNvmAlign = kNvmVPageSize;

static std::string kNvmMetaExt = ".meta";

NVMFile::NVMFile(
  EnvNVM* env,
  const FPathInfo& info,
  bool from_meta
) : env_(env), buffers_(), refs_(), info_(info), fsize_(), ppas_() {
  NVM_DBG(this, "");

  if (from_meta) {      // Reconstruct NVMFile from .meta file
    std::string mpath = info.fpath() + kNvmMetaExt;

    std::ifstream meta(mpath);
    uint64_t fsize = 0, nppas = 0;
    if (!(meta >> fsize)) {
      NVM_DBG(this, "shucks...");
    }
    if (!(meta >> nppas)) {
      NVM_DBG(this, "shucks...");
    }

    fsize_ = fsize;
    uint64_t ppa = 0;
    while(meta >> ppa) {
      ppas_.push_back(ppa);
    }
  }

  NVM_DBG(this, "");
}

NVMFile::~NVMFile(void) {
  NVM_DBG(this, "");

  for (auto &buf : buffers_) {
    if (buf) {
      delete [] buf;
    }
  }
}

bool NVMFile::IsNamed(const std::string& fname) const {
  NVM_DBG(this, "fname(" << fname << ")");

  return !info_.fname().compare(fname);
}

const std::string& NVMFile::GetFname(void) const {
  NVM_DBG(this, "return(" << info_.fname() << ")");

  return info_.fname();
}

const std::string& NVMFile::GetDpath(void) const {
  NVM_DBG(this, "return(" << info_.dpath() << ")");

  return info_.dpath();
}

void NVMFile::Rename(const std::string& fname) {
  NVM_DBG(this, "fname(" << fname << ")");

  info_.fname(fname);
}

size_t NVMFile::GetRequiredBufferAlignment(void) const {
  NVM_DBG(this, "hard-coded return(kNvmAlign)");

  return kNvmAlign;
}

void NVMFile::Ref(void) {
  MutexLock lock(&refs_mutex_);

  NVM_DBG(this, "refs_(" << refs_ << ")");

  ++refs_;

  NVM_DBG(this, "refs_(" << refs_ << ")");
}

void NVMFile::Unref(void) {
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

std::string NVMFile::txt(void) const {
  std::stringstream ss;
  ss << "fname(" << info_.fname() << ") ";
  return ss.str();
}

// Used by WritableFile
bool NVMFile::UseDirectIO(void) const {
  NVM_DBG(this, "hard-coded return(true)");

  return true;
}

// Used by WritableFile
bool NVMFile::UseOSBuffer(void) const {
  NVM_DBG(this, "hard-coded return(false)");

  return false;
}

// Used by WritableFile
bool NVMFile::IsSyncThreadSafe(void) const {
  NVM_DBG(this, "hard-coded return(false)");

  return false;
}

//
// Implemented using the address of the NVMFile instance. This seem to satisfy
// the requirements: it lives as long as need be, is unique and probably won't
// have the same address after termination.
//
// Although... depending on what is meant by prefix... they probably could be.
//
//
// Used by RandomAccessFile, WritableFile
size_t NVMFile::GetUniqueId(char* id, size_t max_size) const {
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
uint64_t NVMFile::GetFileSize(void) const {
  NVM_DBG(this, "return(" << fsize_ << ")");

  return fsize_;
}

// Used by WritableFile
void NVMFile::PrepareWrite(size_t offset, size_t len) {
  NVM_DBG(this, "offset(" << offset << "), len(" << len << ") ignoring...");
}

// Used by WritableFile
Status NVMFile::Allocate(uint64_t offset, uint64_t len) {
  NVM_DBG(this, "offset(" << offset << "), len(" << len << ") ignoring");

  return Status::OK();
}

// Used by WritableFile
Status NVMFile::Append(const Slice& data) {
  NVM_DBG(this, "forwarding");

  return PositionedAppend(data, GetFileSize());
}

// Used by WritableFile
Status NVMFile::Sync(void) {
  NVM_DBG(this, "writing file meta to default env...");

  return Flush();
}

// Used by WritableFile
Status NVMFile::Fsync(void) {
  NVM_DBG(this, "writing file meta to default env...");

  return Flush();
}

// Used by WritableFile
Status NVMFile::RangeSync(uint64_t offset, uint64_t nbytes) {
  NVM_DBG(
    this,
    "offset(" << offset << "), nbytes(" << nbytes << ")"
  );

  return Flush();
}

// Used by WritableFile
Status NVMFile::Close(void) {
  NVM_DBG(this, "ignoring...");

  return Status::OK();
}

// Deletes any buffers covering the range [offset; offset+length].
//
// Used by SequentialFile, RandomAccessFile, WritableFile
Status NVMFile::InvalidateCache(size_t offset, size_t length) {

  size_t first = offset / kNvmVPageSize;
  size_t count = length / kNvmVPageSize;

  for (size_t idx = first; idx < (first+count); ++idx) {
    if (buffers_[idx]) {
      delete [] buffers_[idx];
      buffers_[idx] = NULL;
    }
  }

  return Status::OK();
}

// Used by WritableFile
Status NVMFile::PositionedAppend(const Slice& slice, uint64_t offset) {
  NVM_DBG(this, "offset(" << offset << "), slice-size(" << slice.size() << ")");

  const char* data = slice.data();
  const size_t data_nbytes = slice.size();

  size_t nbufs = (offset + data_nbytes) / kNvmVPageSize;
  for (size_t i = buffers_.size(); i < nbufs; ++i) {
    buffers_.push_back(new char[kNvmVPageSize]);
  }

  // Translate offset to buffer and offset within buffer
  size_t buf_idx = offset / kNvmVPageSize;
  size_t buf_offset = offset % kNvmVPageSize;

  size_t nbytes_remaining = data_nbytes;
  size_t nbytes_written = 0;
  while(nbytes_remaining > 0) {
    size_t avail = kNvmVPageSize - buf_offset;
    size_t nbytes = std::min(nbytes_remaining, avail);

    memcpy(buffers_[buf_idx] + buf_offset, data + nbytes_written, nbytes);

    nbytes_remaining -= nbytes;
    nbytes_written += nbytes;
    ++buf_idx;
    buf_offset = 0;
  }

  NVM_DBG(this, "nbytes_written(" << nbytes_written << "), data_nbytes(" << data_nbytes << ")");
  fsize_ += nbytes_written;

  return Status::OK();
}

Status NVMFile::wmeta(void) const {
  unique_ptr<WritableFile> fmeta;

  Status s = env_->posix_->NewWritableFile(
    info_.fpath() + kNvmMetaExt, &fmeta, env_options_
  );
  if (!s.ok()) {
    return s;
  }

  std::string meta("");
  meta += std::to_string(fsize_) + "\n";
  meta += std::to_string(ppas_.size()) + "\n";
  for (auto ppa : ppas_) {
    meta += std::to_string(ppa) + "\n";
  }

  Slice slice(meta.c_str(), meta.size());
  s = fmeta->Append(slice);
  if (!s.ok()) {
    NVM_DBG(this, "s(" << s.ToString() << ")");
  }

  s = fmeta->Flush();
  if (!s.ok()) {
    NVM_DBG(this, "s(" << s.ToString() << ")");
  }

  fmeta.reset(nullptr);

  return s;
}

// Used by WritableFile
Status NVMFile::Flush(void) {
  NVM_DBG(this, "flushing to media...");

  size_t huh = 0;
  for (auto &buf : buffers_) {
    ++huh;
    if (!buf) {
      continue;
    }
    NVM_DBG(this, "w00p(" << (void*)buf << "), huh(" << huh << ")");

    // TODO: Persist the buffer and de-allocate it
    //delete [] buf;
    //buf = NULL;
  }

  return wmeta();
}

// Used by WritableFile
Status NVMFile::Truncate(uint64_t size) {
  NVM_DBG(this, "size(" << size << ")");

  size_t needed = size / kNvmVPageSize;

  // De-allocate unused buffers
  for(size_t i = needed+1; i < buffers_.size(); ++i) {
    NVM_DBG(this, "i(" << i << ")");
    delete [] buffers_[i];
    buffers_[i] = NULL;
  }

  // TODO: Release allocated NVM

  fsize_ = size;

  return wmeta();
}

// Used by SequentialFile, RandomAccessFile
Status NVMFile::Read(
  uint64_t offset, size_t n, Slice* result, char* scratch
) const {
  NVM_DBG(this, "offset(" << offset << "), n(" << n << "), fsize(" << fsize_ << ")");

  const uint64_t available = fsize_ - std::min(fsize_, offset);
  if (n > available) {
    n = available;
  }
  if (n == 0) {
    *result = Slice();
    return Status::OK();
  }

  size_t buf = offset / kNvmVPageSize;
  size_t buf_offset = offset % kNvmVPageSize;

  if (n <= kNvmVPageSize - buf_offset) { // All within a single block
    *result = Slice(buffers_[buf] + buf_offset, n);
    return Status::OK();
  }

  size_t bytes_to_copy = n;
  char* dst = scratch;

  while (bytes_to_copy > 0) {
    size_t avail = kNvmVPageSize - buf_offset;
    if (avail > bytes_to_copy) {
      avail = bytes_to_copy;
    }
    memcpy(dst, buffers_[buf] + buf_offset, avail);

    bytes_to_copy -= avail;
    dst += avail;
    buf++;
    buf_offset = 0;
  }

  *result = Slice(scratch, n);
  return Status::OK();
}

}       // namespace rocksdb

