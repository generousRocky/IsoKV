#include <iostream>
#include <csignal>

#include "util/coding.h"
#include "env_nvm.h"

int count = 0;

namespace rocksdb {

NVMFile::NVMFile(
  void
) : env_(NULL), dpath_(), fname_(), fsize_(0),
    buf_(NULL), buf_len_(0), ppas_(), refs_(0) {
  NVM_TRACE(this, "");
}

NVMFile::NVMFile(
  EnvNVM* env, const std::string& dpath, const std::string& fname
) : env_(env), dpath_(dpath), fname_(fname), fsize_(0),
    buf_(NULL), buf_len_(0), ppas_(), refs_(0) {
  NVM_TRACE(this, "");
}

NVMFile::~NVMFile(void) {
  NVM_TRACE(this, "");

  free(buf_);
}

bool NVMFile::UseDirectIO(void) const {
  NVM_TRACE(this, "hard-coded return(true)");

  return true;
}

bool NVMFile::UseOSBuffer(void) const {
  NVM_TRACE(this, "hard-coded return(false)");

  return false;
}

Status NVMFile::Read(
  uint64_t offset, size_t n, Slice* result, char* scratch
) const {
  NVM_TRACE(this, "offset(" << offset << ")");

  if (offset >= fsize_)
    return Status::IOError("Out of bounds!");

  const uint64_t nbytes_remaining = fsize_ - offset;

  if (!nbytes_remaining) {              // End of file
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  const uint64_t nbytes_toread = std::min(nbytes_remaining, n);

  memcpy(scratch, buf_ + offset, nbytes_toread);
  *result = Slice(scratch, nbytes_toread);

  return Status::OK();
}

void NVMFile::PrepareWrite(size_t offset, size_t len) {
  NVM_TRACE(this, "forwarding");

  Allocate(offset, len);
}

Status NVMFile::Allocate(uint64_t offset, uint64_t len) {
  NVM_TRACE(this, "offset(" << offset << "), len(" << len << ")");

  uint64_t new_len = offset + len;

  if (new_len > buf_len_) {     // Grow buf in chunks of 4K
    uint64_t min_len = ((new_len / 4096) + 1) * 4096;

    char *new_buf = (char*)realloc((void*)buf_, min_len);
    if (!new_buf) {
      return Status::IOError("NVMFile::Allocate -- ENOMEM");
    }

    buf_ = new_buf;
    buf_len_ = min_len;
  }

  return Status::OK();
}

Status NVMFile::PositionedAppend(const Slice& data, uint64_t offset) {
  NVM_TRACE(this, "offset(" << offset << "), data-size(" << data.size() << ")");

  const uint64_t data_size = data.size();
  const uint64_t remaining = fsize_ - offset;
  uint64_t grow = remaining < data_size ? data_size - remaining : 0;

  if (!Allocate(offset, data_size).ok()) {
    return Status::IOError("Exceeding capacity");
  }

  memcpy((void*)(buf_ + offset), (void*)data.data(), data_size);

  fsize_ += grow;

  return Status::OK();
}

Status NVMFile::Append(const Slice& data) {
  NVM_TRACE(this, "forwarding");

  return PositionedAppend(data, GetFileSize());
}

Status NVMFile::Truncate(uint64_t size) {
  NVM_TRACE(this, "size(" << size << ")");

  fsize_ = size;

  return Status::OK();
}

Status NVMFile::Close(void) {
  NVM_TRACE(this, "ignoring...");

  return Status::OK();
}

Status NVMFile::Flush(void) {
  NVM_TRACE(this, "ignoring...");

  return Status::OK();
}

Status NVMFile::Sync(void) {
  NVM_TRACE(this, "ignoring...");

  return Status::OK();
}

Status NVMFile::Fsync(void) {
  NVM_TRACE(this, "ignoring...");

  return Status::OK();
}

bool NVMFile::IsSyncThreadSafe(void) const {
  NVM_TRACE(this, "hard-coded return(false)");

  return false;
}

//
// Implemented using the address of the NVMFile instance. This seem to satisfy
// the requirements: it lives as long as need be, is unique and probably won't
// have the same address after termination.
//
// Although... depending on what is meant by prefix... they probably could be.
//
size_t NVMFile::GetUniqueId(char* id, size_t max_size) const {
  NVM_TRACE(this, "");

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

uint64_t NVMFile::GetFileSize(void) const {
  NVM_TRACE(this, "return(" << fsize_ << ")");

  return fsize_;
}

uint64_t NVMFile::GetFileSize(void) {
  NVM_TRACE(this, "return(" << fsize_ << ")");

  return fsize_;
}

Status NVMFile::InvalidateCache(size_t offset, size_t length) {
  NVM_TRACE(
    this,
    "ignoring... offset(" << offset << "), length(" << length << ")"
  );

  return Status::OK();
}

Status NVMFile::RangeSync(uint64_t offset, uint64_t nbytes) {
  NVM_TRACE(
    this,
    "ignoring... offset(" << offset << "), nbytes(" << nbytes << ")"
  );

  return Status::OK();
}

void NVMFile::Rename(const std::string& fname) {
  NVM_TRACE(this, "fname(" << fname << ")");

  fname_ = fname;
}

bool NVMFile::IsNamed(const std::string& fname) const {
  NVM_TRACE(this, "fname(" << fname << ")");

  return !fname_.compare(fname);
}

const std::string& NVMFile::GetFname(void) const {
  NVM_TRACE(this, "return(" << fname_ << ")");

  return fname_;
}

const std::string& NVMFile::GetDpath(void) const {
  NVM_TRACE(this, "return(" << dpath_ << ")");

  return dpath_;
}

size_t NVMFile::GetRequiredBufferAlignment(void) const {
  NVM_TRACE(this, "hard-coded return(4096)");

  return 4096*4;  // TODO: Get this from liblightnvm device geometry
}

void NVMFile::Ref(void) {
  MutexLock lock(&refs_mutex_);

  NVM_TRACE(this, "refs_(" << refs_ << ")");

  ++refs_;

  NVM_TRACE(this, "refs_(" << refs_ << ")");
}

void NVMFile::Unref(void) {
  NVM_TRACE(this, "");

  bool do_delete = false;

  {
    MutexLock lock(&refs_mutex_);
    NVM_TRACE(this, "refs_(" << refs_ << ")");
    --refs_;
    NVM_TRACE(this, "refs_(" << refs_ << ")");
    if (refs_ < 0) {
      do_delete = true;
    }
  }

  if (do_delete) {
    delete this;
  }
}

std::string NVMFile::txt(void) {
  std::stringstream ss;
  ss << "fname_(" << fname_ << ") ";
  return ss.str();
}

std::string NVMFile::txt(void) const {
  std::stringstream ss;
  ss << "fname_(" << fname_ << ") ";
  return ss.str();
}

}       // namespace rocksdb

