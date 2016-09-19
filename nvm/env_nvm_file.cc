#include <iostream>
#include <csignal>
#include "env_nvm.h"

int count = 0;

namespace rocksdb {

NVMFile::NVMFile(
  void
) : env_(NULL), dname_(), fname_(), fsize_(0),
    buf_(NULL), buf_len_(0), refs_(0) {
  NVM_DEBUG("env_(%p), dname_(%s), fname_(%s)",
            env_, dname_.c_str(), fname_.c_str());
  NVM_DEBUG("fsize_(%lu), buf_(%p), buf_len_(%lu), refs_(%d)\n",
            fsize_, buf_, buf_len_, refs_);
}

NVMFile::NVMFile(
  EnvNVM* env, const std::string& dname, const std::string& fname
) : env_(env), dname_(dname), fname_(fname), fsize_(0),
    buf_(NULL), buf_len_(0), refs_(0) {
  NVM_DEBUG("env_(%p), dname_(%s), fname_(%s)",
            env_, dname_.c_str(), fname_.c_str());
  NVM_DEBUG("fsize_(%lu), buf_(%p), buf_len_(%lu), refs_(%d)\n",
            fsize_, buf_, buf_len_, refs_);
}

NVMFile::~NVMFile(void) {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);
}

bool NVMFile::UseDirectIO(void) const {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return true;
}

bool NVMFile::UseOSBuffer(void) const {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return false;
}

Status NVMFile::Close(void) {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return Status::OK();
}

Status NVMFile::Read(
  uint64_t offset, size_t n, Slice* result, char* scratch
) const {
  NVM_DEBUG("fname(%s), file(%p), offset(%lu), n(%lu)\n",
            fname_.c_str(), this, offset, n);

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

  NVM_DEBUG("fname(%s) - read %lu, offset(%lu)\n",
            GetName().c_str(), nbytes_toread, offset);
  return Status::OK();
}

void NVMFile::PrepareWrite(size_t offset, size_t len) {
  NVM_DEBUG("fname(%s), file(%p), offset(%lu), len(%lu)\n",
            fname_.c_str(), this, offset, len);

  Allocate(offset, len);
}

Status NVMFile::Allocate(uint64_t offset, uint64_t len) {
  NVM_DEBUG("fname(%s), file(%p), offset(%lu), len(%lu)\n",
            fname_.c_str(), this, offset, len);

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

  NVM_DEBUG("buf_(%p), buf_len_(%lu)\n", buf_, buf_len_);

  return Status::OK();
}

Status NVMFile::PositionedAppend(const Slice& data, uint64_t offset)
{
  NVM_DEBUG("fname(%s), data(%p), data-size(%lu), offset(%lu)\n",
            fname_.c_str(), &data, data.size(), offset);

  const uint64_t data_size = data.size();
  const uint64_t remaining = fsize_ - offset;
  uint64_t grow = remaining < data_size ? data_size - remaining : 0;

  if (!Allocate(offset, data_size).ok()) {
    return Status::IOError("Exceeding capacity");
  }

  memcpy((void*)(buf_ + offset), (void*)data.data(), data_size);

  NVM_DEBUG("fsize_(%lu)\n", fsize_);
  NVM_DEBUG("grow(%lu)\n", grow);

  fsize_ += grow;
  NVM_DEBUG("fsize_(%lu)\n", fsize_);

  if (fname_.find("OPTIONS") != std::string::npos) {
    NVM_DEBUG("AFTER WRITE\n");
    NVM_DEBUG("data(%s)\n", buf_);
    NVM_DEBUG("AFTER WRITE\n");
  }

  return Status::OK();
}

Status NVMFile::Append(const Slice& data) {
  NVM_DEBUG("data(%p), data-size(%lu), fsize_(%lu)\n",
            &data, data.size(), fsize_);

  return PositionedAppend(data, GetFileSize());
}

Status NVMFile::Truncate(uint64_t size)
{
  NVM_DEBUG("fname(%s), file(%p) : fsize_(%lu), size(%lu)\n",
            fname_.c_str(), this, fsize_, size);

  fsize_ = size;

  return Status::OK();
}

Status NVMFile::Flush(void) {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return Status::OK();
}

Status NVMFile::Sync(void) {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return Status::OK();
}

Status NVMFile::Fsync(void) {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return Status::OK();
}

bool NVMFile::IsSyncThreadSafe(void) const {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);

  return false;
}

size_t NVMFile::GetUniqueId(char* id, size_t max_size) const {
  NVM_DEBUG("fname(%s), file(%p)\n", fname_.c_str(), this);
  NVM_DEBUG("id(%s), max_size(%lu\n", id, max_size);

  return 0;
}

uint64_t NVMFile::GetFileSize(void) const {
  NVM_DEBUG("fname(%s), file(%p), fsize_(%lu)\n", fname_.c_str(), this, fsize_);

  return fsize_;
}

uint64_t NVMFile::GetFileSize(void) {
  NVM_DEBUG("fname(%s), file(%p), fsize_(%lu)\n", fname_.c_str(), this, fsize_);

  return fsize_;
}

Status NVMFile::InvalidateCache(size_t offset, size_t length) {
  NVM_DEBUG("offset(%lu), length(%lu)\n", offset, length);

  return Status::OK();
}

Status NVMFile::RangeSync(uint64_t offset, uint64_t nbytes) {
  NVM_DEBUG("offset(%lu), nbytes(%lu)\n", offset, nbytes);

  return Status::OK();
}

void NVMFile::Rename(const std::string& fname) {
  NVM_DEBUG("'%s' -> '%s'\n", fname_.c_str(), fname.c_str());

  fname_ = fname;
}

bool NVMFile::IsNamed(const std::string& fname) const {
  return !fname_.compare(fname);
}

const std::string& NVMFile::GetName(void) const {
  return fname_;
}

const std::string& NVMFile::GetDir(void) const {
  return dname_;
}

size_t NVMFile::GetRequiredBufferAlignment(void) const {
  NVM_DEBUG("\n");

  return 4096;  // TODO: Get this from liblightnvm device geometry
}

void NVMFile::Ref(void) {
  ++refs_;
  NVM_DEBUG("post state: refs_(%d).\n", refs_);
}

void NVMFile::Unref(void) {
  --refs_;
  NVM_DEBUG("post state: refs_(%d).\n", refs_);
}


}       // namespace rocksdb

