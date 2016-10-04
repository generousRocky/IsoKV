#include "env_nvm.h"
#include <exception>

namespace rocksdb {

NvmStore::NvmStore(
  EnvNVM* env,
  const std::string& dev_name,
  const std::string& mpath,
  size_t rate
) : env_(env), dev_name_(dev_name), mpath_(mpath), rate_(rate), curs_(0) {

  dev_ = nvm_dev_open(dev_name_.c_str());       // Open device
  if (!dev_) {
    NVM_DBG(this, "too bad.");
    throw std::runtime_error("NvmStore: failed opening device");
  }
  geo_ = nvm_dev_attr_geo(dev_);                // Grab geometry

  Status s = env_->posix_->FileExists(mpath);   // Load curs_ and reservations_
  if (s.ok()) {
    std::ifstream meta(mpath);
    std::string foo;
    size_t ppa;
    if (!(meta >> curs_)) {
      NVM_DBG(this, "shucks...");
    }
    if (!(meta >> foo)) {
      NVM_DBG(this, "shucks...");
    }
    while(meta >> ppa) {
      reserved_.push_back(nvm_vblock_new_on_dev(dev_, ppa));
    }
  }
}

NvmStore::~NvmStore(void) {
  NVM_DBG(this, "");
  Status s;

  nvm_dev_close(dev_);

  s = wmeta();
  if (!s.ok()) {
    NVM_DBG(this, "writing meta failed.");
  }
}

NVM_VBLOCK NvmStore::get(void) {
  NVM_DBG(this, "");
  MutexLock lock(&mutex_);

  if (reserved_.empty()) {
    reserve();
    wmeta();
  }

  NVM_VBLOCK blk = reserved_.front();
  reserved_.pop_front();

  return blk;
}

void NvmStore::put(NVM_VBLOCK blk) {
  NVM_DBG(this, "");

  nvm_vblock_put(blk);
  nvm_vblock_free(&blk);
}

Status NvmStore::reserve(void) {
  NVM_DBG(this, "");

  while(reserved_.size() < rate_) {
    NVM_VBLOCK blk;

    blk = nvm_vblock_new();
    if (!blk) {
      NVM_DBG(this, "Failed allocating vblock (ENOMEM)");
      return Status::IOError("Failed allocating vblock (ENOMEM)");
    }

    const size_t ch = curs_ % geo_.nchannels;
    const size_t ln = curs_ % geo_.nluns;

    if (nvm_vblock_gets(blk, dev_, ch, ln)) {
      NVM_DBG(this, "Failed _gets: ch(" << ch << "), ln(" << ln << ")");
      nvm_vblock_free(&blk);
      return Status::IOError("Failed nvm_vblock_gets");
    }

    reserved_.push_back(blk);
    ++curs_;
  }

  return Status::OK();
}

Status NvmStore::release(void) {
  NVM_DBG(this, "");

  while(!reserved_.empty()) {
    NVM_VBLOCK blk = reserved_.back();
    nvm_vblock_put(blk);
    reserved_.pop_back();
    nvm_vblock_free(&blk);
  }

  return Status::OK();
}

Status NvmStore::wmeta(void) {
  NVM_DBG(this, "");

  unique_ptr<WritableFile> fmeta;

  Status s = env_->posix_->NewWritableFile(mpath_, &fmeta, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  std::string meta("");
  meta += std::to_string(curs_) + "\n";
  meta += std::string("---\n");
  for (auto blk : reserved_) {
    meta += std::to_string(nvm_vblock_attr_ppa(blk)) + "\n";
  }

  Slice slice(meta.c_str(), meta.size());
  s = fmeta->Append(slice);
  if (!s.ok()) {
    NVM_DBG(this, "meta append failed s(" << s.ToString() << ")");
  }

  s = fmeta->Flush();
  if (!s.ok()) {
    NVM_DBG(this, "meta flush failed s(" << s.ToString() << ")");
  }

  fmeta.reset(nullptr);

  return s;
}

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
