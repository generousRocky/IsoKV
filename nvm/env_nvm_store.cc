#include "env_nvm.h"
#include <exception>

namespace rocksdb {

NvmStore::NvmStore(
  EnvNVM* env,
  const std::string& dev_name,
  const std::string& mpath,
  size_t rate
) : env_(env), dev_name_(dev_name), mpath_(mpath), rate_(rate), curs_(0) {

  std::deque<uint64_t> meta_ppas, meta_skips;

  Status s = env_->posix_->FileExists(mpath);   // Load from meta
  if (s.ok()) {
    s = env_->rmeta(mpath, curs_, dev_name_, meta_ppas, meta_skips);
  }

  dev_ = nvm_dev_open(dev_name_.c_str());       // Open device
  if (!dev_) {
    NVM_DBG(this, "too bad.");
    throw std::runtime_error("NvmStore: failed opening device");
  }
  geo_ = nvm_dev_attr_geo(dev_);                // Grab geometry

  for (auto ppa : meta_ppas) {                  // Populate reservations
    reserved_.push_back(nvm_vblock_new_on_dev(dev_, ppa));
  }

  for (auto skip : meta_skips) {                // Check skips
    if (skip) {
      NVM_DBG(this, "skip(" << skip << ")");
      throw std::runtime_error("Got skip in store meta");
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
  }

  NVM_VBLOCK blk = reserved_.front();
  reserved_.pop_front();

  wmeta();

  return blk;
}

void NvmStore::put(NVM_VBLOCK blk) {
  NVM_DBG(this, "");

  //nvm_vblock_put(blk);
  nvm_vblock_free(&blk);
}

Status NvmStore::reserve(void) {
  NVM_DBG(this, "");

  while(reserved_.size() < rate_) {
    NVM_VBLOCK blk;
    NVM_ADDR addr;
    ssize_t err;

    addr.g.ch = 0;
    addr.g.lun = 0;
    addr.g.pl = 0;
    addr.g.blk = curs_ % geo_.nblocks;
    addr.g.pg = 0;
    addr.g.sec = 0;

    //addr.g.lun = (curs_ % geo_.nluns);
    //addr.g.blk = (curs_ / geo_.nluns) % geo_.nblocks;

    blk = nvm_vblock_new_on_dev(dev_, addr.ppa);
    if (!blk) {
      NVM_DBG(this, "Failed allocating vblock (ENOMEM)");
      return Status::IOError("Failed allocating vblock (ENOMEM)");
    }

    err = nvm_vblock_erase(blk);
    if (err) {
      NVM_DBG(this, "Failed nvm_vblock_erase err(" << err << ")");
      return Status::IOError("Failed erasing vblock");
    }

    /*
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
    */

    reserved_.push_back(blk);
    ++curs_;
  }

  return Status::OK();
}

Status NvmStore::release(void) {
  NVM_DBG(this, "");

  while(!reserved_.empty()) {
    NVM_VBLOCK blk = reserved_.back();
    //nvm_vblock_put(blk);
    reserved_.pop_back();
    nvm_vblock_free(&blk);
  }

  return Status::OK();
}

Status NvmStore::wmeta(void) {
  NVM_DBG(this, "");

  return env_->wmeta(mpath_, curs_, dev_name_, reserved_);
}

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
