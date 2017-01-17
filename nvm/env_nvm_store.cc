#include "env_nvm.h"
#include <exception>

namespace rocksdb {

NvmStore::NvmStore(
  EnvNVM* env,
  const std::string& dev_name,
  const std::string& mpath,
  size_t rate
) : env_(env), dev_name_(dev_name), dev_path_("/dev/"+dev_name), mpath_(mpath),
    rate_(rate), curs_(0) {

  std::deque<std::vector<struct nvm_addr>> vblks;
  std::string meta_dev_path;

  Status s = env_->posix_->FileExists(mpath);           // Reservations file
  if (s.ok()) {
    s = env_->rmeta(mpath, curs_, meta_dev_path, vblks);

    if (meta_dev_path.compare(dev_path_)) {
      NVM_DBG(this, "NvmStore: dev_path != meta_dev_path");
      throw std::runtime_error("NvmStore: dev_path != meta_dev_path");
    }
  }

  dev_ = nvm_dev_open(dev_path_.c_str());       // Open device
  if (!dev_) {
    NVM_DBG(this, "NvmStore: failed opening device");
    throw std::runtime_error("NvmStore: failed opening device");
  }
  geo_ = nvm_dev_get_geo(dev_);                         // Grab geometry

  std::cout << "ss" << std::endl;

  for (size_t i = 0; i < vblks.size(); ++i) {           // Populate reservations
    struct nvm_vblk *vblk;
    std::vector<struct nvm_addr>& addrs = vblks[i];

    vblk = nvm_vblk_alloc(dev_, &addrs[0], addrs.size());
    if (!vblk) {
      NVM_DBG(this, "NvmStore: failed allocating `vblk`");
      throw std::runtime_error("NvmStore: failed allocating `vblk`");
    }
    reserved_.push_back(vblk);
  }

  if (reserved_.empty()) {
    reserve();
    wmeta();
  }
}

NvmStore::~NvmStore(void) {
  NVM_DBG(this, "");
  Status s;

  s = wmeta();
  if (!s.ok()) {
    NVM_DBG(this, "writing meta failed.");
  }

  nvm_dev_close(dev_);
}

struct nvm_vblk* NvmStore::get(void) {
  NVM_DBG(this, "");
  MutexLock lock(&mutex_);

  if (reserved_.empty()) {
    reserve();
  }
  struct nvm_vblk* blk = reserved_.front();
  reserved_.pop_front();

  wmeta();

  return blk;
}

void NvmStore::put(struct nvm_vblk* blk) {
  NVM_DBG(this, "");

  nvm_vblk_free(blk);
}

Status NvmStore::reserve(void) {
  NVM_DBG(this, "");

  while(reserved_.size() < rate_) {
    struct nvm_vblk* blk;
    int blk_idx;
    ssize_t err;

    if (curs_ >= geo_->nblocks) {
      return Status::IOError("No more vblks available");
    }

    blk_idx = curs_++ % geo_->nblocks;

    blk = nvm_vblk_alloc_line(
      dev_, 0, geo_->nchannels-1, 0, geo_->nluns-1, blk_idx
    );
    if (!blk) {
      NVM_DBG(this, "Failed allocating vblk (ENOMEM)");
      return Status::IOError("Failed allocating vblk (ENOMEM)");
    }

    err = nvm_vblk_erase(blk);
    if (err < 0) {
      NVM_DBG(this, "Failed nvm_vblk_erase err(" << err << ")");
      continue;
    }

    reserved_.push_back(blk);
  }

  return Status::OK();
}

Status NvmStore::release(void) {
  NVM_DBG(this, "");

  while(!reserved_.empty()) {
    struct nvm_vblk* blk = reserved_.back();
    reserved_.pop_back();
    nvm_vblk_free(blk);
  }

  return Status::OK();
}

Status NvmStore::wmeta(void) {
  NVM_DBG(this, "");

  return env_->wmeta(mpath_, curs_, dev_path_, reserved_);
}

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
