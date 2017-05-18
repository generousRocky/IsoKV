#include "env_nvm.h"
#include <exception>

namespace rocksdb {

//
// Current implementation:
//
//  * Meta is assumed represent the entire drive and in lines, thus
//    only state is persisted
//  * Persists nvm.meta ONLY on shutdown
//  * Recovers only from nvm.meta
//  * Erases upon get
//
NvmStore::NvmStore(
  EnvNVM* env,
  const std::string& dev_name,
  const std::string& mpath,
  size_t rate
) : env_(env), dev_name_(dev_name), dev_path_("/dev/"+dev_name), mpath_(mpath),
    rate_(rate), curs_(0) {

  dev_ = nvm_dev_open(dev_path_.c_str());               // Open device
  if (!dev_) {
    NVM_DBG(this, "FAILED: opening device");
    throw std::runtime_error("FAILED: opening device");
  }
  geo_ = nvm_dev_get_geo(dev_);                         // Grab geometry

  Status s = recover(mpath);                            // Initialize blks

  if (!s.ok()) {
    NVM_DBG(this, "FAILED: recover");
    throw std::runtime_error("FAILED: recover");
  }
}

Status NvmStore::recover(const std::string& mpath)
{
  NVM_DBG(this, "mpath(" << mpath << ")");
  MutexLock lock(&mutex_);

  // Initialize with default state (kFree)
  for (size_t blk_idx = 0; blk_idx < geo_->nblocks; ++blk_idx) {
    struct nvm_vblk *blk;

    blk = nvm_vblk_alloc_line(dev_, 0, geo_->nchannels-1, 0, geo_->nluns-1, blk_idx);

    if (!blk) {
      NVM_DBG(this, "FAILED: nvm_vblk_alloc_line");
      return Status::IOError("FAILED: nvm_vblk_alloc_line");
    }

    blks_.push_back(std::make_pair(kFree, blk));
  }

  if (!env_->posix_->FileExists(mpath).ok()) {
    NVM_DBG(this, "INFO: mpath does not exist, nothing to recover.");
    return Status::OK();
  }

  std::string meta, head, dev_name;             // Recover from mpath
  Status s = ReadFileToString(env_->posix_, mpath, &meta);
  if (!s.ok()) {
    NVM_DBG(this, "FAILED: ReadFileToString");
    return s;
  }

  std::istringstream meta_ss(meta);
  if (!(meta_ss >> curs_)) {                    // Recover curs_
    NVM_DBG(this, "FAILED: parsing HEAD(curs_) from meta");
    return Status::IOError("FAILED: parsing HEAD(curs_) from meta");
  }
  if (!(meta_ss >> dev_name)) {                 // Ignore dev_name
    NVM_DBG(this, "FAILED: getting dev_name");
    return Status::IOError("FAILED: parsing dev_name");
  }

  std::string line;                             // Recover states
  for(size_t blk_idx = 0; meta_ss >> line; ++blk_idx) {
    int state = strtoul(line.c_str(), NULL, 16);

    switch(state) {
      case kFree:
      case kOpen:
      case kReserved:
      case kBad:
        blks_[blk_idx].first = BlkState(state);
        break;

      default:
        break;
    }
  }

  return Status::OK();
}

Status NvmStore::persist(const std::string &mpath) {
  NVM_DBG(this, "mpath(" << mpath << ")");
  MutexLock lock(&mutex_);

  std::stringstream meta_ss;

  meta_ss << curs_ << std::endl;
  meta_ss << dev_name_ << std::endl;
  for (auto &entry : blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;

  const std::string meta = meta_ss.str();

  Slice slice(meta.c_str(), meta.size());

  return WriteStringToFile(env_->posix_, slice, mpath, true);
}

NvmStore::~NvmStore(void) {
  NVM_DBG(this, "");
  Status s;

  s = persist(mpath_);
  if (!s.ok()) {
    NVM_DBG(this, "FAILED: writing meta");
  }

  for (auto &entry : blks_) {   // De-allocate blks
    if (!entry.second)
      continue;

    nvm_vblk_free(entry.second);
  }

  nvm_dev_close(dev_);          // Release device
}

struct nvm_vblk* NvmStore::get(void) {
  NVM_DBG(this, "");
  MutexLock lock(&mutex_);

  for (size_t i = 0; i < geo_->nblocks; ++i) {
    const size_t blk_idx = curs_++ % geo_->nblocks;
    std::pair<BlkState, struct nvm_vblk*> &entry = blks_[blk_idx];

    switch (entry.first) {
    case kFree:
      if (nvm_vblk_erase(entry.second) < 0) {
        entry.first = kBad;
        NVM_DBG(this, "WARN: Erase failed blk_idx(" << blk_idx << ")");
        break;
      }

    case kOpen:
      entry.first = kReserved;

      return entry.second;

    case kReserved:
    case kBad:
      break;
    }
  }

  NVM_DBG(this, "FAILED: OUT OF BLOCKS!");

  return NULL;
}

void NvmStore::put(struct nvm_vblk* blk) {
  NVM_DBG(this, "");
  MutexLock lock(&mutex_);

  size_t blk_idx = nvm_vblk_get_addrs(blk)[0].g.blk;
  NVM_DBG(this, "blk_idx(" << blk_idx << ")");
  blks_[blk_idx].first = kFree;
}

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
