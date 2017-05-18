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
  const std::vector<int>& punits,
  const std::string& mpath,
  size_t rate
) : env_(env), dev_name_(dev_name), dev_path_("/dev/"+dev_name), mpath_(mpath),
    rate_(rate), curs_(0) {
  NVM_DBG(this, "Opening NvmStore");

  dev_ = nvm_dev_open(dev_path_.c_str());               // Open device
  if (!dev_) {
    NVM_DBG(this, "FAILED: opening device");
    throw std::runtime_error("FAILED: opening device");
  }
  geo_ = nvm_dev_get_geo(dev_);                         // Grab geometry

  for (size_t i = 0; i < punits.size(); ++i) {          // Construct punit addrs
    struct nvm_addr addr;

    addr.ppa = 0;
    addr.g.ch = punits[i] % geo_->nchannels;
    addr.g.lun = (punits[i] / geo_->nchannels) % geo_->nluns;

    punits_.push_back(addr);
  }

  // TODO: Check for duplicates

  if (!recover(mpath_).ok()) {
    NVM_DBG(this, "FAILED: recovering");
    throw std::runtime_error("FAILED: recovering");
  }

  if (!persist(mpath).ok()) {
    NVM_DBG(this, "FAILED: persisting store meta");
    throw std::runtime_error("FAILED: persisting store meta");
  }
}

Status NvmStore::recover(const std::string& mpath)
{
  NVM_DBG(this, "mpath(" << mpath << ")");

  // Initialize and allocate vblks with defaults (kFree)
  for (size_t blk_idx = 0; blk_idx < geo_->nblocks; ++blk_idx) {
    struct nvm_vblk *blk;

    std::vector<struct nvm_addr> addrs(punits_);
    for (auto &addr : addrs)
      addr.g.blk = blk_idx;

    blk = nvm_vblk_alloc(dev_, addrs.data(), addrs.size());

    if (!blk) {
      NVM_DBG(this, "FAILED: nvm_vblk_alloc_line");
      return Status::IOError("FAILED: nvm_vblk_alloc_line");
    }

    blks_.push_back(std::make_pair(kFree, blk));
  }

  if (!env_->posix_->FileExists(mpath).ok()) {          // DONE
    NVM_DBG(this, "INFO: mpath does not exist, nothing to recover.");
    return Status::OK();
  }

  std::string content;
  Status s = ReadFileToString(env_->posix_, mpath, &content);
  if (!s.ok()) {
    NVM_DBG(this, "FAILED: ReadFileToString");
    return s;
  }

  std::istringstream meta(content);

  // Recover and verify that device name in meta matches the instance
  {
    std::string dev_name;
    if (!(meta >> dev_name)) {
      NVM_DBG(this, "FAILED: getting dev_name from nvm.meta");
      return Status::IOError("FAILED: getting dev_name from nvm.meta");
    }
    NVM_DBG(this, "dev_name: " << dev_name);

    if (dev_name != dev_name_) {
      NVM_DBG(this, "FAILED: instance dev_name != file dev_name");
      return Status::IOError("FAILED: instance dev_name != file dev_name");
    }
  }

  // Recover and verify punits in nvm.meta matches the instance
  {
    std::string punits;
    std::getline(meta, punits);
    std::getline(meta, punits);

    NVM_DBG(this, "punits: " << punits);
    if (meta.fail()) {      // TODO: Verify punits
      NVM_DBG(this, "FAILED: parsing HEAD(punits) from meta");
      return Status::IOError("FAILED: parsing HEAD(punits) from meta");
    }

    std::istringstream punits_ss(punits);
    std::string tok;
    size_t punit_idx;

    for (punit_idx = 0; punits_ss >> tok; ++punit_idx) {
      size_t punit = strtoul(tok.c_str(), NULL, 16);

      NVM_DBG(this, "punit_idx: " << punit_idx << ", punit: " << num_to_hex(punit, 16));
      if ((punit_idx+1) > punits_.size()) {
        NVM_DBG(this, "FAILED: Invalid punit count");
        return Status::IOError("FAILED: Invalid punit count");
      }

      if (punits_[punit_idx].ppa != punit) {
        NVM_DBG(this, "FAILED: punit address mismatch");
        return Status::IOError("FAILED: punit address mismatch");
      }
    }

    if (punit_idx != punits_.size()) {
      NVM_DBG(this, "FAILED: Invalid number of punits");
      return Status::IOError("Failed: Invalid number of punits");
    }
  }

  // Recover the wear-leveling cursor
  {
    if (!(meta >> curs_)) {
      NVM_DBG(this, "FAILED: parsing HEAD(curs_) from meta");
      return Status::IOError("FAILED: parsing HEAD(curs_) from meta");
    }
    NVM_DBG(this, "curs_: " << curs_);
  }

  // Recover blk states
  {
    std::string line;
    size_t blk_idx;

    for (blk_idx = 0; meta >> line; ++blk_idx) {

      if ((blk_idx+1) > geo_->nblocks) {
        NVM_DBG(this, "FAILED: Block count exceeding geometry");
        return Status::IOError("FAILED: Block count exceeding geometry");
      }

      int state = strtoul(line.c_str(), NULL, 16);

      NVM_DBG(this, "blk_idx:" << blk_idx << ", line: " << line);

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

    if (blk_idx != geo_->nblocks) {
      NVM_DBG(this, "FAILED: Insufficient block count");
      return Status::IOError("FAILED: Insufficient block count");
    }

  }

  return Status::OK();
}

Status NvmStore::persist(const std::string &mpath) {
  NVM_DBG(this, "mpath(" << mpath << ")");

  std::stringstream meta_ss;

  meta_ss << dev_name_ << std::endl;    // Store device name

  for (auto &addr : punits_) {          // Store parallel units
    meta_ss << num_to_hex(addr.ppa, 16) << " ";
  }
  meta_ss << std::endl;

  meta_ss << curs_ << std::endl;        // Store state of blks
  for (auto &entry : blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;

  const std::string meta = meta_ss.str();

  Slice slice(meta.c_str(), meta.size());

  return WriteStringToFile(env_->posix_, slice, mpath, true);
}

NvmStore::~NvmStore(void) {
  NVM_DBG(this, "");

  if (!persist(mpath_).ok()) {
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
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  for (size_t i = 0; i < geo_->nblocks; ++i) {
    const size_t blk_idx = curs_++ % geo_->nblocks;
    std::pair<BlkState, struct nvm_vblk*> &entry = blks_[blk_idx];

    switch (entry.first) {
    case kFree:
      if (nvm_vblk_erase(entry.second) < 0) {
        entry.first = kBad;

        NVM_DBG(this, "WARN: Erase failed blk_idx(" << blk_idx << ")");

        if (!persist(mpath_).ok()) {
          NVM_DBG(this, "FAILED: writing meta");
        }
        break;
      }

    case kOpen:
      entry.first = kReserved;

      if (!persist(mpath_).ok()) {
        NVM_DBG(this, "FAILED: writing meta");
      }
      return entry.second;

    case kReserved:
    case kBad:
      break;
    }
  }

  NVM_DBG(this, "FAILED: OUT OF BLOCKS!");

  return NULL;
}

struct nvm_vblk* NvmStore::get_reserved(size_t blk_idx) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  std::pair<BlkState, struct nvm_vblk*> &entry = blks_[blk_idx];

  switch(entry.first) {
  case kReserved:
    return entry.second;

  default:
    NVM_DBG(this, "FAILED: block is not reserved");
    return NULL;
  }
}

void NvmStore::put(struct nvm_vblk* blk) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  size_t blk_idx = nvm_vblk_get_addrs(blk)[0].g.blk;
  NVM_DBG(this, "blk_idx(" << blk_idx << ")");
  blks_[blk_idx].first = kFree;
}

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
