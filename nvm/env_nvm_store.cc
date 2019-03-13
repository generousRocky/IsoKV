#include "env_nvm.h"
#include <exception>
#include <iostream>

#define USE_ALPHA 1
#define USE_BETA 1
#define USE_THETA 1
#define USE_GAMMA 1

/*
#define ALPHA_PUNIT_BEGIN 0
#define ALPHA_PUNIT_END 0

#define BETA_PUNIT_BEGIN 0
#define BETA_PUNIT_END 0

#define THETA_PUNIT_BEGIN 0
#define THETA_PUNIT_END 127
#define THETA_NR_LUN_PER_VBLK 128
*/

#define ALPHA_PUNIT_BEGIN 0
#define ALPHA_PUNIT_END 31

#define BETA_PUNIT_BEGIN 32
#define BETA_PUNIT_END 79

#define THETA_PUNIT_BEGIN 80
#define THETA_PUNIT_END 127
#define THETA_NR_LUN_PER_VBLK 48

#define GAMMA_PUNIT_BEGIN 0
#define GAMMA_PUNIT_END 127


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
    rate_(rate) {
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
#if USE_ALPHA
	for (size_t blk_idx = 510; blk_idx < geo_->nblocks; ++blk_idx) {
		std::vector<struct nvm_addr> addrs(punits_);
    std::vector<struct nvm_addr> resized_addrs_alpha;
		
		for(size_t vpunit_idx = ALPHA_PUNIT_BEGIN; vpunit_idx <= ALPHA_PUNIT_END; vpunit_idx++){
			addrs[vpunit_idx].g.blk = blk_idx;   
			resized_addrs_alpha.push_back(addrs[vpunit_idx]);
		}

		struct nvm_vblk *blk_alpha = nvm_vblk_alloc(dev_, resized_addrs_alpha.data(), resized_addrs_alpha.size());
		if (!blk_alpha) {
			NVM_DBG(this, "FAILED: [rocky] ALPHA_VBLK - nvm_vblk_alloc_line");
			return Status::IOError("FAILED: nvm_vblk_alloc_line");
		}
		
		vblks_.alpha_blks_.push_back(std::make_pair(kFree, blk_alpha));
		}
#endif
		
#if USE_BETA
	for (size_t blk_idx = 510; blk_idx < geo_->nblocks; ++blk_idx) {
		std::vector<struct nvm_addr> addrs(punits_);
		std::vector<struct nvm_addr> resized_addrs_beta;

		for(size_t vpunit_idx = BETA_PUNIT_BEGIN; vpunit_idx <= BETA_PUNIT_END; vpunit_idx++){
			addrs[vpunit_idx].g.blk = blk_idx;   
			resized_addrs_beta.push_back(addrs[vpunit_idx]);
		}
		
		struct nvm_vblk *blk_beta = nvm_vblk_alloc(dev_, resized_addrs_beta.data(), resized_addrs_beta.size());
		if (!blk_beta) {
			NVM_DBG(this, "FAILED: [rocky] BETA_VBLK - nvm_vblk_alloc_line");
			return Status::IOError("FAILED: nvm_vblk_alloc_line");
		}
		
		vblks_.beta_blks_.push_back(std::make_pair(kFree, blk_beta));
	}
#endif
		
#if USE_THETA
	// rocky: vblk 타입 중 예만 vblk 넘버와 blk idx가 같지 않음.
	for (size_t blk_idx = 510; blk_idx < geo_->nblocks; ++blk_idx) {
		std::vector<struct nvm_addr> addrs(punits_);

		size_t vpunit_idx = THETA_PUNIT_BEGIN; // 32
		while(vpunit_idx <= THETA_PUNIT_END){

			std::vector<struct nvm_addr> resized_addrs_theta;
  		NVM_DBG(this, "vpunit_idx: " << vpunit_idx);

			for(size_t i=0; i<THETA_NR_LUN_PER_VBLK; i++){
				addrs[vpunit_idx + i].g.blk = blk_idx;   
				resized_addrs_theta.push_back(addrs[vpunit_idx+i]);
			}
			struct nvm_vblk *blk_theta = nvm_vblk_alloc(dev_, resized_addrs_theta.data(), resized_addrs_theta.size());
			if (!blk_theta) {
				NVM_DBG(this, "FAILED: [rocky] THETA_VBLK - nvm_vblk_alloc_line");
				return Status::IOError("FAILED: nvm_vblk_alloc_line");
			}

			vblks_.theta_blks_.push_back(std::make_pair(kFree, blk_theta));
			vpunit_idx = vpunit_idx + THETA_NR_LUN_PER_VBLK;
		}
	}
#endif

#if USE_GAMMA
	for (size_t blk_idx = 0; blk_idx < 510; ++blk_idx) {
		std::vector<struct nvm_addr> addrs(punits_);
		std::vector<struct nvm_addr> resized_addrs_gamma;

		for(size_t vpunit_idx = GAMMA_PUNIT_BEGIN; vpunit_idx <= GAMMA_PUNIT_END; vpunit_idx++){
			addrs[vpunit_idx].g.blk = blk_idx;   
			resized_addrs_gamma.push_back(addrs[vpunit_idx]);
		}
		
		struct nvm_vblk *blk_gamma = nvm_vblk_alloc(dev_, resized_addrs_gamma.data(), resized_addrs_gamma.size());
		if (!blk_gamma) {
			NVM_DBG(this, "FAILED: [rocky] GAMMA_VBLK - nvm_vblk_alloc_line");
			return Status::IOError("FAILED: nvm_vblk_alloc_line");
		}
		
		vblks_.gamma_blks_.push_back(std::make_pair(kFree, blk_gamma));
	}
#endif

  
	NVM_DBG(this, "vblks_.alpha_blks_.size(): " << vblks_.alpha_blks_.size());
	NVM_DBG(this, "vblks_.beta_blks_.size(): " << vblks_.beta_blks_.size());
	NVM_DBG(this, "vblks_.theta_blks_.size(): " << vblks_.theta_blks_.size());
	NVM_DBG(this, "vblks_.gamma_blks_.size(): " << vblks_.gamma_blks_.size());

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
  
	// Recover blk states
  {
    std::string line;
		size_t vblk_idx;

		// for 3 vblk types
#if USE_ALPHA
    if (!(meta >> curs_.alpha_vblk_curs_)) {
      NVM_DBG(this, "FAILED: parsing HEAD(alpha_curs_) from meta");
      return Status::IOError("FAILED: parsing HEAD(alpha_curs_) from meta");
    }
		NVM_DBG(this, "alpha_curs_: " << curs_.alpha_vblk_curs_);
		
		for(vblk_idx = 510; vblk_idx < geo_->nblocks; vblk_idx++){
			meta >> line;	
			int state = strtoul(line.c_str(), NULL, 16);
			NVM_DBG(this, "alpha vblk_idx:" << vblk_idx << ", line: " << line);

			switch(state) {
				case kFree:
				case kOpen:
				case kReserved:
				case kBad:
						vblks_.alpha_blks_[vblk_idx].first = BlkState(state);
          break;
				default:
					break;
			}
		}
#endif

#if USE_BETA
    if (!(meta >> curs_.beta_vblk_curs_)) {
      NVM_DBG(this, "FAILED: parsing HEAD(beta_curs_) from meta");
      return Status::IOError("FAILED: parsing HEAD(beta_curs_) from meta");
    }
   
		NVM_DBG(this, "beta_curs_: " << curs_.beta_vblk_curs_);
		for(vblk_idx = 510; vblk_idx < geo_->nblocks; vblk_idx++){
			meta >> line;	
			int state = strtoul(line.c_str(), NULL, 16);
			NVM_DBG(this, "beta vblk_idx:" << vblk_idx << ", line: " << line);

			switch(state) {
				case kFree:
				case kOpen:
				case kReserved:
				case kBad:
						vblks_.beta_blks_[vblk_idx].first = BlkState(state);
          break;
				default:
					break;
			}
		}
#endif

#if USE_THETA
		if (!(meta >> curs_.theta_vblk_curs_)) {
      NVM_DBG(this, "FAILED: parsing HEAD(theta_curs_) from meta");
      return Status::IOError("FAILED: parsing HEAD(theta_curs_) from meta");
    }
    NVM_DBG(this, "theta_curs_: " << curs_.theta_vblk_curs_);
	
		size_t theta_power = (THETA_PUNIT_END - THETA_PUNIT_BEGIN + 1) / THETA_NR_LUN_PER_VBLK;
		for(vblk_idx = 510; vblk_idx < geo_->nblocks * theta_power; vblk_idx++){
			meta >> line;	
			int state = strtoul(line.c_str(), NULL, 16);
			NVM_DBG(this, "theta vblk_idx:" << vblk_idx << ", line: " << line);

			switch(state) {
				case kFree:
				case kOpen:
				case kReserved:
				case kBad:
						vblks_.theta_blks_[vblk_idx].first = BlkState(state);
          break;
				default:
					break;
			}
		}
#endif

#if USE_GAMMA
    if (!(meta >> curs_.gamma_vblk_curs_)) {
      NVM_DBG(this, "FAILED: parsing HEAD(gamma_curs_) from meta");
      return Status::IOError("FAILED: parsing HEAD(gamma_curs_) from meta");
    }
   
		NVM_DBG(this, "gamma_curs_: " << curs_.gamma_vblk_curs_);
		for(vblk_idx = 0; vblk_idx < 510; vblk_idx++){
			meta >> line;	
			int state = strtoul(line.c_str(), NULL, 16);
			NVM_DBG(this, "gamma vblk_idx:" << vblk_idx << ", line: " << line);

			switch(state) {
				case kFree:
				case kOpen:
				case kReserved:
				case kBad:
						vblks_.gamma_blks_[vblk_idx].first = BlkState(state);
          break;
				default:
					break;
			}
		}
#endif
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

#if USE_ALPHA
  meta_ss << curs_.alpha_vblk_curs_ << std::endl;        // Store state of blks
	for (auto &entry : vblks_.alpha_blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;
#endif

#if USE_BETA
	meta_ss << curs_.beta_vblk_curs_ << std::endl;        // Store state of blks
	for (auto &entry : vblks_.beta_blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;
#endif

#if USE_THETA 
	meta_ss << curs_.theta_vblk_curs_ << std::endl;        // Store state of blks
	for (auto &entry : vblks_.theta_blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;
#endif

#if USE_GAMMA
	meta_ss << curs_.gamma_vblk_curs_ << std::endl;        // Store state of blks
	for (auto &entry : vblks_.gamma_blks_)
    meta_ss << num_to_hex(entry.first, 2) << std::endl;
#endif
  
  const std::string meta = meta_ss.str();

  Slice slice(meta.c_str(), meta.size());

  return WriteStringToFile(env_->posix_, slice, mpath, true);
}

NvmStore::~NvmStore(void) {
  NVM_DBG(this, "");

  if (!persist(mpath_).ok()) {
    NVM_DBG(this, "FAILED: writing meta");
  }

  for (auto &entry : vblks_.alpha_blks_) {   // De-allocate blks
    if (!entry.second)
      continue;

    nvm_vblk_free(entry.second);
  }
  
	for (auto &entry : vblks_.beta_blks_) {   // De-allocate blks
    if (!entry.second)
      continue;

    nvm_vblk_free(entry.second);
  }
	
	for (auto &entry : vblks_.theta_blks_) {   // De-allocate blks
    if (!entry.second)
      continue;

    nvm_vblk_free(entry.second);
  }
	
  for (auto &entry : vblks_.gamma_blks_) {   // De-allocate blks
    if (!entry.second)
      continue;

    nvm_vblk_free(entry.second);
  }

  nvm_dev_close(dev_);          // Release device
}

struct nvm_vblk* NvmStore::get_dynamic(VblkType type) {

  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");
  
  for(size_t i=0; i < 510; i++){

    std::pair<BlkState, struct nvm_vblk*> *entry;
    size_t vblk_idx;

    switch(type){
			case gamma:
				vblk_idx = get_and_inc_curs_(type) % 510;
				entry = &(vblks_.gamma_blks_[vblk_idx]);
				break;
			case alpha:
				vblk_idx = (get_and_inc_curs_(type) % 510);
				entry = &(vblks_.alpha_blks_[vblk_idx]);
				break;
			case beta:
				vblk_idx = (get_and_inc_curs_(type) % 510);
				entry = &(vblks_.beta_blks_[vblk_idx]);
				break;
			case theta:
				//size_t theta_power = (THETA_PUNIT_END - THETA_PUNIT_BEGIN + 1) / THETA_NR_LUN_PER_VBLK;
				//vblk_idx = get_and_inc_curs_(type) % (geo_->nblocks * theta_power );
				vblk_idx = (get_and_inc_curs_(type) % 510);
				entry = &(vblks_.theta_blks_[vblk_idx]);
        break;
		}
		NVM_DBG(this, "[rocky] type (" << type << ")");
		NVM_DBG(this, "[rocky]: return vblk idx: " << vblk_idx  );
    
		switch (entry->first) {
      case kFree:
        if (nvm_vblk_erase(entry->second) < 0) {
          entry->first = kBad;
          NVM_DBG(this, "WARN: Erase failed tmp_blk_idx(" << vblk_idx << ")");

          if (!persist(mpath_).ok()) {
            NVM_DBG(this, "FAILED: writing meta");
          }
          break;
        }

      case kOpen:
        entry->first = kReserved;

        if (!persist(mpath_).ok()) {
          NVM_DBG(this, "FAILED: writing meta");
        }

        NVM_DBG(this, "[rocky] type (" << type << ")");
        NVM_DBG(this, "[rocky]: return vblk idx: " << vblk_idx  );
        return entry->second;

      case kReserved:
      case kBad:
        break;
		}
	}
	
  NVM_DBG(this, "FAILED: OUT OF BLOCKS!");
  return NULL;
}
#if 0
struct nvm_vblk* NvmStore::get(void) {

  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  size_t NR_VBLK = ( geo_->nblocks * punits_.size() ) / NR_LUN_FOR_VBLK ;

  for (size_t i = 0; i < NR_VBLK; i++) {
    const size_t vblk_idx = curs_++ % NR_VBLK ;

    std::pair<BlkState, struct nvm_vblk*> &entry = blks_[vblk_idx];

    switch (entry.first) {
      case kFree:
        if (nvm_vblk_erase(entry.second) < 0) {
          entry.first = kBad;

          NVM_DBG(this, "WARN: Erase failed vblk_idx(" << vblk_idx << ")");

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

        NVM_DBG(this, "[rocky]: return vblk idx: " << vblk_idx  );
        return entry.second;

      case kReserved:
      case kBad:
        break;
    }
  }


  return NULL;
}
#endif

#if 0
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
#endif

struct nvm_vblk* NvmStore::get_reserved_dynamic(size_t blk_idx, VblkType type) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");
	
	NVM_DBG(this, "[rocky] type (" << type << ")");
	

  std::pair<BlkState, struct nvm_vblk*> *entry;

  switch(type){
    case alpha:
      entry = &(vblks_.alpha_blks_[blk_idx]);
      break;
    case beta:
      entry = &(vblks_.beta_blks_[blk_idx]);
      break;
    case theta:
      entry = &(vblks_.theta_blks_[blk_idx]);
      break;
    case gamma:
      entry = &(vblks_.gamma_blks_[blk_idx]);
      break;
    default:
      return NULL;
  }

  switch(entry->first) {
  case kReserved:
    return entry->second;

  default:
    NVM_DBG(this, "FAILED: block is not reserved");
    return NULL;
  }
}

#if 0
void NvmStore::put(struct nvm_vblk* blk) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  size_t blk_idx = nvm_vblk_get_addrs(blk)[0].g.blk;
  NVM_DBG(this, "blk_idx(" << blk_idx << ")");
  blks_[blk_idx].first = kFree;
}
#endif

void NvmStore::put_dynamic(struct nvm_vblk* blk, VblkType type) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

	NVM_DBG(this, "[rocky] type (" << type << ")");
	
	size_t no_vblk;
	no_vblk = nvm_vblk_get_addrs(blk)[0].g.blk % 510;
	//std::cout << "[rocky dbg] type: " << type << "no_vlk: " << no_vblk << std::endl;

	std::pair<BlkState, struct nvm_vblk*> *entry;
	entry = &(vblks_.gamma_blks_[no_vblk]);

#if 0
	switch(type){
		case gamma:
			no_vblk = nvm_vblk_get_addrs(blk)[0].g.blk;
			entry = &(vblks_.gamma_blks_[no_vblk]);
			break;
		case alpha:
			no_vblk = nvm_vblk_get_addrs(blk)[0].g.blk;
			entry = &(vblks_.alpha_blks_[no_vblk]);
			break;
		case beta:
			no_vblk = nvm_vblk_get_addrs(blk)[0].g.blk;
			entry = &(vblks_.beta_blks_[no_vblk]);
			break;
		case theta:
			/*
			size_t theta_power = (THETA_PUNIT_END - THETA_PUNIT_BEGIN + 1) / THETA_NR_LUN_PER_VBLK;

			size_t vblk_blk = nvm_vblk_get_addrs(blk)[0].g.blk;
			size_t vblk_ch = nvm_vblk_get_addrs(blk)[0].g.ch;
			size_t vblk_lun = nvm_vblk_get_addrs(blk)[0].g.lun;

			size_t st_lun_no = 16 * vblk_lun + vblk_ch; // 48, 64, . . . 
			size_t st_lun_no_adj = st_lun_no - THETA_PUNIT_BEGIN; // 0, 16, . . .

			no_vblk = (vblk_blk * theta_power) + (st_lun_no_adj / THETA_NR_LUN_PER_VBLK);
			*/
			no_vblk = nvm_vblk_get_addrs(blk)[0].g.blk;
			entry = &(vblks_.theta_blks_[no_vblk]);
			break;
	}
#endif

	entry->first = kFree;
}

#if 0
// rocky: discard
void NvmStore::discard(struct nvm_vblk* blk) {
  NVM_DBG(this, "");
  NVM_DBG(this, "LOCK ?");
  MutexLock lock(&mutex_);
  NVM_DBG(this, "LOCK !");

  size_t blk_idx = nvm_vblk_get_addrs(blk)[0].g.blk;
  NVM_DBG(this, "blk_idx(" << blk_idx << ")");
  blks_[blk_idx].first = kBad;
}a
#endif 

std::string NvmStore::txt(void) {
  return "";
}

}       // namespace rocksdb
