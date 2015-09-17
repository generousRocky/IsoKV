#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"
#include "malloc.h"
#include "util/coding.h"
#include <cstring>

namespace rocksdb {

// Static method encoding metadata for DFlash backend
// See comment above NVM:PrivateMetadata::GetMetadata()
void Env::EncodePrivateMetadata(std::string *dst, void *metadata) {
  if (metadata == nullptr) {
    return;
  }

  // metadata already contains the encoded data. See comment in
  // NVMPrivateMetadata::GetMetadata()
  struct vblock_meta *vblock_meta = (struct vblock_meta*)metadata;
  dst->append((const char*)vblock_meta->encoded_vblocks, vblock_meta->len);
}

void Env::DecodePrivateMetadata(Slice *input) {
  uint32_t meta32;
  uint64_t meta64;

  GetVarint64(input, &meta64);
  uint64_t left = meta64;

  while (left > 0) {
    GetVarint32(input, &meta32);
    printf("separator: %d", meta32);
    GetVarint64(input, &meta64);
    printf("id: %lu", meta64);
    GetVarint64(input, &meta64);
    printf("owner id: %lu", meta64);
    GetVarint64(input, &meta64);
    printf("nppas: %lu", meta64);
    GetVarint64(input, &meta64);
    printf("bitmap: %lu", meta64);
    GetVarint64(input, &meta64);
    printf("bppa: %lu", meta64);
    GetVarint32(input, &meta32);
    printf("vlunid: %d", meta32);
    GetVarint32(input, &meta32);
    printf("flags: %d\n", meta32);

    left--;
  }
}

#if defined(OS_LINUX)

static size_t GetUniqueIdFromFile(nvm_file *fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length*3) {
    return 0;
  }

  char* rid = id;

  rid = EncodeVarint64(rid, (uint64_t)fd);

  if (rid >= id) {
    return static_cast<size_t>(rid - id);
  } else {
    return static_cast<size_t>(id - rid);
  }
}

#endif

nvm_file::nvm_file(const char *_name, const int fd, nvm_directory *_parent) {
  char *name;

  NVM_DEBUG("constructing file %s in %s", _name, _parent == nullptr ? "NULL" : _parent->GetName());

  int name_len = strlen(_name);

  if (name_len != 0) {
    SAFE_ALLOC(name, char[name_len + 1]);
    strcpy(name, _name);

    ALLOC_CLASS(names, list_node(name))
  } else {
    names = nullptr;
  }

  size_ = 0;
  fd_ = fd;

  last_modified = time(nullptr);
  vblocks_.clear();
  pages.clear();

  opened_for_write = false;

  parent = _parent;

  seq_writable_file = nullptr;
  current_vblock_ = nullptr;

  pthread_mutexattr_init(&page_update_mtx_attr);
  pthread_mutexattr_settype(&page_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);

  pthread_mutex_init(&page_update_mtx, &page_update_mtx_attr);
  pthread_mutex_init(&meta_mtx, &page_update_mtx_attr);
  pthread_mutex_init(&write_lock, &page_update_mtx_attr);
  pthread_mutex_init(&file_lock, &page_update_mtx_attr);
}

nvm_file::~nvm_file() {
  NVM_DEBUG("Destroy nvm_file\n");
  if (seq_writable_file) {
    //flush any existing buffers
    seq_writable_file->Close();
    seq_writable_file = nullptr;
  }

  list_node *temp = names;
  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete[] (char *)temp->GetData();
    delete temp;

    temp = temp1;
  }

  struct nvm *nvm = parent->GetNVMApi();
  PutAllBlocks(nvm);
  vblocks_.clear();

  pthread_mutexattr_destroy(&page_update_mtx_attr);
  pthread_mutex_destroy(&page_update_mtx);
  pthread_mutex_destroy(&meta_mtx);
  pthread_mutex_destroy(&write_lock);
  pthread_mutex_destroy(&file_lock);
}

void nvm_file::ReclaimPage(nvm *nvm_api, struct nvm_page *pg) {
#ifdef NVM_ALLOCATE_BLOCKS

  bool has_more_pages_allocated = false;

  for (unsigned long i = 0; i < pages.size(); ++i) {
    if (pages[i]->lun_id != pg->lun_id) {
      continue;
    }

    if (pages[i]->block_id != pg->block_id) {
      continue;
    }

    has_more_pages_allocated = true;

    break;
  }

  if (has_more_pages_allocated) {
    return;
  }

  for (unsigned long i = 0; i < block_pages.size(); ++i) {
    if (block_pages[i]->lun_id != pg->lun_id) {
      continue;
    }

    if (block_pages[i]->block_id != pg->block_id) {
      continue;
    }

    has_more_pages_allocated = true;

    break;
  }

  if (has_more_pages_allocated == false) {
    NVM_DEBUG("block %lu - %lu has no more pages allocated", pg->lun_id, pg->block_id);

    nvm_api->ReclaimBlock(pg->lun_id, pg->block_id);
  }

#else

  nvm_api->ReclaimPage(pg);

#endif
}

struct nvm_page *nvm_file::RequestPage(nvm *nvm_api) {
  struct nvm_page *ret;

#ifdef NVM_ALLOCATE_BLOCKS

  if (block_pages.empty()) {
    if (nvm_api->RequestBlock(&block_pages) == false) {
      return nullptr;
    }
  }

  ret = block_pages.back();
  block_pages.pop_back();

#else

  ret = nvm_api->RequestPage();

#endif

  return ret;
}

struct nvm_page *nvm_file::RequestPage(nvm *nvm_api, const unsigned long lun_id,
                  const unsigned long block_id, const unsigned long page_id) {

#ifdef NVM_ALLOCATE_BLOCKS

  struct nvm_page *ret = nullptr;

retry:

  for (unsigned long i = 0; i < block_pages.size(); ++i) {
    if (block_pages[i]->lun_id != lun_id) {
      continue;
    }

    if (block_pages[i]->block_id != block_id) {
      continue;
    }

    if (block_pages[i]->id != page_id) {
      continue;
    }

    ret = block_pages[i];

    break;
  }

  if (ret) {
    return ret;
  }

  if (nvm_api->RequestBlock(&block_pages, lun_id, block_id) == false) {
    return nullptr;
  }

  goto retry;

#else

  return nvm_api->RequestPage(lun_id, block_id, page_id);

#endif
}

void nvm_file::SetSeqWritableFile(NVMWritableFile *_writable_file) {
  seq_writable_file = _writable_file;
}

void nvm_file::SetParent(nvm_directory *_parent) {
  parent = _parent;
}

nvm_directory *nvm_file::GetParent() {
  return parent;
}

int nvm_file::LockFile() {
  return pthread_mutex_lock(&file_lock);
}

void nvm_file::UnlockFile() {
  pthread_mutex_unlock(&file_lock);
}

Status nvm_file::Load(const int fd) {
  std::string _name;

  char readIn;

  NVM_DEBUG("loading file %p", this);

  if (read(fd, &readIn, 1) != 1) {
    return Status::IOError("Could no read f 1");
  }

  if (readIn != ':') {
    NVM_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  //load names

  _name = "";

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 2");
    }

    if (readIn == ',' || readIn == ':') {
      NVM_DEBUG("Adding name %s to %p", _name.c_str(), this);

      AddName(_name.c_str());

      _name = "";
    } else {
      _name.append(&readIn, 1);
    }
  } while (readIn != ':');

  //load size

  size_ = 0;

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 3");
    }

    if (readIn >= '0' && readIn <= '9') {
      size_ = size_ * 10 + readIn - '0';
    }
  } while (readIn >= '0' && readIn <= '9');

  if (readIn != ':') {
    NVM_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  //load last modified

  last_modified = 0;

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 3");
    }

    if (readIn >= '0' && readIn <= '9') {
      last_modified = last_modified * 10 + readIn - '0';
    }
  } while (readIn >= '0' && readIn <= '9');

  if (readIn != ':') {
    NVM_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  do {
    unsigned long lun_id = 0;
    unsigned long block_id = 0;
    unsigned long page_id = 0;

    bool page_to_claim = true;

    do {
      if (readIn == '\n') {
        page_to_claim = false;

        break;
      }

      if (read(fd, &readIn, 1) != 1) {
        return Status::IOError("Could no read f 3");
      }

      if (readIn >= '0' && readIn <= '9') {
        lun_id = lun_id * 10 + readIn - '0';
      }
    } while (readIn != '-');

    do {
      if (readIn == '\n') {
        page_to_claim = false;

        break;
      }

      if (read(fd, &readIn, 1) != 1) {
        return Status::IOError("Could no read f 3");
      }

      if (readIn >= '0' && readIn <= '9') {
        block_id = block_id * 10 + readIn - '0';
      }
    } while (readIn != '-');

    do {
      if (readIn == '\n') {
        page_to_claim = false;

        break;
      }

      if (read(fd, &readIn, 1) != 1) {
        return Status::IOError("Could no read f 3");
      }

      if (readIn >= '0' && readIn <= '9') {
        page_id = page_id * 10 + readIn - '0';
      }
    } while (readIn != ',' && readIn != '\n');

    if (page_to_claim) {
      ClaimNewPage(parent->GetNVMApi(), lun_id, block_id, page_id);
    }
  } while (readIn != '\n');

  return Status::OK();
}

Status nvm_file::Save(const int fd) {
  char temp[100];

  unsigned int len;

  std::vector<std::string> _names;

  EnumerateNames(&_names);

  if (write(fd, "f:", 2) != 2) {
    return Status::IOError("fError writing 2");
  }

  for (unsigned int i = 0; i < _names.size(); ++i) {
    if (i > 0) {
      if (write(fd, ",", 1) != 1) {
        return Status::IOError("fError writing 3");
      }
    }

    len = _names[i].length();

    if (write(fd, _names[i].c_str(), len) != len) {
      return Status::IOError("fError writing 4");
    }
  }

  if (write(fd, ":", 1) != 1) {
    return Status::IOError("fError writing 5");
  }

  len = sprintf(temp, "%lu", GetSize());

  if (write(fd, temp, len) != len) {
    return Status::IOError("fError writing 6");
  }

  if (write(fd, ":", 1) != 1) {
    return Status::IOError("fError writing 7");
  }

  len = sprintf(temp, "%ld", GetLastModified());

  if (write(fd, temp, len) != len) {
    return Status::IOError("fError writing 8");
  }

  if (write(fd, ":", 1) != 1) {
    return Status::IOError("fError writing 9");
  }

  for (unsigned int i = 0; i < pages.size(); ++i) {
    if (i > 0) {
      if (write(fd, ",", 1) != 1) {
        return Status::IOError("fError writing 10");
      }
    }

    len = sprintf(temp, "%lu-%lu-%lu", pages[i]->lun_id, pages[i]->block_id, pages[i]->id);

    if (write(fd, temp, len) != len) {
      return Status::IOError("fError writing 11");
    }
  }

  if (write(fd, "\n", 1) != 1) {
    return Status::IOError("fError writing 12");
  }

  return Status::OK();
}

bool nvm_file::ClearLastPage(nvm *nvm_api) {
  pthread_mutex_lock(&page_update_mtx);

  if (pages.size() == 0) {
    pthread_mutex_unlock(&page_update_mtx);
    return true;
  }

  struct nvm_page *pg = pages[pages.size() - 1];

  ReclaimPage(nvm_api, pg);

  pg = RequestPage(nvm_api);

  if (pg == nullptr) {
    pthread_mutex_unlock(&page_update_mtx);
    return false;
  }

  pages[pages.size() - 1] = pg;

  pthread_mutex_unlock(&page_update_mtx);

  return true;
}

bool nvm_file::ClaimNewPage(nvm *nvm_api, const unsigned long lun_id,
                const unsigned long block_id, const unsigned long page_id) {
  struct nvm_page *new_page = RequestPage(nvm_api, lun_id, block_id, page_id);

  NVM_DEBUG("File at %p claimed page %lu-%lu-%lu", this, new_page->lun_id,
                                            new_page->block_id, new_page->id);

  if (new_page == nullptr) {
    return false;
  }

  pthread_mutex_lock(&page_update_mtx);
  pages.push_back(new_page);
  pthread_mutex_unlock(&page_update_mtx);

  return true;
}

bool nvm_file::ClaimNewPage(nvm *nvm_api) {
  struct nvm_page *new_page = RequestPage(nvm_api);

  NVM_DEBUG("File at %p claimed page %lu-%lu-%lu", this, new_page->lun_id, new_page->block_id, new_page->id);

  if (new_page == nullptr) {
    return false;
  }

  pthread_mutex_lock(&page_update_mtx);
  pages.push_back(new_page);
  pthread_mutex_unlock(&page_update_mtx);

  return true;
}

time_t nvm_file::GetLastModified() {
  time_t ret;

  pthread_mutex_lock(&meta_mtx);
  ret = last_modified;
  pthread_mutex_unlock(&meta_mtx);

  return ret;
}

void nvm_file::Close(const char *mode) {
  if (mode[0] == 'r' || mode[0] == 'l') {
    return;
  }

  opened_for_write = false;
}

bool nvm_file::CanOpen(const char *mode) {
  bool ret = true;

  //file can always be opened for read or lock
  if (mode[0] == 'r' || mode[0] == 'l') {
    return ret;
  }

  pthread_mutex_lock(&meta_mtx);

  if (opened_for_write) {
    ret = false;
  } else {
    opened_for_write = true;
  }

  pthread_mutex_unlock(&meta_mtx);
  return ret;
}

void nvm_file::EnumerateNames(std::vector<std::string>* result) {
  pthread_mutex_lock(&meta_mtx);

  list_node *name_node = names;

  while (name_node) {
    char *_name = (char *)name_node->GetData();

    result->push_back(_name);

    name_node = name_node->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);
}

bool nvm_file::HasName(const char *name, const int n) {
  int i;

  pthread_mutex_lock(&meta_mtx);

  list_node *name_node = names;

  while (name_node) {
    char *_name = (char *)name_node->GetData();

    for (i = 0; i < n; ++i) {
      if (name[i] != _name[i]) {
        goto next;
      }
    }
    if (_name[i] == '\0') {
      pthread_mutex_unlock(&meta_mtx);

      return true;
    }

next:

    name_node = name_node->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);

  return false;
}

void nvm_file::AddName(const char *name) {
  list_node *name_node;

  char *_name;

  SAFE_ALLOC(_name, char[strlen(name) + 1]);
  strcpy(_name, name);

  ALLOC_CLASS(name_node, list_node(_name));

  pthread_mutex_lock(&meta_mtx);

  name_node->SetNext(names);

  if (names) {
    names->SetPrev(name_node);
  }

  names = name_node;

  pthread_mutex_unlock(&meta_mtx);
}

void nvm_file::ChangeName(const char *crt_name, const char *new_name) {
  pthread_mutex_lock(&meta_mtx);

  list_node *name_node = names;

  while (name_node) {
    char *crt_name_node = (char *)name_node->GetData();

    NVM_DEBUG("change name %s vs %s", crt_name_node, crt_name);

    if (strcmp(crt_name_node, crt_name) == 0) {
      NVM_DEBUG("MATCH");

      delete[] crt_name_node;

      SAFE_ALLOC(crt_name_node, char[strlen(new_name) + 1]);
      strcpy(crt_name_node, new_name);

      name_node->SetData(crt_name_node);

      NVM_DEBUG("SET DATA %s", crt_name_node);

      pthread_mutex_unlock(&meta_mtx);

      return;
    }

    name_node = name_node->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);
}

int nvm_file::GetFD() {
  return fd_;
}

unsigned long nvm_file::GetSize() {
  unsigned long ret;

  pthread_mutex_lock(&page_update_mtx);
  ret = size_;
  pthread_mutex_unlock(&page_update_mtx);
  return ret;
}

void nvm_file::UpdateFileModificationTime() {
  pthread_mutex_lock(&meta_mtx);

  last_modified = time(nullptr);

  pthread_mutex_unlock(&meta_mtx);
}

void nvm_file::DeleteAllLinks(struct nvm *_nvm_api) {
  NVM_DEBUG("removing all links in %p", this);

  pthread_mutex_lock(&page_update_mtx);

  for (unsigned long i = 0; i < pages.size(); ++i) {
    ReclaimPage(_nvm_api, pages[i]);
  }

  pages.clear();

  size_ = 0;

  pthread_mutex_unlock(&page_update_mtx);

  UpdateFileModificationTime();
}

bool nvm_file::Delete(const char * filename, struct nvm *nvm_api) {
  bool link_files_left = true;

  list_node *temp;
  list_node *temp1;
  list_node *next;
  list_node *prev;

  pthread_mutex_lock(&meta_mtx);

  temp = names;

  NVM_DEBUG("Deleting %s", filename);

  while (temp) {
    if (strcmp(filename, (char *)temp->GetData()) == 0) {
      temp1 = temp;

      prev = temp->GetPrev();
      next = temp->GetNext();

      if (prev) {
        NVM_DEBUG("Prev is not null");
        prev->SetNext(next);
      }

      if (next) {
        NVM_DEBUG("Next is not null");
        next->SetPrev(prev);
      }

      if (prev == nullptr && next != nullptr) {
        NVM_DEBUG("Moving head");
        names = names->GetNext();
      }

      if (next == nullptr && prev == nullptr) {
        NVM_DEBUG("No more links for this file");
        link_files_left = false;
        names = nullptr;
      }

      delete[] (char *)temp1->GetData();
      delete temp1;

      break;
    }

    temp = temp->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);

  if (link_files_left) {
    //we have link file pointing here.. don't delete

    return false;
  }

  NVM_DEBUG("Deleting all links");

  DeleteAllLinks(nvm_api);

  return true;
}

struct nvm_page *nvm_file::GetLastPage(unsigned long *page_idx) {
  struct nvm_page *ret;

  pthread_mutex_lock(&page_update_mtx);

  if (pages.size() == 0) {
    ret = nullptr;
  } else {
    ret = pages[pages.size() - 1];

    *page_idx = pages.size() - 1;
  }

  pthread_mutex_unlock(&page_update_mtx);
  return ret;
}

bool nvm_file::SetPage(const unsigned long page_idx, nvm_page *page) {
  bool ret = false;

  pthread_mutex_lock(&page_update_mtx);

  if (pages.size() > page_idx) {
    pages[page_idx] = page;

    ret = true;
  }

  pthread_mutex_unlock(&page_update_mtx);

  return ret;
}

struct nvm_page *nvm_file::GetNVMPage(const unsigned long idx) {
  struct nvm_page *ret;

  pthread_mutex_lock(&page_update_mtx);

  if (idx < pages.size()) {
    ret = pages[idx];
  } else {
    ret = nullptr;
  }

  pthread_mutex_unlock(&page_update_mtx);
  return ret;
}

//TODO: REFACTOR FROM HERE UP!!

size_t nvm_file::ReadBlock(struct nvm *nvm, unsigned int block_offset,
                              size_t ppa_offset, unsigned int page_offset,
                                                char *data, size_t data_len) {
  struct vblock *current_vblock = vblocks_[block_offset];
  size_t base_ppa = current_vblock->bppa;
  size_t nppas = current_vblock->nppas;
  size_t current_ppa = base_ppa + ppa_offset;
  size_t read_offset;
  size_t ret;
  unsigned long max_bytes_per_read = nvm->max_pages_in_io * PAGE_SIZE;
  unsigned long bytes_per_read;
  unsigned int meta_beg_size = sizeof(struct vblock_recov_meta);
  uint8_t pages_per_read;

  // Attempting to read an empty file
  if(vblocks_.size() == 0) {
    return 0;
  }

  //Always read at a page granurality
  uint8_t x =
            ((data_len + page_offset + meta_beg_size) % PAGE_SIZE == 0) ? 0 : 1;
  size_t left =
      ((((data_len + page_offset + meta_beg_size) / PAGE_SIZE) + x) * PAGE_SIZE);

  assert(left <= (nppas * PAGE_SIZE));
  assert((left % PAGE_SIZE) == 0);

  char *page = (char*)memalign(PAGE_SIZE, left * PAGE_SIZE);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory of length: %lu\n",
                                                            nppas * PAGE_SIZE);
    return -1;
  }

  char *read_iter = page;

  while (left > 0) {
retry:
    bytes_per_read = (left > max_bytes_per_read) ? max_bytes_per_read : left;
    pages_per_read = bytes_per_read / PAGE_SIZE;

    if ((unsigned)pread(fd_, read_iter, bytes_per_read, current_ppa * PAGE_SIZE)
                                                          != bytes_per_read) {
      if (errno == EINTR) {
        goto retry;
      }
      ret = -1;
      goto out;
    }

    current_ppa += pages_per_read;
    read_iter += bytes_per_read;
    left -= bytes_per_read;
  }

  //Account for crash recovery metadata at the beginning of the block
  read_offset = page_offset + meta_beg_size;
  memcpy(data, page + read_offset, data_len);

  IOSTATS_ADD(bytes_read, data_len);
  ret = data_len - left;

out:
  free(page);
  return ret;
}

size_t nvm_file::Read(struct nvm *nvm, size_t read_pointer, char *data,
                                                        size_t data_len) {
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix (FIXME)
  unsigned int block_offset = read_pointer / (nppas * PAGE_SIZE);
  size_t ppa_offset = (read_pointer / PAGE_SIZE) % (nppas * PAGE_SIZE);
  unsigned int page_offset = read_pointer % PAGE_SIZE;
  size_t left = data_len;
  size_t bytes_left_block;
  size_t bytes_per_read;
  size_t total_read = 0;
  size_t meta_size =
            sizeof(struct vblock_recov_meta) + sizeof(struct vblock_close_meta);

  while (left > 0) {
    bytes_left_block = ((nppas - ppa_offset) * PAGE_SIZE) - meta_size;
    bytes_per_read = (left > bytes_left_block) ? bytes_left_block : left;
    size_t read = ReadBlock(nvm, block_offset, ppa_offset, page_offset,
                                          data + total_read, bytes_per_read);

    total_read += read;
    block_offset++;
    ppa_offset = 0;
    left -= read;
  }

  return total_read;
}

//TODO: Javier: get list of vluns at initialization and ask for a vblock from
//the right vlun at runtime. We still need to define how the BM and the FTL
//will agree on which vlun is going to be used. To start with we have a single
//vlun (vlun 0).
void nvm_file::GetBlock(struct nvm *nvm, unsigned int vlun_id) {
  //TODO: Make this better: mmap memory into device??
  struct vblock *new_vblock = (struct vblock*)malloc(sizeof(struct vblock));
  if (!new_vblock) {
    NVM_FATAL("Could not allocate memory\n");
  }

  if (!nvm->GetBlock(vlun_id, new_vblock)) {
    NVM_FATAL("could not get a new block - ssd out of space\n");
  }

  pthread_mutex_lock(&page_update_mtx);
  vblocks_.push_back(new_vblock);
  current_vblock_ = new_vblock;
  pthread_mutex_unlock(&page_update_mtx);
}

void nvm_file::ReplaceBlock(struct nvm *nvm, unsigned int vlun_id,
                                                    unsigned int block_idx) {
  assert(block_idx < vblocks_.size());

  struct vblock *new_vblock = (struct vblock*)malloc(sizeof(struct vblock));
  struct vblock *old_vblock;

  if (!new_vblock) {
    NVM_FATAL("Could not allocate memory\n");
  }

  if (!nvm->GetBlock(vlun_id, new_vblock)) {
    NVM_FATAL("could not get a new block - ssd out of space\n");
  }

  pthread_mutex_lock(&page_update_mtx);
  old_vblock = vblocks_[block_idx];
  vblocks_[block_idx] = new_vblock;

  current_vblock_ = new_vblock;

  pthread_mutex_unlock(&page_update_mtx);
  nvm->EraseBlock(old_vblock);
  free(old_vblock);
}

void nvm_file::PutBlock(struct nvm *nvm, struct vblock *vblock) {
  assert(vblock != nullptr);
  if (!nvm->PutBlock(vblock)) {
    //TODO: Can we recover from this?
    NVM_FATAL("could not return block to BM\n");
    return;
  }

  free(vblock);
  vblock = nullptr;
}

void nvm_file::PutAllBlocks(struct nvm *nvm) {
  std::vector<struct vblock *>::iterator it;

  for (it = vblocks_.begin(); it != vblocks_.end(); it++) {
    PutBlock(nvm, *it);
  }

  current_vblock_ = nullptr;
}

// For the moment we assume that all pages in a block are good pages. In the
// future we would have to check ppa_bitmap and come back to the memtable to
// move the KV pairs that do not fit in the block assigned to it to create the
// sstable. This will require to send a ppa_list down to LightNVM to enable
// multipage writes - at the moment multipage assumes sequential ppas
//
// Note that data_len is the real length of the data to be flushed to the flash
// block. FlushBlock takes care of working on PAGE_SIZE chunks
size_t nvm_file::FlushBlock(struct nvm *nvm, char *data, size_t ppa_offset,
                                        size_t data_len, bool page_aligned) {
  size_t base_ppa = current_vblock_->bppa;
  size_t nppas = current_vblock_->nppas;
  size_t current_ppa = base_ppa + ppa_offset;
  unsigned long max_bytes_per_write = nvm->max_pages_in_io * PAGE_SIZE;
  unsigned long bytes_per_write;
  uint8_t pages_per_write;
  uint8_t allocate_aligned_buf = 0;
  unsigned int meta_size = 0;
  size_t ret;

  // Always write at a page granurality
  size_t write_len;
  char *data_aligned;

  // Page metadata to be stored in out of bound area
  struct vpage_meta per_page_meta = {
    .valid_bytes = PAGE_SIZE,
    .flags = VPAGE_VALID | VPAGE_FULL,
  };
  struct vpage_meta last_page_meta;

  // Flush has been forced by upper layers and page is not aligned to PAGE_SIZE
  if (!page_aligned) {
    size_t disaligned_data = data_len % PAGE_SIZE;
    size_t aligned_data = data_len / PAGE_SIZE;
    uint8_t x = (disaligned_data == 0) ? 0 : 1;
    write_len = ((aligned_data + x) * PAGE_SIZE);

    if (write_len != data_len) {
      last_page_meta.valid_bytes = disaligned_data;
      last_page_meta.flags = VPAGE_VALID;
    } else {
      // Last page is yet another complete page
      last_page_meta = per_page_meta;
    }
  } else {
    write_len = data_len;
  }

  unsigned long left = write_len;

  if (write_len > nppas * PAGE_SIZE) {
    NVM_DEBUG("writelen: %lu, nppas: %lu, total:%lu\n", write_len, nppas, nppas * PAGE_SIZE);
  }
  // assert(write_len <= nppas * PAGE_SIZE);

  /* Verify that data buffer is aligned, although it should already be aligned */
  if (UNLIKELY(((uintptr_t)data % PAGE_SIZE) != 0)) {
    data_aligned = (char*)memalign(PAGE_SIZE, write_len);
    if (!data_aligned) {
      NVM_FATAL("Cannot allocate aligned memory\n");
      return 0;
    }
    memcpy(data_aligned, data, write_len);
    allocate_aligned_buf = 1;
  } else {
    data_aligned = data;
  }

  //TODO: Use libaio instead of pread/pwrite
  //TODO: Write in out of bound area when API is ready (per_page_meta and
  //last_page_meta
  while (left > 0) {
    // left is guaranteed to be a multiple of PAGE_SIZE
    bytes_per_write = (left > max_bytes_per_write) ? max_bytes_per_write : left;
    pages_per_write = bytes_per_write / PAGE_SIZE;

    if ((unsigned)pwrite(fd_, data_aligned, bytes_per_write,
                                     current_ppa * PAGE_SIZE) != bytes_per_write) {
      //TODO: See if we can recover. Use another ppa + mark bad page in bitmap?
      NVM_ERROR("ERROR: Page no written\n");
      ret = 0;
      goto out;
    }

    data_aligned += bytes_per_write;
    left -= bytes_per_write;
    current_ppa += pages_per_write;
  }

  if (current_ppa == base_ppa + nppas) {
     meta_size += sizeof(struct vblock_close_meta);
  }

  if (ppa_offset == 0) {
    meta_size += sizeof(struct vblock_recov_meta);
  }

  pthread_mutex_lock(&page_update_mtx);
  size_ += data_len - left - meta_size;
  pthread_mutex_unlock(&page_update_mtx);

  UpdateFileModificationTime();
  IOSTATS_ADD(bytes_written, write_len);

  ret = write_len;

out:
  if (allocate_aligned_buf)
    free(data_aligned);

  return ret;
}

bool nvm_file::HasBlock() {
  return (current_vblock_ == nullptr) ? false : true;
}

/*
 * FilePrivateMetadata implementation
 */
NVMPrivateMetadata::NVMPrivateMetadata(nvm_file *file) {
  file_ = file;
}

NVMPrivateMetadata::~NVMPrivateMetadata() {}

//TODO: Can we maintain a friend reference to NVMWritableFile to simplify this?
void NVMPrivateMetadata::UpdateMetadataHandle(nvm_file *file) {
  file_ = file;
}

// For now store the whole vblock as metadata. When we can retrieve a vblock
// from its ID from the BM we can reduces the amount of metadata stored in
// MANIFEST
void* NVMPrivateMetadata::GetMetadata() {
  std::vector<struct vblock *>::iterator it;
  std::string metadata;

  PutVarint32(&metadata, file_->vblocks_.size());
  for (it = file_->vblocks_.begin(); it != file_->vblocks_.end(); it++) {
    printf("METADATA: Writing:\nsep:%d,id:%lu\noid:%lu\nnppas:%lu\nbitmap:%lu\nbppa:%llu\nvlunid:%d\nflags:%d\n",
      separator_, (*it)->id, (*it)->owner_id, (*it)->nppas, (*it)->ppa_bitmap, (*it)->bppa,
      (*it)->vlun_id, (*it)->flags);
    PutVarint32(&metadata, separator_); //This might go away
    PutVarint64(&metadata, (*it)->id);
    PutVarint64(&metadata, (*it)->owner_id);
    PutVarint64(&metadata, (*it)->nppas);
    PutVarint64(&metadata, (*it)->ppa_bitmap);
    PutVarint64(&metadata, (*it)->bppa);
    PutVarint32(&metadata, (*it)->vlun_id);
    PutVarint32(&metadata, (*it)->flags);
  }

  uint64_t metadata_size = metadata.length();
  struct vblock_meta *vblock_meta =
                        (struct vblock_meta*)malloc(sizeof(struct vblock_meta));
  vblock_meta->encoded_vblocks = (char*)malloc(metadata_size);

  vblock_meta->len = metadata_size;
  memcpy(vblock_meta->encoded_vblocks, metadata.c_str(), metadata_size);

  return (void*)vblock_meta;
}

/*
 * SequentialFile implementation
 */
NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *fd,
                                                        nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;

  if (!fd_->HasBlock()) {
    NVM_ERROR("No block associated with file descriptor\n");
  }

  read_pointer_ = 0;
}

NVMSequentialFile::~NVMSequentialFile() {
  dir_->nvm_fclose(fd_, "r");
}

//TODO: Cache last read page to avoid small reads submitting extra IOs. We
//should only cache until file size
Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  struct nvm *nvm = dir_->GetNVMApi();

  if (read_pointer_ + n > fd_->GetSize()) {
    n = fd_->GetSize() - read_pointer_;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  if (fd_->Read(nvm, read_pointer_, scratch, n) != n) {
    return Status::IOError("Unable to read\n");
  }

  read_pointer_ += n;
  *result = Slice(scratch, n);
  return Status::OK();
}

Status NVMSequentialFile::Skip(uint64_t n) {
  if (n == 0) {
    return Status::OK();
  }

  read_pointer_ += n;
  if (read_pointer_ > fd_->GetSize()) {
    read_pointer_ -=n;
    return Status::IOError(filename_, "EINVAL file pointer goes out of bounds");
  }


  return Status::OK();
}

Status NVMSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

/*
 * RandomAccessFile implementation
 */
NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, nvm_file *f,
                                       nvm_directory *dir) : filename_(fname) {
  fd_ = f;
  dir_ = dir;
}

NVMRandomAccessFile::~NVMRandomAccessFile() {
  dir_->nvm_fclose(fd_, "r");
}

Status NVMRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                                        char* scratch) const {
  if (offset >= fd_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  if (offset >= fd_->GetSize()) {
    n = fd_->GetSize() - offset;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  struct nvm *nvm = dir_->GetNVMApi();

  //Account for the metadata stored at the beginning of the virtual block.
  // uint64_t internal_offset = offset + sizeof(struct vblock_recov_meta);
  uint64_t internal_offset = offset;

  unsigned int ppa_offset = internal_offset / PAGE_SIZE;
  unsigned int page_offset = internal_offset % PAGE_SIZE;

  size_t data_len = (((n / PAGE_SIZE) + 1) * PAGE_SIZE);
  char *data = (char*)memalign(PAGE_SIZE, data_len);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  if (fd_->Read(nvm, ppa_offset, data, data_len) != data_len) {
    free(data);
    return Status::IOError("Unable to read\n");
  }

  //TODO: Can we avoid this memory copy?
  memcpy(scratch, data + page_offset, n);
  free(data);

  *result = Slice(scratch, n);
  return Status::OK();
}

#ifdef OS_LINUX
size_t NVMRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void NVMRandomAccessFile::Hint(AccessPattern pattern) {

}

Status NVMRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

/*
 * WritableFile implementation
 */

NVMWritableFile::NVMWritableFile(const std::string& fname, nvm_file *fd,
                                                       nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;
  metadata_handle = new NVMPrivateMetadata(fd_); //JAVIER: parameters?

  struct nvm *nvm = dir_->GetNVMApi();
  //TODO: Use the vlun type when this is available
  unsigned int vlun_type = 0;
  //Get block from block manager
  fd_->GetBlock(nvm, vlun_type);

  size_t real_buf_limit = nvm->GetNPagesBlock(vlun_type) * PAGE_SIZE;

  // Account for the metadata to be stored at the end of the file
  buf_limit_ = real_buf_limit - sizeof(struct vblock_close_meta);
  buf_ = (char*)memalign(PAGE_SIZE, real_buf_limit);
  if (!buf_) {
    NVM_FATAL("Could not allocate aligned memory\n");
  }
  mem_ = buf_;
  flush_ = buf_;

  cursize_ = 0;
  curflush_ = 0;
  closed_ = false;

  l0_table = false;

  //Write metadata to the internal buffer to enable recovery before giving it
  //to the upper layers. The responsibility of when to flush that buffer is left
  //to the upper layers.
  struct vblock_recov_meta vblock_meta;
  std::strcpy(vblock_meta.filename, filename_.c_str());
  vblock_meta.pos = fd_->GetNextPos();

  unsigned int meta_size = sizeof(vblock_meta);
  memcpy(mem_, &vblock_meta, meta_size);
  mem_ += meta_size;
  cursize_ += meta_size;
}

NVMWritableFile::~NVMWritableFile() {
  fd_->SetSeqWritableFile(nullptr);

  NVMWritableFile::Close();
}

size_t NVMWritableFile::CalculatePpaOffset(size_t curflush) {
  struct nvm *nvm = dir_->GetNVMApi();
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix (FIXME)

  // For now we assume that all blocks have the same size. When this assumption
  // no longer holds, we would need to iterate vblocks_ in nvm_file, or hold a
  // ppa pointer for each WritableFile.
  // We always flush one page at the time
  return curflush % nppas * PAGE_SIZE;
}

// We try to flush at a page granurality. We need to see how this affects
// writes.
bool NVMWritableFile::Flush(const bool force_flush) {
  struct nvm *nvm = dir_->GetNVMApi();
  size_t flush_len = cursize_ - curflush_;
  size_t ppa_flush_offset = CalculatePpaOffset(curflush_);
  struct vblock_close_meta vblock_meta;
  bool page_aligned = false;

  if (!force_flush && flush_len < PAGE_SIZE) {
    return true;
  }

  if (flush_len == 0) {
    return true;
  }

  assert (curflush_ + flush_len <= buf_limit_);

  if (force_flush) {
    // Append vblock medatada when closing a block.
    if (curflush_ + flush_len == buf_limit_) {
      unsigned int meta_size = sizeof(vblock_meta);
      vblock_meta.flags = VBLOCK_CLOSED;
      vblock_meta.written_bytes = buf_limit_;
      vblock_meta.ppa_bitmap = 0x0; //Use real bad page information
      memcpy(mem_, &vblock_meta, meta_size);
      flush_len += meta_size;
      page_aligned = (flush_len % PAGE_SIZE == 0) ? true : false;
    } else {
      //TODO: Pass on to upper layers to append metadata to RocksDB WAL
      // TODO: Save this partial write structure to the metadata file
      write_pointer_.ppa_offset = ppa_flush_offset + flush_len / PAGE_SIZE;
      write_pointer_.page_offset = flush_len % PAGE_SIZE;
    }
  } else {
    size_t disaligned_data = flush_len % PAGE_SIZE;
    flush_len -= disaligned_data;
    page_aligned = true;
  }

  size_t written_bytes = fd_->FlushBlock(nvm, flush_, ppa_flush_offset,
                                                        flush_len, page_aligned);
  if (written_bytes < flush_len) {
    NVM_DEBUG("unable to write data");
    return false;
  }

  curflush_ += written_bytes;
  flush_ += written_bytes;

  return true;
}

// We preallocate a new block to store future flushes in flash memory. We also
// reset all buffer pointers and sizes; there is no need to maintain old
// buffered data in cache.
bool NVMWritableFile::GetNewBlock() {
  struct nvm *nvm = dir_->GetNVMApi();
  unsigned int vlun_id = 0;
  fd_->GetBlock(nvm, vlun_id);

  //Preserve until we implement double buffering
  assert(cursize_ == buf_limit_);
  assert(curflush_ == buf_limit_ + sizeof(struct vblock_close_meta));
  assert(flush_ == mem_ + sizeof(struct vblock_close_meta));

  size_t new_buf_limit = nvm->GetNPagesBlock(vlun_id) * PAGE_SIZE;

  // No need to reallocate memory and aligned. We reuse the same buffer. If this
  // becomes a security issues, we can zeroized the buffer before reusing it.
  if (new_buf_limit != buf_limit_) {
    buf_limit_ = new_buf_limit;
    free(buf_);

    buf_ = (char*)memalign(PAGE_SIZE, buf_limit_);
    if (!buf_) {
      NVM_FATAL("Could not allocate aligned memory\n");
      return false;
    }
  }

  mem_ = buf_;
  flush_ = buf_;
  cursize_ = 0;
  curflush_ = 0;

  //Write metadata to the internal buffer to enable recovery before giving it
  //to the upper layers. The responsibility of when to flush that buffer is left
  //to the upper layers.
  struct vblock_recov_meta vblock_meta;
  std::strcpy(vblock_meta.filename, filename_.c_str());
  vblock_meta.pos = fd_->GetNextPos();

  unsigned int meta_size = sizeof(vblock_meta);
  memcpy(mem_, &vblock_meta, meta_size);
  mem_ += meta_size;
  cursize_ += meta_size;

  return true;
}

// At this moment we buffer a whole block before syncing in normal operation.
// TODO: We need to differentiate between the log and the memtable to use
// different buffer sizes.
Status NVMWritableFile::Append(const Slice& data) {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  const char* src = data.data();
  size_t left = data.size();
  size_t offset = 0;

  // If the size of the appended data does not fit in one flash block, fill out
  // this block, get a new block and continue writing
  if (cursize_ + left > buf_limit_) {
    size_t fits_in_buf = (buf_limit_ - cursize_);
    memcpy(mem_, src, fits_in_buf);
    mem_ += fits_in_buf;
    cursize_ += fits_in_buf;
    if (Flush(true) == false) {
      return Status::IOError("out of ssd space");
    }

    //This might not be necessary
    if (l0_table) {
      // TODO: Inform the caller to move non-flushed data to a new memtable.
    } else {
      if (!GetNewBlock()) {
        Status::IOError("Cannot allocate new block from flash media\n");
      }
      offset = fits_in_buf;
    }
  }

  memcpy(mem_, src + offset, left - offset);
  mem_ += left - offset;
  cursize_ += left - offset;

  return Status::OK();
}

Status NVMWritableFile::Close() {
  if (closed_) {
    return Status::OK();
  }

  closed_ = true;

  if (Flush(true) == false) {
    return Status::IOError("out of ssd space");
  }

  dir_->nvm_fclose(fd_, "a");

  if (buf_) {
    free(buf_);
  }
  buf_ = nullptr;

  return Status::OK();
}

// We do the caching the backend and sync using direct I/O. Thus, we do not need
// to flush cached data to OS cache.
Status NVMWritableFile::Flush() {
#if 0
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  if (Flush(false) == false) {
    return Status::IOError("out of ssd space");
  }
#endif

  return Status::OK();
}

Status NVMWritableFile::Sync() {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  // We do not force Sync in order to guarantee that we write at a page
  // granurality. Force is reserved for emergency syncing
  if (Flush(false) == false) {
    return Status::IOError("out of ssd space");
  }

  return Status::OK();
}

Status NVMWritableFile::Fsync() {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  if (Flush(false) == false) {
    return Status::IOError("out of ssd space");
  }

  return Status::OK();
}

uint64_t NVMWritableFile::GetFileSize() {
  return fd_->GetSize() + cursize_;
}

Status NVMWritableFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMWritableFile::Allocate(off_t offset, off_t len) {
  return Status::OK();
}

Status NVMWritableFile::RangeSync(off_t offset, off_t nbytes) {
  return Status::OK();
}

size_t NVMWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(fd_, id, max_size);
}

#endif

FilePrivateMetadata* NVMWritableFile::GetMetadataHandle() {
  metadata_handle->UpdateMetadataHandle(fd_);
  return metadata_handle;
}

/*
 * RandomRWFile implementation
 */
NVMRandomRWFile::NVMRandomRWFile(const std::string& fname, nvm_file *f,
                                                         nvm_directory *dir) :
  filename_(fname) {
  fd_ = f;
  dir_ = dir;

  channel = 0;

  NVM_DEBUG("created %s", fname.c_str());
}

NVMRandomRWFile::~NVMRandomRWFile() {
  NVMRandomRWFile::Close();
}

Status NVMRandomRWFile::Write(uint64_t offset, const Slice& data) {
  unsigned int vlun_id = 0;
  if (offset + data.size() > fd_->GetSize()) {
    //this should not be used for sequential writes
    return Status::IOError("Out of bounds");
  }

  struct nvm *nvm = dir_->GetNVMApi();

  //Account for the metadata stored at the beginning of the virtual block.
  uint64_t internal_offset = offset + sizeof(struct vblock_recov_meta);

  unsigned int start_block_id = internal_offset / PAGE_SIZE;
  unsigned int end_block_id = (internal_offset + data.size()) / PAGE_SIZE;
  unsigned int block_offset = internal_offset % PAGE_SIZE;
  unsigned int crt_written = 0;

  while(start_block_id != end_block_id) {
    char *crt_data;
    //TODO get proper number of pages
    size_t npages = 128;
    size_t size = PAGE_SIZE * npages;
    size_t size_to_write;

    crt_data = (char *)memalign(PAGE_SIZE, size);
    if (!crt_data) {
      NVM_FATAL("Could not allocate aligned memory\n");
      return Status::IOError("could not allocate aligned memory");
    }

    if(fd_->ReadBlock(nvm, start_block_id, 0, 0, crt_data, size) != size) {
      free(crt_data);
      return Status::IOError("could not read");
    }

    fd_->ReplaceBlock(nvm, vlun_id, start_block_id);
    size_to_write = data.size() - crt_written;

    if(size_to_write > size - block_offset) {
      size_to_write = size - block_offset;
    }

    memcpy(crt_data + block_offset, data.data() + crt_written, size_to_write);
    block_offset = 0;
    crt_written += size_to_write;

    if(fd_->FlushBlock(nvm, crt_data, 0, size, true) != size) {
      free(crt_data);
      return Status::IOError("could not read");
    }

    free(crt_data);
    ++start_block_id;
}

return Status::OK();
}

//keep synchronized with NVMRandomAccessFile::Read
Status NVMRandomRWFile::Read(uint64_t offset, size_t n, Slice* result,
                                                          char* scratch) const {
  if (offset >= fd_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  if (offset >= fd_->GetSize()) {
    n = fd_->GetSize() - offset;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  struct nvm *nvm = dir_->GetNVMApi();

  //Account for the metadata stored at the beginning of the virtual block.
  uint64_t internal_offset = offset + sizeof(struct vblock_recov_meta);

  unsigned int ppa_offset = internal_offset / PAGE_SIZE;
  unsigned int page_offset = internal_offset % PAGE_SIZE;

  size_t data_len = (((n / PAGE_SIZE) + 1) * PAGE_SIZE);
  char *data = (char*)memalign(PAGE_SIZE, data_len);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  if (fd_->Read(nvm, ppa_offset, data, data_len) != data_len) {
    free(data);
    return Status::IOError("Unable to read\n");
  }

  //TODO: Can we avoid this memory copy?
  memcpy(scratch, data + page_offset, n);
  free(data);

  NVM_DEBUG("read %lu bytes", n);

  *result = Slice(scratch, n);
  return Status::OK();
}

Status NVMRandomRWFile::Close() {
  dir_->nvm_fclose(fd_, "w");

  return Status::OK();
}

Status NVMRandomRWFile::Sync() {
  return Status::OK();
}

Status NVMRandomRWFile::Fsync() {
  return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMRandomRWFile::Allocate(off_t offset, off_t len) {
  return Status::OK();
}
#endif

NVMDirectory::NVMDirectory(nvm_directory *fd) {
  fd_ = fd;
}

NVMDirectory::~NVMDirectory() {

}

Status NVMDirectory::Fsync() {
  return Status::OK();
}

}

#endif
