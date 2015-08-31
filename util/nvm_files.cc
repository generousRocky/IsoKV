#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"
#include "malloc.h"

namespace rocksdb {

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

  size = 0;

  fd_ = fd;

  last_modified = time(nullptr);

  pages.clear();

  opened_for_write = false;

  parent = _parent;

  seq_writable_file = nullptr;

  pthread_mutexattr_init(&page_update_mtx_attr);
  pthread_mutexattr_settype(&page_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);

  pthread_mutex_init(&page_update_mtx, &page_update_mtx_attr);
  pthread_mutex_init(&meta_mtx, &page_update_mtx_attr);
  pthread_mutex_init(&file_lock, &page_update_mtx_attr);
}

nvm_file::~nvm_file() {
  if (seq_writable_file) {
    //flush any existing buffers
    seq_writable_file->Close();
    seq_writable_file = nullptr;
  }

  //delete all nvm pages in the vector
  pages.clear();

  list_node *temp = names;

  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete[] (char *)temp->GetData();
    delete temp;

    temp = temp1;
  }

  pthread_mutexattr_destroy(&page_update_mtx_attr);
  pthread_mutex_destroy(&page_update_mtx);
  pthread_mutex_destroy(&meta_mtx);
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

struct nvm_page *nvm_file::RequestPage(nvm *nvm_api, const unsigned long lun_id, const unsigned long block_id, const unsigned long page_id) {

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

  size = 0;

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 3");
    }

    if (readIn >= '0' && readIn <= '9') {
      size = size * 10 + readIn - '0';
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
  ret = size;
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

  size = 0;

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

// data must be aligned
// TODO: do checks?
size_t nvm_file::Read(struct nvm *nvm, size_t ppa_offset, char *data,
                                                            size_t data_len) {
  assert(ppa_offset % 4096 == 0);
  assert(data_len % 4096 == 0);

  size_t current_ppa = ppa_offset;
  // size_t last_ppas = vblock_->nppas;
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix
  size_t left = data_len;
  uint8_t pages_per_read = nvm->max_pages_in_io;
  unsigned long bytes_per_read = pages_per_read * 4096; //TODO: Generalize page size

  assert(data_len <= nppas * 4096);

  while (left > 0) {
    if (left < bytes_per_read) {
      pages_per_read = (left / 4096);
      if (left % 4096 != 0) {
        pages_per_read++;
      }
      bytes_per_read = pages_per_read * 4096;
    }

retry:
    if ((unsigned)pread(fd_, data, bytes_per_read, ppa_offset) !=
                                                              bytes_per_read) {
      if (errno == EINTR) {
        goto retry;
      }
      return -1;
    }

    data += bytes_per_read;
    left -= bytes_per_read;
    current_ppa += pages_per_read;

    IOSTATS_ADD(bytes_read, bytes_per_read);
  }

  return data_len - left;
}

size_t nvm_file::ReadPage(const nvm_page *page, const unsigned long channel,
                                              struct nvm *nvm_api, void *data) {
  unsigned long offset;

  unsigned long page_size = page->sizes[channel];
  unsigned long block_size = nvm_api->luns[page->lun_id].nr_pages_per_blk * page_size;
  unsigned long lun_size = nvm_api->luns[page->lun_id].nr_blocks * block_size;

  offset = page->lun_id * lun_size + page->block_id * block_size + page->id * page_size;

  NVM_DEBUG("reading %lu bytes at %lu", page_size, offset);

retry:

  if ((unsigned)pread(fd_, data, page_size, offset) != page_size) {
    if (errno == EINTR) {
      goto retry;
    }
    return -1;
  }

  IOSTATS_ADD(bytes_read, page_size);

  return page_size;
}

//TODO: Javier: This allocation should happen in the background so that we do
//not need to wait for it. This should probably be done when a new memtable is
//allocated -> space in the disk is allocated (reserved) when a new potential
//sstable is created.
void nvm_file::GetBlock(struct nvm *nvm, unsigned int vlun_id) {
  //TODO: Make this better: mmap memory into device??
  assert(vblock_ == nullptr);
  vblock_ = (struct vblock*)malloc(sizeof(struct vblock));
  if (!vblock_) {
    NVM_FATAL("Could not allocate memory\n");
  }

  if (!nvm->GetBlock(vlun_id, vblock_)) {
    NVM_FATAL("could not get a new block - ssd out of space\n");
  }
}

void nvm_file::PutBlock(struct nvm *nvm) {
  assert(vblock_ != nullptr);
  if (!nvm->PutBlock(vblock_)) {
    //TODO: Can we recover from this?
    NVM_FATAL("could not return block to BM\n");
    return;
  }

  FreeBlock();
}

void nvm_file::FreeBlock() {
  NVM_DEBUG("Freeing block\n");
  if (vblock_ != nullptr) {
    free(vblock_);
  }

  vblock_ = nullptr;
}

// For the moment we assume that all pages in a block are good pages. In the
// future we would have to check ppa_bitmap and come back to the memtable to
// move the KV pairs that do not fit in the block assigned to it to create the
// sstable. This will require to send a ppa_list down to LightNVM to enable
// multipage writes - at the moment multipage assumes sequential ppas
size_t nvm_file::WriteBlock(struct nvm *nvm, void *data,
                                              const unsigned long data_len) {
  char *data_aligned;
  size_t current_ppa = vblock_->bppa;
  // size_t nppas = vblock_->nppas;
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix (FIXME)
  unsigned long left = data_len;
  unsigned long max_bytes_per_write = nvm->max_pages_in_io * 4096;
  unsigned long bytes_per_write;
  uint8_t pages_per_write;
  uint8_t allocate_aligned_buf = 0;

  //TODO: Generalize page size
  assert(data_len <= nppas * 4096);
  assert((data_len % 4096) == 0); //TODO: support data_len not % 4096? Padding?

  if (data_len < nppas * 4096) {
    NVM_DEBUG("Writing block. Not using %lu bytes\n", (nppas * 4096) - data_len);
  }

  /* Verify that data is aligned although it should already be aligned */
  if (UNLIKELY(((uintptr_t)data % 4096) != 0)) {
    NVM_DEBUG("Aligning data\n");
    data_aligned = (char*)memalign(4096, data_len);
    if (!data_aligned) {
      NVM_FATAL("Cannot allocate aligned memory\n");
      return -1;
    }

    memcpy(data_aligned, data, data_len);
    allocate_aligned_buf = 1;
  } else {
    data_aligned = (char*)data;
  }

  //TODO: Use libaio instead of pread/pwrite
  while (left > 0) {
    //data_len has been asserted to be multiple of PAGE_SIZE
    bytes_per_write = (left > max_bytes_per_write) ? max_bytes_per_write : left;
    pages_per_write = bytes_per_write / 4096;

    NVM_DEBUG("Writing %lu bytes in ppa:%lu\n", bytes_per_write, current_ppa);
    if ((unsigned)pwrite(fd_, data_aligned, bytes_per_write,
                                     current_ppa * 4096) != bytes_per_write) {
      //TODO: See if we can recover. Use another ppa + mark bad page in bitmap?
      NVM_ERROR("ERROR: Page no written\n");
      return -1;
    }

    data_aligned += bytes_per_write;
    left -= bytes_per_write;
    current_ppa += pages_per_write;
  }

  if (allocate_aligned_buf)
    free(data_aligned);

  return data_len - left;
}

// size_t nvm_file::ReadBlock() {

// }

NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *fd,
                                                        nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;

  struct nvm *nvm = dir_->GetNVMApi();
  unsigned int vlun_id = 0;
  fd_->GetBlock(nvm, vlun_id);

  // TODO: Generalize page size

  //Javier: This will probably go
  // file_pointer = 0;

  // crt_page_idx = 0;
  // crt_page = file_->GetNVMPage(0);
  // page_pointer = 0;
  // channel = 0;

  NVM_DEBUG("created %s", fname.c_str());
}

NVMSequentialFile::~NVMSequentialFile() {
  NVM_DEBUG("Close sequential file\n");
  dir_->nvm_fclose(fd_, "r");
}

void NVMSequentialFile::SeekPage(const unsigned long offset) {

#if 0 //Old implementation
  if (crt_page == nullptr) {
    crt_page = file_->GetNVMPage(crt_page_idx);

    if (crt_page == nullptr) {
      return;
    }
  }
  page_pointer += offset;
  crt_page_idx += (unsigned long)(page_pointer / crt_page->sizes[channel]);
  crt_page = file_->GetNVMPage(crt_page_idx);
  page_pointer %= crt_page->sizes[channel];
#endif
}

Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  // struct nvm *nvm = dir_->GetNVMApi();
  //JAVIER: YOU ARE HERE!
  // size_t data_len = (n + 4096 - ((n % 4096) * 4096));

  NVM_DEBUG("File pointer is %lu, n is %lu, \n", file_pointer, n);

  //TODO: Recover in upper layers when writing out of bounds of current block
  // if (n > buf_limit_) {
    // NVM_FATAL("TODO::: Writing more than block size - need to recover\n");
  // }

  // char *data = (char*)memalign(4096, );
  // if (!data) {
    // NVM_FATAL("Cannot allocate aligned memory\n");
    // return Status::Corruption("Cannot allocate aligned memory''");
  // }

  // if (fd_->Read(nvm, 0, data));


#if 0 //old implementation
  if (file_pointer + n > file_->GetSize()) {
    n = file_->GetSize() - file_pointer;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  SeekPage(0);

  if (crt_page == nullptr) {
    NVM_DEBUG("crt_page is null");

    *result = Slice(scratch, 0);
    return Status::OK();
  }

  size_t len = n;
  size_t l;
  size_t scratch_offset = 0;
  size_t size_to_copy;

  char *data = (char*)memalign(4096, crt_page->sizes[channel]);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  while (len > 0) {
    l = file_->ReadPage(crt_page, channel, nvm_api, data);

    if (len > l - page_pointer) {
      size_to_copy = l - page_pointer;
    } else {
      size_to_copy = len;
    }

    NVM_DEBUG("copy %lu to scratch from offset %lu", size_to_copy, scratch_offset);

    memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

    len -= size_to_copy;
    scratch_offset += size_to_copy;

    SeekPage(size_to_copy);

    NVM_DEBUG("page pointer becomes %lu and page is %p", page_pointer, crt_page);
  }

  free(data);

  file_pointer += n;

  NVM_DEBUG("creating slice with %lu bytes", n);

  *result = Slice(scratch, n);
#endif

  return Status::OK();
}

//n is unsigned -> skip is only allowed forward
Status NVMSequentialFile::Skip(uint64_t n) {
  if (n == 0) {
    return Status::OK();
  }

  if (file_pointer + n > fd_->GetSize()) {
    return Status::IOError(filename_, "EINVAL file pointer goes out of bounds");
  }

  SeekPage(n);

  file_pointer += n;

  NVM_DEBUG("SEEKED %lu forward; file pointer is %lu, page pointer is %lu", n, file_pointer, page_pointer);

  return Status::OK();
}

Status NVMSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, nvm_file *f, nvm_directory *_dir) :
  filename_(fname) {
  file_ = f;

  dir = _dir;

  channel = 0;

  nvm_api = dir->GetNVMApi();

  NVM_DEBUG("created %s", fname.c_str());
}

NVMRandomAccessFile::~NVMRandomAccessFile() {
  dir->nvm_fclose(file_, "r");
}

struct nvm_page *NVMRandomAccessFile::SeekPage(const unsigned long offset, unsigned long *page_pointer, unsigned long *page_idx) const {
  struct nvm_page *crt_page = file_->GetNVMPage(0);

  *page_pointer = offset;

  *page_idx = (unsigned long)(*page_pointer / crt_page->sizes[channel]);

  *page_pointer %= crt_page->sizes[channel];

  crt_page = file_->GetNVMPage(*page_idx);

  return crt_page;
}

Status NVMRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                                        char* scratch) const {
  unsigned long page_pointer;

  if (offset >= file_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");

    *result = Slice(scratch, 0);
    return Status::OK();
  }

  if (offset + n > file_->GetSize()) {
    n = file_->GetSize() - offset;
  }

  if (n <= 0) {
    NVM_DEBUG("n is <= 0");

    *result = Slice(scratch, 0);
    return Status::OK();
  }

  size_t len = n;
  size_t l;
  size_t scratch_offset = 0;
  size_t size_to_copy;

  char *data;

  unsigned long page_idx;

  struct nvm_page *crt_page = SeekPage(offset, &page_pointer, &page_idx);

  if (crt_page == nullptr) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  data = (char*)memalign(4096, crt_page->sizes[channel]);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  while (len > 0) {
    if (crt_page == nullptr) {
      n -= len;
      break;
    }

    l = file_->ReadPage(crt_page, channel, nvm_api, data);

    if (len > l - page_pointer) {
      size_to_copy = l - page_pointer;
    } else {
      size_to_copy = len;
    }

    memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

    len -= size_to_copy;
    scratch_offset += size_to_copy;

    crt_page = file_->GetNVMPage(++page_idx);
    page_pointer = 0;
  }

  free(data);

  NVM_DEBUG("read %lu bytes", n);

  *result = Slice(scratch, n);

  return Status::OK();
}

#ifdef OS_LINUX
size_t NVMRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(file_, id, max_size);
}
#endif

void NVMRandomAccessFile::Hint(AccessPattern pattern) {

}

Status NVMRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

NVMWritableFile::NVMWritableFile(const std::string& fname, nvm_file *fd,
                                                       nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;

  //TODO: Javier: get list of vluns at initialization and ask for a vblock from
  //the right vlun at runtime. We still need to define how the BM and the FTL
  //will agree on which vlun is going to be used. To start with we have a single
  //vlun (vlun 0)
  // Allocate a new flash block in LightNVM's block manager
  struct nvm *nvm = dir_->GetNVMApi();
  unsigned int vlun_id = 0;
  fd_->GetBlock(nvm, vlun_id);

  // TODO: Generalize page size
  // Allocate memory to buffer the contents of flash block in memory. Since we
  // are using direct IO memory needs to be aligned to the OS page size
  buf_limit_ = nvm->GetNPagesBlock(vlun_id) * 4096;
  buf_ = (char*)memalign(4096, buf_limit_);
  if (!buf_) {
    NVM_FATAL("Could not allocate aligned memory\n");
  }
  dst_ = buf_;

  //Javier: This will all go
  channel = 0;
  last_page = nullptr;
  last_page_idx = 0;
  closed = false;

  NVM_DEBUG("created %s at %p", fname.c_str(), this);
}

NVMWritableFile::~NVMWritableFile() {
  fd_->SetSeqWritableFile(nullptr);

  NVMWritableFile::Close();
}

// WritableFile is mainly used to flush memtables and make them persistent
// (sstables). We match them the size of the memtable to the size of the vblock,
// thus we write one block at the time.
// TODO: Return value to the upper layers informing if some data has not been
// flushed and needs to be allocated in a different memtable.
bool NVMWritableFile::Flush(const bool closing) {
  struct nvm *nvm = dir_->GetNVMApi();
  long written_bytes;

  NVM_DEBUG("cursize: %lu, buf_limit_: %lu\n", cursize_, buf_limit_);

  //TODO: This constrain needs to be relaxed and recover when it happens
  assert(cursize_ <= buf_limit_);

  written_bytes = fd_->WriteBlock(nvm, buf_, cursize_);
  if (written_bytes < 0) {
    NVM_DEBUG("unable to write data");
    return false;
  }

  cursize_ += written_bytes;
  return true;
}

Status NVMWritableFile::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  // size_t offset = 0;
  NVM_DEBUG("Appending slice %lu bytes to %p", left, this);

  assert(buf_ <= dst_);
  assert(dst_ <= buf_ + buf_limit_);

  memcpy(dst_, src, left);
  dst_ += left;
  cursize_ += left;
  if (cursize_ > buf_limit_) {
    //TODO: What is in the NVM_DEBUG message. This should not  happen, since the
    //memtable will include a maximun size, which must alwast be <= buf_limit_
    //(which at the same time represents the actual vblock size)
    NVM_DEBUG("WE NEED TO INFORM THE UPPER LAYERS THAT THE BLOCK IS FULL!!\n");
    NVM_DEBUG("cursize_: %lu, buf_limit_:%lu\n", cursize_, buf_limit_);
    if (Flush(true) == false) {
      return Status::IOError("out of ssd space");
    }
  }

  return Status::OK();
}

Status NVMWritableFile::Close() {
  if (closed) {
    return Status::OK();
  }

  closed = true;

  Flush(true);

  struct nvm *nvm = dir_->GetNVMApi();
  fd_->PutBlock(nvm);
  dir_->nvm_fclose(fd_, "a");

  if (buf_) {
    free(buf_);
  }
  buf_ = nullptr;

  return Status::OK();
}

Status NVMWritableFile::Flush() {
  if (closed) {
    return Status::IOError("file has been closed");
  }

  if (Flush(true) == false) {
    return Status::IOError("out of ssd space");
  }

  return Status::OK();
}

Status NVMWritableFile::Sync() {
  if (closed) {
    return Status::IOError("file has been closed");
  }

  if (Flush(false) == false) {
    return Status::IOError("out of ssd space");
  }

  return Status::OK();
}

Status NVMWritableFile::Fsync() {
  if (closed) {
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


NVMRandomRWFile::NVMRandomRWFile(const std::string& fname, nvm_file *_fd, nvm_directory *_dir) :
  filename_(fname) {
  fd_ = _fd;
  dir = _dir;

  channel = 0;

  nvm_api = dir->GetNVMApi();

  NVM_DEBUG("created %s", fname.c_str());
}

NVMRandomRWFile::~NVMRandomRWFile() {
  NVMRandomRWFile::Close();
}

struct nvm_page *NVMRandomRWFile::SeekPage(const unsigned long offset, unsigned long *page_pointer, unsigned long *page_idx) const {
  struct nvm_page *crt_page = fd_->GetNVMPage(0);

  if (crt_page == nullptr) {
    return nullptr;
  }

  *page_pointer = offset;

  *page_idx = (unsigned long)(*page_pointer / crt_page->sizes[channel]);

  *page_pointer %= crt_page->sizes[channel];

  crt_page = fd_->GetNVMPage(*page_idx);

  if (crt_page == nullptr && offset == fd_->GetSize()) {
    if (fd_->ClaimNewPage(dir->GetNVMApi()) == false) {
      NVM_FATAL("Out of SSD space");
    }
  }

  return crt_page;
}

//TODO: REIMPLEMENT
Status NVMRandomRWFile::Write(uint64_t offset, const Slice& data) {
  unsigned long page_pointer;
  unsigned long page_idx;

  size_t i = 0;

  nvm_page *new_pg = nullptr;

  struct nvm_page *crt_page = nullptr;

  char *crt_data = nullptr;

  const char* src = data.data();

  size_t left = data.size();

  if (offset > fd_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");

    return Status::IOError("offset is out of bounds");
  }

  bool page_just_claimed;

  while (left > 0) {
    page_just_claimed = false;

    if (crt_page == nullptr) {
      crt_page = SeekPage(offset, &page_pointer, &page_idx);

      if (crt_page == nullptr) {
        if (fd_->ClaimNewPage(dir->GetNVMApi()) == false) {
          NVM_FATAL("Out of SSD space");
        }

        crt_page = SeekPage(offset, &page_pointer, &page_idx);

        page_just_claimed = true;
      }

      crt_data = (char*)memalign(4096, crt_page->sizes[channel]);
      if (!crt_data) {
        NVM_FATAL("Could not allocate aligned memory");
      }
    } else {
      crt_page = fd_->GetNVMPage(++page_idx);

      if (crt_page == nullptr) {
        if (fd_->ClaimNewPage(dir->GetNVMApi()) == false) {
          NVM_FATAL("Out of SSD space");
        }

        crt_page = fd_->GetNVMPage(page_idx);

        if (crt_page == nullptr) {
          NVM_FATAL("Out of SSD space");
        }

        page_just_claimed = true;
      }

      page_pointer = 0;
    }

    NVM_DEBUG("page pointer is %lu, page size is %u", page_pointer, crt_page->sizes[channel]);

    if (page_just_claimed == false) {
      fd_->ReadPage(crt_page, channel, nvm_api, crt_data);
    }

    for (i = 0; i < crt_page->sizes[channel] - page_pointer && i < left; ++i) {
      crt_data[page_pointer + i] = src[i];
    }

    left -= i;

    if (page_just_claimed) {
      new_pg = crt_page;
    } else {
      new_pg = fd_->RequestPage(nvm_api);

      if (new_pg == nullptr) {
        free(crt_data);

        return Status::IOError("request new page returned");
      }

      fd_->SetPage(page_idx, new_pg);
    }

    struct nvm_page *wrote_pg = new_pg;

    // if (fd_->WritePage(wrote_pg, channel, nvm_api, crt_data,
        // wrote_pg->sizes[channel], page_pointer, i) != wrote_pg->sizes[channel]) {
      // NVM_FATAL("write error");
    // }

    if (wrote_pg != new_pg) {
      fd_->SetPage(page_idx, wrote_pg);
    }

    if (page_just_claimed == false) {
      fd_->ReclaimPage(nvm_api, crt_page);
    }
  }

  if (crt_data) {
    free(crt_data);
  }

  return Status::OK();
}

Status NVMRandomRWFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
  unsigned long page_pointer;

  if (offset > fd_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");

    *result = Slice(scratch, 0);
    return Status::OK();
  }

  if (offset + n > fd_->GetSize()) {
    n = fd_->GetSize() - offset;
  }

  if (n <= 0) {
    NVM_DEBUG("n is <= 0");

    *result = Slice(scratch, 0);
    return Status::OK();
  }

  size_t len = n;
  size_t l;
  size_t scratch_offset = 0;
  size_t size_to_copy;

  char *data;

  unsigned long page_idx;

  struct nvm_page *crt_page = SeekPage(offset, &page_pointer, &page_idx);

  if (crt_page == nullptr) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  data = (char*)memalign(4096, crt_page->sizes[channel]);
  if (!data) {
    return Status::Corruption("Could not allocate aligned memory");
  }

  while (len > 0) {
    if (crt_page == nullptr) {
      n -= len;
      break;
    }

    l = fd_->ReadPage(crt_page, channel, nvm_api, data);

    if (len > l - page_pointer) {
      size_to_copy = l - page_pointer;
    } else {
      size_to_copy = len;
    }

    memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

    len -= size_to_copy;
    scratch_offset += size_to_copy;

    crt_page = fd_->GetNVMPage(++page_idx);
    page_pointer = 0;
  }

  free(data);

  NVM_DEBUG("read %lu bytes", n);

  *result = Slice(scratch, n);

  return Status::OK();
}

Status NVMRandomRWFile::Close() {
  dir->nvm_fclose(fd_, "w");

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
