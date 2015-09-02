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

  size_ = 0;
  fd_ = fd;

  last_modified = time(nullptr);
  pages.clear();

  opened_for_write = false;

  parent = _parent;

  seq_writable_file = nullptr;
  vblock_ = nullptr;

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

  //delete all nvm pages in the vector
  pages.clear();

  list_node *temp = names;

  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete[] (char *)temp->GetData();
    delete temp;

    temp = temp1;
  }

  struct nvm *nvm = parent->GetNVMApi();
  PutBlock(nvm);

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

// The caller must ensure that data i aligned to PAGE_SIZE.
// Note that this function reads entire pages. Thus the caller must take care of
// in-page offsets to only return the requested bytes.
size_t nvm_file::ReadPage(struct nvm *nvm, size_t ppa_offset, char *data,
                                                            size_t data_len) {
  size_t current_ppa = vblock_->bppa + ppa_offset;
  // size_t last_ppas = vblock_->nppas;
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix until we use
                                         //the right structure
  size_t left = data_len;
  unsigned long max_bytes_per_read = nvm->max_pages_in_io * 4096;
  // unsigned long max_bytes_per_read = 4096;
  unsigned long bytes_per_read;
  uint8_t pages_per_read;
  char *read_iter = data;

  assert(data_len <= nppas * 4096); //TODO: Recover? Who takes responsibility?
  assert((data_len % 4096) == 0);
  assert(current_ppa <= vblock_->bppa + nppas);

  while (left > 0) {
retry:
    bytes_per_read = (left > max_bytes_per_read) ? max_bytes_per_read : left;
    pages_per_read = bytes_per_read / 4096;

    if ((unsigned)pread(fd_, read_iter, bytes_per_read, current_ppa * 4096)
                                                          != bytes_per_read) {
      if (errno == EINTR) {
        goto retry;
      }
      return -1;
    }

    read_iter += bytes_per_read;
    left -= bytes_per_read;
    current_ppa += pages_per_read;
  }

  IOSTATS_ADD(bytes_read, data_len);
  return data_len - left;
}

//TODO: Javier: This allocation should happen in the background so that we do
//not need to wait for it. This should probably be done when a new memtable is
//allocated -> space in the disk is allocated (reserved) when a new potential
//sstable is created.
void nvm_file::GetBlock(struct nvm *nvm, unsigned int vlun_id) {
  if (vblock_ != nullptr) {
    goto out;
  }

  //TODO: Make this better: mmap memory into device??
  vblock_ = (struct vblock*)malloc(sizeof(struct vblock));
  if (!vblock_) {
    NVM_FATAL("Could not allocate memory\n");
  }

  if (!nvm->GetBlock(vlun_id, vblock_)) {
    NVM_FATAL("could not get a new block - ssd out of space\n");
  }

  NVM_DEBUG("Get block. id: %lu\n", vblock_->id);

out:
  write_ppa_ = vblock_->bppa;
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
//
// TODO: Keep a pointer to write block to flash as it is being filled up. This
// would allow to write in smaller chunks (e.g., 1MB). This is what write_ppa_
// is there for. At the moment we write in a block basis to avoid concurrency
// issues.
//
// Note that data_len is the real length of the data to be flushed to the flash
// block. FlushBlock takes care of working on PAGE_SIZE chunks
// TODO: Make a function to flush at a page level to allow partial writes but
// still respect PAGE_SIZE as write granurality
size_t nvm_file::FlushBlock(struct nvm *nvm, void *data,
                                                      size_t data_len) {
  // size_t nppas = vblock_->nppas;
  size_t nppas = nvm->GetNPagesBlock(0); //This is a momentary fix (FIXME)
  // size_t current_ppa = write_ppa_;   //This will allow partial block writes
  size_t current_ppa = vblock_->bppa;
  unsigned long max_bytes_per_write = nvm->max_pages_in_io * 4096;
  unsigned long bytes_per_write;
  uint8_t pages_per_write;
  uint8_t allocate_aligned_buf = 0;

  //Always write at a page granurality
  uint8_t x = (data_len % 4096 == 0) ? 0 : 1;
  size_t write_len = (((data_len / 4096) + x) * 4096);
  unsigned long left = write_len;
  char *data_aligned;

  assert(write_len <= nppas * 4096);

  NVM_DEBUG("writing %lu bytes - nppages : %lu\n", write_len, nppas);
  //TODO: Generalize page size

  if (write_len < nppas * 4096) {
    NVM_DEBUG("Writing block. Not using %lu bytes\n", (nppas * 4096) - write_len);
  }

  /* Verify that data is aligned although it should already be aligned */
  if (UNLIKELY(((uintptr_t)data % 4096) != 0)) {
    NVM_DEBUG("Aligning data\n");
    data_aligned = (char*)memalign(4096, write_len);
    if (!data_aligned) {
      NVM_FATAL("Cannot allocate aligned memory\n");
      return 0;
    }

    memcpy(data_aligned, data, write_len);
    allocate_aligned_buf = 1;
  } else {
    data_aligned = (char*)data;
  }

  //TODO: Use libaio instead of pread/pwrite
  while (left > 0) {
    //write_len is multiple of PAGE_SIZE
    bytes_per_write = (left > max_bytes_per_write) ? max_bytes_per_write : left;
    pages_per_write = bytes_per_write / 4096;

    NVM_DEBUG("Writing %lu bytes in ppa:%lu\n", bytes_per_write, current_ppa);
    NVM_DEBUG("FIRST: %c\n", data_aligned[0]);
    if ((unsigned)pwrite(fd_, data_aligned, bytes_per_write,
                                     current_ppa * 4096) != bytes_per_write) {
      //TODO: See if we can recover. Use another ppa + mark bad page in bitmap?
      NVM_ERROR("ERROR: Page no written\n");
      return 0;
    }

    data_aligned += bytes_per_write;
    left -= bytes_per_write;
    current_ppa += pages_per_write;
  }

  if (allocate_aligned_buf)
    free(data_aligned);

  // write_ppa_ = current_ppa;

  pthread_mutex_lock(&page_update_mtx);
  size_ += data_len - left;
  NVM_DEBUG("NEW SIZE: %lu\n", size_);
  pthread_mutex_unlock(&page_update_mtx);

  UpdateFileModificationTime();
  IOSTATS_ADD(bytes_written, write_len);

  return data_len - left;
}

bool nvm_file::HasBlock() {
  return (vblock_ == nullptr) ? false : true;
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

  NVM_DEBUG("created %s", fname.c_str());
}

NVMSequentialFile::~NVMSequentialFile() {
  NVM_DEBUG("Close sequential file\n");
  dir_->nvm_fclose(fd_, "r");
}

//TODO: Important Cache last read page to avoid small reads submitting extra IOs
Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  unsigned int ppa_offset = read_pointer_ / 4096;
  unsigned int page_offset = read_pointer_ % 4096;

  if (read_pointer_ >= fd_->GetSize()) {
    n = fd_->GetSize() - read_pointer_;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  struct nvm *nvm = dir_->GetNVMApi();

  //Always read at a page granurality
  uint8_t x = (n % 4096 == 0) ? 0 : 1;
  size_t data_len = (((n / 4096) + x) * 4096);

  char *data = (char*)memalign(4096, data_len);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  if (fd_->ReadPage(nvm, ppa_offset, data, data_len) != data_len) {
    return Status::IOError("Unable to read\n");
  }

  //TODO: Can we avoid this memory copy?
  memcpy(scratch, data + page_offset, n);
  free(data);

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

  read_pointer_ = 0;

  NVM_DEBUG("created %s", fname.c_str());
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

  if (read_pointer_ >= fd_->GetSize()) {
    n = fd_->GetSize() - read_pointer_;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  struct nvm *nvm = dir_->GetNVMApi();

  unsigned int ppa_offset = offset / 4096;
  unsigned int page_offset = offset % 4096;

  size_t data_len = (((n / 4096) + 1) * 4096);

  char *data = (char*)memalign(4096, data_len);
  if (!data) {
    NVM_FATAL("Cannot allocate aligned memory\n");
    return Status::Corruption("Cannot allocate aligned memory''");
  }

  if (fd_->ReadPage(nvm, ppa_offset, data, data_len) != data_len) {
    return Status::IOError("Unable to read\n");
  }

  //TODO: Can we avoid this memory copy?
  memcpy(scratch, data + page_offset, n);
  free(data);

  //TODO: JAVIER: Look into this
  // read_pointer_ += n;

  NVM_DEBUG("read %lu bytes", n);

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

  struct nvm *nvm = dir_->GetNVMApi();
  unsigned int vlun_id = 0;
  //TODO: Javier: get list of vluns at initialization and ask for a vblock from
  //the right vlun at runtime. We still need to define how the BM and the FTL
  //will agree on which vlun is going to be used. To start with we have a single
  //vlun (vlun 0)
  fd_->GetBlock(nvm, vlun_id);

  // TODO: Generalize page size
  buf_limit_ = nvm->GetNPagesBlock(vlun_id) * 4096;
  buf_ = (char*)memalign(4096, buf_limit_);
  if (!buf_) {
    NVM_FATAL("Could not allocate aligned memory\n");
  }
  dst_ = buf_;
  flush_ = buf_;

  cursize_ = 0;
  flushsize_ = 0;
  closed_ = false;

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
  size_t flush_len = cursize_ - flushsize_;

  if (flush_len == 0) {
    return true;
  }

  //TODO: This constrain needs to be relaxed and recover when it happens
  assert(cursize_ <= buf_limit_);

  NVM_DEBUG("cursize: %lu, buf_limit_: %lu, flushsize_: %lu\n",
                                                cursize_, buf_limit_, flushsize_);

  size_t written_bytes = fd_->FlushBlock(nvm, flush_, flush_len);
  if (written_bytes != flush_len) {
    NVM_DEBUG("unable to write data");
    return false;
  }

  flushsize_ += written_bytes;
  flush_ += written_bytes;

  return true;
}

Status NVMWritableFile::Append(const Slice& data) {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  const char* src = data.data();
  size_t left = data.size();
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

Status NVMWritableFile::Flush() {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  if (Flush(true) == false) {
    return Status::IOError("out of ssd space");
  }

  return Status::OK();
}

Status NVMWritableFile::Sync() {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

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
#if 0
  const char* src = data.data();
  size_t left = data.size();

  if (offset > fd_->GetSize()) {
    NVM_DEBUG("offset is out of bounds");
    return Status::IOError("offset is out of bounds");
  }

  unsigned int ppa_offset = offset / 4096;
  unsigned int page_offset = offset % 4096;

#endif
  //TODO: IMPLEMENT

  return Status::OK();
}

Status NVMRandomRWFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const {
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

#if 0
  size_t len = n;
  // size_t l;
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

    // l = fd_->ReadPage(crt_page, channel, nvm_api, data);

    // if (len > l - page_pointer) {
      // size_to_copy = l - page_pointer;
    // } else {
      // size_to_copy = len;
    // }

    memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

    len -= size_to_copy;
    scratch_offset += size_to_copy;

    crt_page = fd_->GetNVMPage(++page_idx);
    page_pointer = 0;
  }

  free(data);

  NVM_DEBUG("read %lu bytes", n);

  *result = Slice(scratch, n);
#endif
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
