#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"
#include <liblightnvm.h>


list_node::list_node(void *_data) {
  data = _data;

  next = nullptr;
  prev = nullptr;
}

list_node::~list_node() {
}

list_node *list_node::GetNext() {
  return next;
}

list_node *list_node::GetPrev() {
  return prev;
}

void *list_node::GetData() {
  return data;
}

void *list_node::SetData(void *_data) {
  void *ret = data;

  data = _data;
  return ret;
}

void *list_node::SetNext(list_node *_next) {
  void *ret = next;

  next = _next;
  return ret;
}

void *list_node::SetPrev(list_node *_prev) {
  void *ret = prev;

  prev = _prev;
  return ret;
}

namespace rocksdb {

/*
 * DFlashEntry implmementation
 */
dflash_entry::dflash_entry(const dflash_entry_type _type, void *_data) {
    type = _type;
    data = _data;
}

dflash_entry::~dflash_entry() {
  switch (type) {
    case FileEntry:
      delete (dflash_file *)data;
      break;
    case DirectoryEntry:
      delete (dflash_dir *)data;
      break;
    default:
      DFLASH_FATAL("unknown entry type!!");
      break;
    }
}

void *dflash_entry::GetData() {
    return data;
}

dflash_entry_type dflash_entry::GetType() {
    return type;
}

/*
 * DFlashFile implementation
 */
dflash_file::dflash_file(const char *_name, const int fd, const int beam,
                         dflash_dir *_parent) {
  int fid;

  // JAVIER: How to reference the target?
  fid = nvm_create_file(nvm_target, beam, 0);
  if (fid) {
    // TODO: Throw exception
    DFLASH_FATAL("Cannot create nvm file\n");
  }
  fid_ = fid;

  char *name;
  int name_len = strlen(_name);
  if (name_len != 0) {
    SAFE_ALLOC(name, char[name_len + 1]);
    strcpy(name, _name);

    ALLOC_CLASS(names, list_node(name))
  } else {
    names = nullptr;
  }


  parent = _parent;
  filesize_ = 0;

  metadata_handle_ = new DFlashPrivateMetadata(this);
  last_modified_ = time(nullptr);


  DFLASH_DEBUG("New dflash file %s in %s. fd: %d", _name,
               _parent == nullptr ? "NULL" : _parent->GetName(), fd_);

  // opened_for_write = false;
  // seq_writable_file = nullptr;
}

dflash_file::~dflash_file() {
  list_node *temp = names;

#if 0
  if (seq_writable_file) {
    //flush any existing buffers
    seq_writable_file->Close();
    seq_writable_file = nullptr;
  }
#endif

  DFLASH_DEBUG("Destroy dflash_file - fd:%d\n", fd_);
  delete metadata_handle_;
  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete[] (char *)temp->GetData();
    delete temp;

    temp = temp1;
  }
}

//JAVIER: NECESSARY?
// void dflash_file::SetSeqWritableFile(DFlashWritableFile *_writable_file) {
  // seq_writable_file = _writable_file;
// }

void dflash_file::SetParent(dflash_dir *_parent) {
  parent = _parent;
}

dflash_dir *dflash_file::GetParent() {
  return parent;
}

// int dflash_file::LockFile() {
  // return pthread_mutex_lock(&file_lock);
// }

// void dflash_file::UnlockFile() {
  // pthread_mutex_unlock(&file_lock);
// }

Status dflash_file::Load(const int fd) {
  std::string _name;
  char readIn;

  DFLASH_DEBUG("loading file %p", this);

  if (read(fd, &readIn, 1) != 1) {
    return Status::IOError("Could no read f 1");
  }

  if (readIn != ':') {
    DFLASH_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  //load names
  _name = "";

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 2");
    }

    if (readIn == ',' || readIn == ':') {
      DFLASH_DEBUG("Adding name %s to %p", _name.c_str(), this);

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
    DFLASH_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  //load last modified

  last_modified_ = 0;

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read f 3");
    }

    if (readIn >= '0' && readIn <= '9') {
      last_modified_ = last_modified_ * 10 + readIn - '0';
    }
  } while (readIn >= '0' && readIn <= '9');

  if (readIn != ':') {
    DFLASH_DEBUG("ftl file is corrupt %c at %p", readIn, this);

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
      ClaimNewPage(parent->GetDFlashApi(), lun_id, block_id, page_id);
    }
  } while (readIn != '\n');

  return Status::OK();
}

Status dflash_file::Save(const int fd) {
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

//JAVIER: NECESSARY??
void dflash_file::Close(const char *mode) {
  if (mode[0] == 'r' || mode[0] == 'l') {
    return;
  }

  // opened_for_write = false;
}


#if 0
time_t dflash_file::GetLastModified() {
  time_t ret;

  pthread_mutex_lock(&meta_mtx);
  ret = last_modified_;
  pthread_mutex_unlock(&meta_mtx);

  return ret;
}
#endif

#if 0
bool dflash_file::CanOpen(const char *mode) {
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
#endif

bool dflash_file::HasName(const char *name, const int n) {
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

void dflash_file::AddName(const char *name) {
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

void dflash_file::ChangeName(const char *crt_name, const char *new_name) {
  pthread_mutex_lock(&meta_mtx);

  list_node *name_node = names;

  while (name_node) {
    char *crt_name_node = (char *)name_node->GetData();

    DFLASH_DEBUG("change name %s vs %s", crt_name_node, crt_name);

    if (strcmp(crt_name_node, crt_name) == 0) {
      DFLASH_DEBUG("MATCH");

      delete[] crt_name_node;

      SAFE_ALLOC(crt_name_node, char[strlen(new_name) + 1]);
      strcpy(crt_name_node, new_name);

      name_node->SetData(crt_name_node);

      DFLASH_DEBUG("SET DATA %s", crt_name_node);

      pthread_mutex_unlock(&meta_mtx);

      return;
    }

    name_node = name_node->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);
}

void dflash_file::EnumerateNames(std::vector<std::string>* result) {
  pthread_mutex_lock(&meta_mtx);

  list_node *name_node = names;

  while (name_node) {
    char *_name = (char *)name_node->GetData();

    result->push_back(_name);

    name_node = name_node->GetNext();
  }

  pthread_mutex_unlock(&meta_mtx);
}

unsigned long dflash_file::GetSize() {
  unsigned long ret;

  pthread_mutex_lock(&page_update_mtx);
  // Account for data in write cache
  if (seq_writable_file != nullptr) {
    ret = seq_writable_file->GetFileSize();
  } else {
    ret = size_;
  }
  pthread_mutex_unlock(&page_update_mtx);
  return ret;
}

void dflash_file::UpdateFileModificationTime() {
  pthread_mutex_lock(&meta_mtx);
  last_modified_ = time(nullptr);
  pthread_mutex_unlock(&meta_mtx);
}

void dflash_file::DeleteAllLinks(struct nvm *_nvm_api) {
  DFLASH_DEBUG("removing all links in %p", this);

  pthread_mutex_lock(&page_update_mtx);

  for (unsigned long i = 0; i < pages.size(); ++i) {
    ReclaimPage(_nvm_api, pages[i]);
  }

  pages.clear();

  // size_ = 0;

  pthread_mutex_unlock(&page_update_mtx);

  if (seq_writable_file) {
    //flush any existing buffers
    seq_writable_file->FileDeletedEvent();
    seq_writable_file = nullptr;
  }

  UpdateFileModificationTime();
}

bool dflash_file::Delete(const char * filename, struct nvm *nvm_api) {
  bool link_files_left = true;

  list_node *temp;
  list_node *temp1;
  list_node *next;
  list_node *prev;

  pthread_mutex_lock(&meta_mtx);

  temp = names;

  DFLASH_DEBUG("Deleting %s", filename);

  while (temp) {
    if (strcmp(filename, (char *)temp->GetData()) == 0) {
      temp1 = temp;

      prev = temp->GetPrev();
      next = temp->GetNext();

      if (prev) {
        DFLASH_DEBUG("Prev is not null");
        prev->SetNext(next);
      }

      if (next) {
        DFLASH_DEBUG("Next is not null");
        next->SetPrev(prev);
      }

      if (prev == nullptr && next != nullptr) {
        DFLASH_DEBUG("Moving head");
        names = names->GetNext();
      }

      if (next == nullptr && prev == nullptr) {
        DFLASH_DEBUG("No more links for this file");
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

  DFLASH_DEBUG("Deleting all links");
  DeleteAllLinks(nvm_api);

  // Put block back to BM
  PutAllBlocks(parent->GetDFlashApi());
  return true;
}

/*
 * DFlashFile: I/O operations - uses liblightnvm
 */
size_t dflash_file::Append() {

}

size_t dflash_file::Read(struct nvm *nvm, size_t read_pointer, char *data,
                      size_t data_len, struct page_cache *cache, uint8_t flags) {
}

bool dflash_file::Sync() {

}

/*
 * DFlashFile: Metadata operations - will use liblightnvm
 */
bool dflash_file::LoadPrivateMetadata(std::string fname, void* metadata) {
  struct vblock_meta *vblock_meta = (struct vblock_meta*)metadata;
  struct vblock *ptr = (struct vblock*)vblock_meta->encoded_vblocks;
  struct vblock *new_vblock;

  uint64_t left = vblock_meta->len;
  while (left > 0) {
    new_vblock = (struct vblock*)malloc(sizeof(struct vblock));
    if (!new_vblock) {
      DFLASH_FATAL("Could not allocate memory\n");
    }
    memcpy(new_vblock, ptr, sizeof(struct vblock));
    DFLASH_DEBUG("Load file: %s - Decoding: id: %lu, ownerid: %lu, nppas: %lu, ppa_bitmap: %lu, bppa: %llu, vlun_id: %d, flags: %d\n",
              fname.c_str(), new_vblock->id, new_vblock->owner_id, new_vblock->nppas, new_vblock->ppa_bitmap,
              new_vblock->bppa, new_vblock->vlun_id, new_vblock->flags);

    // Add to vblock vector
    LoadBlock(new_vblock);
    left--;
    ptr++;
  }
  UpdateCurrentBlock();
  free(vblock_meta->encoded_vblocks);
  free(vblock_meta);
  return true;
}

bool dflash_file::LoadSpecialMetadata(std::string fname) {
  std::string recovery_location = "testingrocks/DFLASH_RECOVERY";

  int fd = open(recovery_location.c_str(), O_RDONLY | S_IWUSR | S_IRUSR);
  if (fd < 0) {
    DFLASH_FATAL("Unable to open RECOVERY for reading\n");
  }

  char* recovery = new char[100]; //TODO: Can file names be larger?
  size_t offset = 0;
  ssize_t r = -1;
  int count = 0;
retry:
  size_t name_len = 0;
  char* ptr = recovery;
  while (1) {
    r = pread(fd, ptr, 1, offset);
    if (r <= 0) {
      if (errno == EINTR) {
        continue;
      } else if (errno == EOF) {
        DFLASH_DEBUG("File %s not found in RECOVERY\n", fname.c_str());
      }
      DFLASH_FATAL("Error reading RECOVERY\n");
      return false;
    }
    if (ptr[0] == ':') {
      break;
    }
    ptr += r;
    offset += r;
    name_len +=r;
  }

  offset += 1;
  if (fname.compare(0, fname.size(), recovery, name_len) != 0) {
    int len;
    char aux[2];
    if (pread(fd, &aux, 2, offset) != 2) {
      DFLASH_FATAL("Error reading RECOVERY file\n");
    }
    len = atoi(aux);

    lseek(fd, offset + len + 2 + 1, SEEK_SET);
    offset += len + 2 + 1; // aux + \n in RECOVERY format
    count ++;
    goto retry;
  }

  DFLASH_DEBUG("Found metadata in RECOVERY for file: %s\n", fname.c_str());
  int len;
  char aux[2];
  if (pread(fd, &aux, 2, offset) != 2) {
    DFLASH_FATAL("Error reading RECOVERY file\n");
  }
  len = atoi(aux);
  offset += 2; // aux

  char* read_meta = (char*)malloc(len * sizeof(char));
  if (!read_meta) {
    DFLASH_FATAL("Could not allocate memory\n");
  }

  if (pread(fd, read_meta, len, offset) != len) {
    DFLASH_FATAL("Error reading RECOVERY file: len: %d\n", len);
  }

  Slice recovery_meta = Slice(read_meta, len);
  void* meta = Env::DecodePrivateMetadata(&recovery_meta);
  if (!LoadPrivateMetadata(fname, meta)) {
    DFLASH_DEBUG("Could not load metadata from RECOVERY\n");
  }

  free(read_meta);
  close(fd);
  return true;
}

void dflash_file::RecoverAndLoadMetadata(struct nvm* nvm) {
  if (UNLIKELY(current_vblock_ == nullptr)) {
    DFLASH_DEBUG("Recovering metadata from an uninitialized dflash_file");
    std::vector<std::string> _names;
    EnumerateNames(&_names);
    uint8_t i = 0;
    while(LoadSpecialMetadata(_names[i])) {
      i++;
      if (i > _names.size()) {
        DFLASH_FATAL("Cannot recover file metadata\n");
      }
    }
  }
  // Recover metadata from last loaded vblock
  size_t meta_size = sizeof(struct vblock_close_meta);
  unsigned int block_offset = nblocks_ - 1;
  struct vblock_close_meta vblock_meta;
  if (ReadBlockClosingMetadata(
        nvm, block_offset, (char*)&vblock_meta) != meta_size) {
    DFLASH_FATAL("Error reading current block metadata\n");
  }

   //TODO: Make this better: mmap memory into device??
  struct vblock *new_vblock = (struct vblock*)malloc(sizeof(struct vblock));
  if (!new_vblock) {
    DFLASH_FATAL("Could not allocate memory\n");
  }

  DFLASH_DEBUG("GetBlockMeta: id: %lu\n", vblock_meta.next_vblock_id);

  if (!nvm->GetBlockMeta(vblock_meta.next_vblock_id, new_vblock)) {
    DFLASH_FATAL("could not get block metadata\n");
  }

  pthread_mutex_lock(&page_update_mtx);
  vblocks_.push_back(new_vblock);
  current_vblock_ = new_vblock;
  nblocks_++;
  pthread_mutex_unlock(&page_update_mtx);
}

void dflash_file::SaveSpecialMetadata(std::string fname) {
  // TODO: Get name from dbname
  std::string recovery_location = "testingrocks/DFLASH_RECOVERY";

  std::string recovery_meta = fname;
  recovery_meta.append(":", 1);

  //TODO: RETURN TO void*
  void* meta = DFlashPrivateMetadata::GetMetadata(this);
  std::string encoded_meta;
  Env::EncodePrivateMetadata(&encoded_meta, meta);
  Env::FreePrivateMetadata(meta);
  // TODO: Use PutVarint64 technique
  std::string len = std::to_string(encoded_meta.size());
  recovery_meta.append(len);
  recovery_meta.append(encoded_meta);

  int fd = open(recovery_location.c_str(), O_WRONLY | O_APPEND | O_CREAT, S_IWUSR);
  if (fd < 0) {
    DFLASH_DEBUG("Cannot create RECOVERY file\n");
    return;
  }

  recovery_meta.append("\n", 1);
  size_t left = recovery_meta.size();
  const char* src = recovery_meta.c_str();
  ssize_t done;
  while (left > 0) {
    done = write(fd, src, left);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      DFLASH_DEBUG("Unable to write private metadata in RECOVERY\n");
      return;
    }
    left -=done;
    src +=done;
  }

  close(fd);
}

/*
 * DFLashDIrectory implementation
 */
dflash_dir::dflash_dir(const char *_name, const int n, nvm *_nvm_api,
                             dflash_dir *_parent) {
  DFLASH_DEBUG("constructing directory %s in %s", _name,
            _parent == nullptr ? "NULL" : _parent->GetName());

  SAFE_ALLOC(name, char[n + 1]);
  strncpy(name, _name, n);
  name[n] = '\0';

  nvm_api = _nvm_api;

  head = nullptr;

  parent = _parent;

  pthread_mutexattr_init(&list_update_mtx_attr);
  pthread_mutexattr_settype(&list_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&list_update_mtx, &list_update_mtx_attr);
}

dflash_dir::~dflash_dir() {
  DFLASH_DEBUG("free directory %s", name);

  delete[] name;

  pthread_mutex_destroy(&list_update_mtx);
  pthread_mutexattr_destroy(&list_update_mtx_attr);

  //delete all files in the directory
  list_node *temp = head;

  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete (dflash_entry *)temp->GetData();
    delete temp;

    temp = temp1;
  }
}

void dflash_dir::ChangeName(const char *_name, const int n) {
  char *temp = name;

  SAFE_ALLOC(name, char[n + 1]);
  strncpy(name, _name, n);
  name[n] = '\0';

  DFLASH_DEBUG("changing %s to %s", temp, name);
  delete[] temp;
}

char *dflash_dir::GetName() {
  return name;
}

void dflash_dir::EnumerateNames(std::vector<std::string>* result) {
  result->push_back(name);
}

bool dflash_dir::HasName(const char *_name, const int n) {
  int i;

  for (i = 0; i < n; ++i) {
    if (name[i] != _name[i]) {
      return false;
    }
  }
  return (name[i] == '\0');
}

Status dflash_dir::Load(const int fd) {
  std::string _name;
  char readIn;

  DFLASH_DEBUG("loading directory %p", this);

  if (read(fd, &readIn, 1) != 1) {
    return Status::IOError("Could no read d 1");
  }

  if (readIn != ':') {
    DFLASH_DEBUG("ftl file is corrupt %c at %p", readIn, this);

    return Status::IOError("Corrupt ftl file");
  }

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read d 1");
    }

    if (readIn != '{') {
      _name.append(&readIn, 1);
    }
  } while(readIn != '{');

  if (name) {
    delete[] name;

    SAFE_ALLOC(name, char[_name.length() + 1]);
  }

  strcpy(name, _name.c_str());

  DFLASH_DEBUG("Loaded directory %s", name);

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read d 2");
    }

    switch (readIn) {
    case 'd': {
      dflash_dir *load_dir =
        (dflash_dir *)create_node("d", DirectoryEntry);

      if (!load_dir->Load(fd).ok()) {
        DFLASH_DEBUG("directory %p reported corruption", load_dir);
        return Status::IOError("Corrupt ftl file");
      }
      break;
    }
    case 'f': {
      dflash_file *load_file = (dflash_file *)create_node("", FileEntry);

      if (!load_file->Load(fd).ok()) {
        DFLASH_DEBUG("File %p reported corruption", load_file);
        return Status::IOError("Corrupt ftl file");
      }
      break;
    }
    case '}':
      break;

    default:
      DFLASH_DEBUG("ftl file is corrupt %c", readIn);
      return Status::IOError("Corrupt ftl file");
    }
  } while (readIn != '}');

  return Status::OK();
}

Status dflash_dir::Save(const int fd) {
  if (write(fd, "d:", 2) != 2) {
    return Status::IOError("Error writing 1");
  }

  int name_len = strlen(name);

  if (write(fd, name, name_len) != name_len) {
    return Status::IOError("Error writing 2");
  }

  if (write(fd, "{", 1) != 1) {
    return Status::IOError("Error writing 3");
  }

  list_node *temp;

  pthread_mutex_lock(&list_update_mtx);
  temp = head;
  pthread_mutex_unlock(&list_update_mtx);

  while (temp != nullptr) {
    dflash_entry *entry = (dflash_entry *)temp->GetData();

    switch (entry->GetType()) {
    case FileEntry: {
      dflash_file *process_file = (dflash_file *)entry->GetData();

      if (!process_file->Save(fd).ok()) {
        return Status::IOError("Error writing 4");
      }
      break;
    }
    case DirectoryEntry: {
      dflash_dir *process_directory = (dflash_dir *)entry->GetData();

      if (!process_directory->Save(fd).ok()) {
        return Status::IOError("Error writing 5");
      }
      break;
    }
    default:
      DFLASH_FATAL("Unknown entry type!!");
      break;
    }

    temp = temp->GetNext();
  }

  if (write(fd, "}", 1) != 1) {
    return Status::IOError("Error writing 3");
  }

  return Status::OK();
}

//Check if the node exists
list_node *dflash_dir::node_look_up(list_node *prev,
                                       const char *look_up_name) {
  list_node *temp;
  int i = 0;

  DFLASH_DEBUG("looking for %s in %s", look_up_name, name);

  while (look_up_name[i] != '/' && look_up_name[i] != '\0') {
    ++i;
  }

  DFLASH_DEBUG("i is %d", i);

  if (i == 0) {
    DFLASH_DEBUG("returning prev");
    return prev;
  }

  pthread_mutex_lock(&list_update_mtx);
  temp = head;
  pthread_mutex_unlock(&list_update_mtx);

  DFLASH_DEBUG("mutex released %p", temp);

  while (temp != nullptr) {
    dflash_entry *entry = (dflash_entry *)temp->GetData();

    switch (entry->GetType()) {
    case FileEntry: {
      dflash_file *process_file = (dflash_file *)entry->GetData();

      if (process_file->HasName(look_up_name, i) == false) {
        break;
      }

      if (look_up_name[i] == '\0') {
        DFLASH_DEBUG("found file at %p", temp);
        return temp;
      } else {
        return nullptr;
      }
      break;
    }
    case DirectoryEntry: {
      dflash_dir *process_directory = (dflash_dir *)entry->GetData();

      if (process_directory->HasName(look_up_name, i) == false) {
        break;
      }

      if (look_up_name[i] == '\0') {
        DFLASH_DEBUG("found directory at %p", temp);
        return temp;
      } else {
        return process_directory->node_look_up(temp, look_up_name + i + 1);
      }
      break;
    }
    default:
      DFLASH_FATAL("Unknown entry type!!");
      break;
    }

    temp = temp->GetNext();
  }

  DFLASH_DEBUG("returning null");
  return nullptr;
}

//Check if the node with a specific type exists
list_node *dflash_dir::node_look_up(const char *look_up_name,
                                       const dflash_entry_type type) {
  list_node *temp = node_look_up(nullptr, look_up_name);

  if (temp == nullptr) {
    return nullptr;
  }

  dflash_entry *entry = (dflash_entry *)temp->GetData();

  if (entry->GetType() != type) {
    return nullptr;
  }

  return temp;
}

//Check if the directory exists
dflash_dir *dflash_dir::directory_look_up(const char *directory_name) {
  list_node *temp = node_look_up(directory_name, DirectoryEntry);

  if (temp == nullptr) {
    return nullptr;
  }

  return (dflash_dir *)(((dflash_entry *)temp->GetData())->GetData());
}

//Check if the file exists
dflash_file *dflash_dir::file_look_up(const char *filename) {
  list_node *temp = node_look_up(filename, FileEntry);

  if (temp == nullptr) {
    return nullptr;
  }

  return (dflash_file *)(((dflash_entry *)temp->GetData())->GetData());
}

void *dflash_dir::create_node(const char *look_up_name,
                                 const dflash_entry_type type) {
  dflash_file *fd;
  dflash_dir *dd;
  dflash_entry *entry;

  list_node *file_node;
  list_node *iterator;

  void *ret;
  int i = 0;

  while (look_up_name[i] != '/' && look_up_name[i] != '\0') {
    ++i;
  }

  pthread_mutex_lock(&list_update_mtx);

  iterator = head;

  while (iterator) {
    entry = (dflash_entry *)iterator->GetData();

    switch (entry->GetType()) {
    case FileEntry: {
      fd = (dflash_file *)entry->GetData();

      if (fd->HasName(look_up_name, i) == false) {
        break;
      }

      if (look_up_name[i] != '\0') {
        ret = nullptr;
        goto out;
      }

      if (type == FileEntry) {
        ret = fd;
      } else {
        ret = nullptr;
      }

      goto out;
    }
    case DirectoryEntry: {
      dd = (dflash_dir *)entry->GetData();

      if (dd->HasName(look_up_name, i) == false) {
        break;
      }

      if (look_up_name[i] != '\0') {
        ret = dd->create_node(look_up_name + i + 1, type);
        goto out;
      }

      if (type == DirectoryEntry) {
        ret = dd;
      } else {
        ret = nullptr;
      }
      goto out;
    }
    default:
      DFLASH_FATAL("Unknown entry type!!");
      break;
    }

    iterator = iterator->GetNext();
  }

  if (look_up_name[i] == '\0') {
    switch (type) {
    case FileEntry: {
      ALLOC_CLASS(fd, dflash_file(look_up_name, nvm_api->fd, this));
      ALLOC_CLASS(entry, dflash_entry(FileEntry, fd));

      ret = fd;
      break;
    }
    case DirectoryEntry: {
      ALLOC_CLASS(dd, dflash_dir(look_up_name, strlen(look_up_name),
                                    nvm_api, this));
      ALLOC_CLASS(entry, dflash_entry(DirectoryEntry, dd));

      ret = dd;
      break;
    }
    default:
      DFLASH_FATAL("unknown node type!!");
      break;
    }

    ALLOC_CLASS(file_node, list_node(entry));
  } else {
    ALLOC_CLASS(dd, dflash_dir(look_up_name, i, nvm_api, this));
    ALLOC_CLASS(entry, dflash_entry(DirectoryEntry, dd));
    ALLOC_CLASS(file_node, list_node(entry));

    ret = dd->create_node(look_up_name + i + 1, type);
  }

  file_node->SetNext(head);
  if (head) {
    head->SetPrev(file_node);
  }

  head = file_node;

out:
  pthread_mutex_unlock(&list_update_mtx);
  DFLASH_DEBUG("created file %s at %p", look_up_name, ret);
  return ret;
}

dflash_file *dflash_dir::create_file(const char *filename, const int beam) {
  //JAVIER: Need to decouple node creation and implement all file functionality
  //here
  return (dflash_file *)create_node(filename, FileEntry);
}

int dflash_dir::CreateDirectory(const char *directory_name) {
  if (create_node(directory_name, DirectoryEntry) != nullptr) {
    return 0;
  }

  return -1;
}

dflash_file *dflash_dir::open_file_if_exists(const char *filename) {
  return file_look_up(filename);
}

dflash_file *dflash_dir::dflash_create(const char *filename, const int beam,
                                      const char *mode) {
  dflash_file *fd;

  if (mode[0] != 'a' && mode[0] != 'w' && mode[0] != 'l') {
    fd = open_file_if_exists(filename);
  } else {
    fd = create_file(filename, beam);
  }

  if (fd->CanOpen(mode)) {
    return fd;
  } else {
    return nullptr;
  }
}

int dflash_dir::GetFileSize(const char *filename, unsigned long *size) {
  dflash_file *fd = file_look_up(filename);

  if (fd) {
    *size = fd->GetSize();
    return 0;
  }

  *size = 0;
  return 1;
}

int dflash_dir::GetFileModificationTime(const char *filename, time_t *mtime) {
  dflash_file *fd = file_look_up(filename);

  if (fd) {
    *mtime = fd->GetLastModified();
    return 0;
  }

  *mtime = 0;
  return 1;
}

bool dflash_dir::FileExists(const char *_name) {
  return (node_look_up(nullptr, _name) != nullptr);
}

nvm *dflash_dir::GetNVMApi() {
  return nvm_api;
}

void dflash_dir::nvm_fclose(dflash_file *file, const char *mode) {
  DFLASH_DEBUG("closing file at %p with %s", file, mode);

  file->Close(mode);
}

int dflash_dir::LinkFile(const char *src, const char *target) {
  pthread_mutex_lock(&list_update_mtx);

  dflash_file *fd = file_look_up(target);

  if (fd) {
    pthread_mutex_unlock(&list_update_mtx);
    return -1;
  }

  fd = file_look_up(src);
  if (fd) {
    fd->AddName(target);
    pthread_mutex_unlock(&list_update_mtx);
    return 0;
  }

  pthread_mutex_unlock(&list_update_mtx);
  return -1;
}

void dflash_dir::Remove(dflash_dir *fd) {
  pthread_mutex_lock(&list_update_mtx);
  struct list_node *iterator = head;

  while (iterator) {
    dflash_entry *entry = (dflash_entry *)iterator->GetData();

    if (entry->GetType() != DirectoryEntry) {
      iterator = iterator->GetNext();
      continue;
    }

    dflash_dir *file = (dflash_dir *)entry->GetData();

    if (file != fd) {
      iterator = iterator->GetNext();
      continue;
    }

    DFLASH_DEBUG("Found file to remove");

    struct list_node *prev = iterator->GetPrev();
    struct list_node *next = iterator->GetNext();

    if (prev) {
      prev->SetNext(next);
    }

    if (next) {
      next->SetPrev(prev);
    }

    if (prev == nullptr && next != nullptr) {
      head = head->GetNext();
    }

    if (prev == nullptr && next == nullptr) {
      head = nullptr;
    }

    break;
  }

  pthread_mutex_unlock(&list_update_mtx);
}

void dflash_dir::Add(dflash_dir *fd) {
  pthread_mutex_lock(&list_update_mtx);
  struct list_node *node;

  dflash_entry *entry;

  ALLOC_CLASS(entry, dflash_entry(DirectoryEntry, fd));
  ALLOC_CLASS(node, list_node(entry));

  node->SetNext(head);

  if (head) {
    head->SetPrev(node);
  }

  head = node;

  pthread_mutex_unlock(&list_update_mtx);
}

void dflash_dir::Remove(dflash_file *fd) {
  pthread_mutex_lock(&list_update_mtx);

  struct list_node *iterator = head;

  while (iterator) {
    dflash_entry *entry = (dflash_entry *)iterator->GetData();
    if (entry->GetType() != FileEntry) {
      iterator = iterator->GetNext();
      continue;
    }

    dflash_file *file = (dflash_file *)entry->GetData();
    if (file != fd) {
      iterator = iterator->GetNext();
      continue;
    }

    DFLASH_DEBUG("Found file to remove");

    struct list_node *prev = iterator->GetPrev();
    struct list_node *next = iterator->GetNext();

    if (prev) {
      prev->SetNext(next);
    }

    if (next) {
      next->SetPrev(prev);
    }

    if (prev == nullptr && next != nullptr) {
      head = head->GetNext();
    }

    if (prev == nullptr && next == nullptr) {
      head = nullptr;
    }

    break;
  }

  pthread_mutex_unlock(&list_update_mtx);
}

void dflash_dir::Add(dflash_file *fd) {
  pthread_mutex_lock(&list_update_mtx);

  struct list_node *node;
  dflash_entry *entry;

  ALLOC_CLASS(entry, dflash_entry(FileEntry, fd));
  ALLOC_CLASS(node, list_node(entry));

  node->SetNext(head);

  if (head) {
    head->SetPrev(node);
  }

  head = node;
  pthread_mutex_unlock(&list_update_mtx);
}

int dflash_dir::RenameDirectory(const char *crt_filename,
                                   const char *new_filename) {
  dflash_dir *fd;

  dflash_dir *crt_parent_dir;
  dflash_dir *new_parent_dir;

  pthread_mutex_lock(&list_update_mtx);

  crt_parent_dir = OpenParentDirectory(crt_filename);

  if (crt_parent_dir == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 1;
  }

  new_parent_dir = OpenParentDirectory(new_filename);

  if (new_parent_dir == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 1;
  }

  fd = directory_look_up(crt_filename);

  if (fd == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 1;
  }

  int last_slash_new = 0;
  int i = 0;

  while (new_filename[i] != '\0') {
    if(new_filename[i] == '/' && new_filename[i + 1] != '\0') {
      last_slash_new = i + 1;
    }

    ++i;
  }

  int len = strlen(new_filename);

  if (new_filename[len - 1] == '/') {
    len -= 1;
  }

  len -= last_slash_new;

  fd->ChangeName(new_filename + last_slash_new, len);

  if(new_parent_dir != crt_parent_dir) {
    DFLASH_DEBUG("Rename is changing directories");

    crt_parent_dir->Remove(fd);
    new_parent_dir->Add(fd);
  }

  pthread_mutex_unlock(&list_update_mtx);
  return 0;
}

int dflash_dir::RenameFile(const char *crt_filename, const char *new_filename) {
  pthread_mutex_lock(&list_update_mtx);

  dflash_file *fd;
  dflash_dir *dir = OpenParentDirectory(new_filename);

  if (dir == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 1;
  }

  fd = file_look_up(crt_filename);
  if (fd == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 1;
  }

  DeleteFile(new_filename);

  int last_slash_crt = 0;
  int i = 0;

  while (crt_filename[i] != '\0') {
    if (crt_filename[i] == '/') {
      last_slash_crt = i + 1;
    }

    ++i;
  }

  int last_slash_new = 0;
  i = 0;

  while (new_filename[i] != '\0') {
    if (new_filename[i] == '/') {
      last_slash_new = i + 1;
    }

    ++i;
  }

  fd->ChangeName(crt_filename + last_slash_crt, new_filename + last_slash_new);
  if (fd->GetParent() != dir) {
    DFLASH_DEBUG("Rename is changing directories");

    fd->GetParent()->Remove(fd);
    dir->Add(fd);
    fd->SetParent(dir);
  }

  pthread_mutex_unlock(&list_update_mtx);
  return 0;
}

int dflash_dir::DeleteFile(const char *filename) {
  unsigned char last_slash = 0;

  for (unsigned int i = 0; i < strlen(filename); ++i) {
    if (filename[i] == '/') {
      last_slash = i;
    }
  }

  pthread_mutex_lock(&list_update_mtx);

  DFLASH_DEBUG("Deleting %s", filename);

  list_node *file_node = node_look_up(filename, FileEntry);

  if (file_node) {
    dflash_entry *entry = (dflash_entry *)file_node->GetData();
    dflash_file *file = (dflash_file *)entry->GetData();

    bool ret;

    if (last_slash > 0) {
      ret = file->Delete(filename + last_slash + 1, nvm_api);
    } else {
      ret = file->Delete(filename, nvm_api);
    }

    if(ret) {
      file->GetParent()->Remove(file);
      
      delete entry;
      delete file_node;
    }    
  }

  pthread_mutex_unlock(&list_update_mtx);
  return 0;
}

dflash_dir *dflash_dir::GetParent() {
  return parent;
}

dflash_dir *dflash_dir::OpenParentDirectory(const char *filename) {
  int i = 0;
  int last_slash = 0;

  while (filename[i] != '\0') {
    if ((filename[i] == '/') && (filename[i + 1] != '\0')) {
      last_slash = i;
    }

    ++i;
  }

  if (last_slash == 0) {
    return this;
  }

  char *_name;
  SAFE_ALLOC(_name, char[last_slash + 1]);

  memcpy(_name, filename, last_slash);
  _name[last_slash] = '\0';

  dflash_dir *ret = OpenDirectory(_name);

  delete _name;

  return ret;
}

dflash_dir *dflash_dir::OpenDirectory(const char *_name) {
  return directory_look_up(_name);
}

void dflash_dir::Delete(nvm *_nvm_api) {
  DFLASH_DEBUG("delete %s", name);

  //delete all files in the directory
  list_node *temp = head;

  while (temp != nullptr) {
    dflash_entry *entry = (dflash_entry *)temp->GetData();

    switch (entry->GetType()) {
    case DirectoryEntry: {
      dflash_dir *dir = (dflash_dir *)entry->GetData();
      dir->Delete(_nvm_api);
      break;
    }
    case FileEntry: {
      dflash_file *fd = (dflash_file *)entry->GetData();
      fd->DeleteAllLinks(_nvm_api);
      break;
    }
    default:
      DFLASH_FATAL("unknown entry type");
      break;
    }

    temp = temp->GetNext();
  }
}

void dflash_dir::GetChildren(std::vector<std::string>* result) {
  list_node *temp;

  pthread_mutex_lock(&list_update_mtx);
  temp = head;
  pthread_mutex_unlock(&list_update_mtx);

  while (temp) {
    dflash_entry *entry = (dflash_entry *)temp->GetData();

    switch (entry->GetType()) {
    case DirectoryEntry: {
      dflash_dir *dir = (dflash_dir *)entry->GetData();
      dir->EnumerateNames(result);
      break;
    }
    case FileEntry: {
      dflash_file *fd = (dflash_file *)entry->GetData();
      fd->EnumerateNames(result);
      break;
    }
    default:
      DFLASH_FATAL("unknown nvm entry");
      break;
    }

    temp = temp->GetNext();
  }
}

int dflash_dir::GetChildren(const char *_name,
                               std::vector<std::string>* result) {
  dflash_dir *dir = directory_look_up(_name);

  if (dir == nullptr) {
    return -1;
  }

  dir->GetChildren(result);
  return 0;
}

int dflash_dir::DeleteDirectory(const char *_name) {
  pthread_mutex_lock(&list_update_mtx);

  list_node *dir_node = node_look_up(_name, DirectoryEntry);

  if (dir_node == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 0;
  }

  dflash_entry *entry = (dflash_entry *)dir_node->GetData();
  dflash_dir *directory = (dflash_dir *)entry->GetData();

  directory->GetParent()->Remove(directory);

  pthread_mutex_unlock(&list_update_mtx);

  directory->Delete(nvm_api);

  delete entry;
  delete dir_node;

  return 0;
}

} // namespace rocksdb

#endif
