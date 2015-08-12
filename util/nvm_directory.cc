#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb {

nvm_directory::nvm_directory(const char *_name, const int n, nvm *_nvm_api,
                                                        nvm_directory *_parent)
{
  NVM_DEBUG("constructing directory %s in %s", _name,
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

nvm_directory::~nvm_directory() {
  NVM_DEBUG("free directory %s", name);

  delete[] name;

  pthread_mutex_destroy(&list_update_mtx);
  pthread_mutexattr_destroy(&list_update_mtx_attr);

  //delete all files in the directory
  list_node *temp = head;

  while (temp != nullptr) {
    list_node *temp1 = temp->GetNext();

    delete (nvm_entry *)temp->GetData();
    delete temp;

    temp = temp1;
  }
}

char *nvm_directory::GetName() {
  return name;
}

void nvm_directory::EnumerateNames(std::vector<std::string>* result) {
    result->push_back(name);
}

bool nvm_directory::HasName(const char *_name, const int n) {
  int i;

  for (i = 0; i < n; ++i) {
    if (name[i] != _name[i]) {
      return false;
    }
  }
  return (name[i] == '\0');
}

Status nvm_directory::Load(const int fd) {
  std::string _name;
  char readIn;

  NVM_DEBUG("loading directory %p", this);

  if (read(fd, &readIn, 1) != 1) {
    return Status::IOError("Could no read d 1");
  }

  if (readIn != ':') {
    NVM_DEBUG("ftl file is corrupt %c at %p", readIn, this);

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

  NVM_DEBUG("Loaded directory %s", name);

  do {
    if (read(fd, &readIn, 1) != 1) {
      return Status::IOError("Could no read d 2");
    }

    switch (readIn) {
      case 'd': {
        nvm_directory *load_dir =
                            (nvm_directory *)create_node("d", DirectoryEntry);

        if (!load_dir->Load(fd).ok()) {
          NVM_DEBUG("directory %p reported corruption", load_dir);
          return Status::IOError("Corrupt ftl file");
        }
        break;
      }
      case 'f': {
        nvm_file *load_file = (nvm_file *)create_node("", FileEntry);

        if (!load_file->Load(fd).ok()) {
          NVM_DEBUG("File %p reported corruption", load_file);
          return Status::IOError("Corrupt ftl file");
        }
        break;
      }
      case '}':
        break;

      default:
        NVM_DEBUG("ftl file is corrupt %c", readIn);
      return Status::IOError("Corrupt ftl file");
    }
  } while (readIn != '}');

  return Status::OK();
}

Status nvm_directory::Save(const int fd) {
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
    nvm_entry *entry = (nvm_entry *)temp->GetData();

    switch (entry->GetType()) {
      case FileEntry: {
        nvm_file *process_file = (nvm_file *)entry->GetData();

        if (!process_file->Save(fd).ok()) {
          return Status::IOError("Error writing 4");
        }
        break;
      }
      case DirectoryEntry: {
        nvm_directory *process_directory = (nvm_directory *)entry->GetData();

        if (!process_directory->Save(fd).ok()) {
          return Status::IOError("Error writing 5");
        }
        break;
      }
      default:
        NVM_FATAL("Unknown entry type!!");
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
list_node *nvm_directory::node_look_up(list_node *prev,
                                                  const char *look_up_name) {
  list_node *temp;
  int i = 0;

  NVM_DEBUG("looking for %s in %s", look_up_name, name);

  while (look_up_name[i] != '/' && look_up_name[i] != '\0') {
    ++i;
  }

  NVM_DEBUG("i is %d", i);

  if (i == 0) {
    NVM_DEBUG("returning prev");
    return prev;
  }

  pthread_mutex_lock(&list_update_mtx);
  temp = head;
  pthread_mutex_unlock(&list_update_mtx);

  NVM_DEBUG("mutex released %p", temp);

  while (temp != nullptr) {
    nvm_entry *entry = (nvm_entry *)temp->GetData();

    switch (entry->GetType()) {
      case FileEntry: {
        nvm_file *process_file = (nvm_file *)entry->GetData();

        if (process_file->HasName(look_up_name, i) == false) {
           break;
        }

        if (look_up_name[i] == '\0') {
          NVM_DEBUG("found file at %p", temp);
          return temp;
        } else {
          return nullptr;
        }
      break;
      }
      case DirectoryEntry: {
        nvm_directory *process_directory = (nvm_directory *)entry->GetData();

        if (process_directory->HasName(look_up_name, i) == false) {
          break;
        }

        if (look_up_name[i] == '\0') {
          NVM_DEBUG("found directory at %p", temp);
          return temp;
        } else {
          return process_directory->node_look_up(temp, look_up_name + i + 1);
        }
        break;
      }
      default:
        NVM_FATAL("Unknown entry type!!");
        break;
    }

    temp = temp->GetNext();
  }

    NVM_DEBUG("returning null");
    return nullptr;
}

//Check if the node with a specific type exists
list_node *nvm_directory::node_look_up(const char *look_up_name,
                                                  const nvm_entry_type type) {
  list_node *temp = node_look_up(nullptr, look_up_name);

  if (temp == nullptr) {
    return nullptr;
  }

  nvm_entry *entry = (nvm_entry *)temp->GetData();

  if (entry->GetType() != type) {
    return nullptr;
  }

  return temp;
}

//Check if the directory exists
nvm_directory *nvm_directory::directory_look_up(const char *directory_name) {
  list_node *temp = node_look_up(directory_name, DirectoryEntry);

  if (temp == nullptr) {
    return nullptr;
  }

  return (nvm_directory *)(((nvm_entry *)temp->GetData())->GetData());
}

//Check if the file exists
nvm_file *nvm_directory::file_look_up(const char *filename)
{
  list_node *temp = node_look_up(filename, FileEntry);

  if (temp == nullptr) {
    return nullptr;
  }

  return (nvm_file *)(((nvm_entry *)temp->GetData())->GetData());
}

void *nvm_directory::create_node(const char *look_up_name,
                                                  const nvm_entry_type type) {
  nvm_file *fd;
  nvm_directory *dd;
  nvm_entry *entry;

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
    entry = (nvm_entry *)iterator->GetData();

    switch (entry->GetType()) {
      case FileEntry: {
        fd = (nvm_file *)entry->GetData();

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
        dd = (nvm_directory *)entry->GetData();

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
        NVM_FATAL("Unknown entry type!!");
        break;
    }

    iterator = iterator->GetNext();
  }

  if (look_up_name[i] == '\0') {
    switch (type) {
      case FileEntry: {
        ALLOC_CLASS(fd, nvm_file(look_up_name, nvm_api->fd, this));
        ALLOC_CLASS(entry, nvm_entry(FileEntry, fd));

        ret = fd;
        break;
      }
      case DirectoryEntry: {
        ALLOC_CLASS(dd, nvm_directory(look_up_name, strlen(look_up_name),
                                                              nvm_api, this));
        ALLOC_CLASS(entry, nvm_entry(DirectoryEntry, dd));

        ret = dd;
        break;
      }
      default:
        NVM_FATAL("unknown node type!!");
        break;
    }

    ALLOC_CLASS(file_node, list_node(entry));
  } else {
    ALLOC_CLASS(dd, nvm_directory(look_up_name, i, nvm_api, this));
    ALLOC_CLASS(entry, nvm_entry(DirectoryEntry, dd));
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
  NVM_DEBUG("created file %s at %p", look_up_name, ret);
  return ret;
}

nvm_file *nvm_directory::create_file(const char *filename)
{
  return (nvm_file *)create_node(filename, FileEntry);
}

int nvm_directory::CreateDirectory(const char *directory_name)
{
  if (create_node(directory_name, DirectoryEntry) != nullptr) {
    return 0;
  }

  return -1;
}

nvm_file *nvm_directory::open_file_if_exists(const char *filename)
{
  return file_look_up(filename);
}

//Open existing file or create a new one
nvm_file *nvm_directory::nvm_fopen(const char *filename, const char *mode)
{
  nvm_file *fd;

  if (mode[0] != 'a' && mode[0] != 'w' && mode[0] != 'l') {
    fd = open_file_if_exists(filename);
  } else {
    fd = create_file(filename);
  }

  if (fd->CanOpen(mode)) {
    return fd;
  } else {
    return nullptr;
  }
}

int nvm_directory::GetFileSize(const char *filename, unsigned long *size)
{
  nvm_file *fd = file_look_up(filename);

  if (fd) {
    *size = fd->GetSize();
    return 0;
  }

  *size = 0;
  return 1;
}

int nvm_directory::GetFileModificationTime(const char *filename, time_t *mtime)
{
  nvm_file *fd = file_look_up(filename);

  if (fd) {
    *mtime = fd->GetLastModified();
    return 0;
  }

  *mtime = 0;
  return 1;
}

bool nvm_directory::FileExists(const char *_name)
{
  return (node_look_up(nullptr, _name) != nullptr);
}

nvm *nvm_directory::GetNVMApi()
{
  return nvm_api;
}

void nvm_directory::nvm_fclose(nvm_file *file, const char *mode)
{
  NVM_DEBUG("closing file at %p with %s", file, mode);

  file->Close(mode);
}

int nvm_directory::LinkFile(const char *src, const char *target)
{
  pthread_mutex_lock(&list_update_mtx);

  nvm_file *fd = file_look_up(target);

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

void nvm_directory::Remove(nvm_file *fd)
{
  pthread_mutex_lock(&list_update_mtx);

  struct list_node *iterator = head;

  while (iterator) {
    nvm_entry *entry = (nvm_entry *)iterator->GetData();
    if (entry->GetType() != FileEntry) {
      iterator = iterator->GetNext();
      continue;
    }

    nvm_file *file = (nvm_file *)entry->GetData();
    if (file != fd) {
      iterator = iterator->GetNext();
      continue;
    }

    NVM_DEBUG("Found file to remove");

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

void nvm_directory::Add(nvm_file *fd)
{
  pthread_mutex_lock(&list_update_mtx);

  struct list_node *node;
  nvm_entry *entry;

  ALLOC_CLASS(entry, nvm_entry(FileEntry, fd));
  ALLOC_CLASS(node, list_node(entry));

  node->SetNext(head);

  if (head) {
    head->SetPrev(node);
  }

  head = node;
  pthread_mutex_unlock(&list_update_mtx);
}

int nvm_directory::RenameFile(const char *crt_filename, const char *new_filename)
{
  pthread_mutex_lock(&list_update_mtx);

  nvm_file *fd;
  nvm_directory *dir = OpenParentDirectory(new_filename);

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
    NVM_DEBUG("Rename is changing directories");

    fd->GetParent()->Remove(fd);
    dir->Add(fd);
    fd->SetParent(dir);
  }

  pthread_mutex_unlock(&list_update_mtx);
  return 0;
}

int nvm_directory::DeleteFile(const char *filename)
{
  unsigned char last_slash = 0;

  for (unsigned int i = 0; i < strlen(filename); ++i) {
    if (filename[i] == '/') {
      last_slash = i;
    }
  }

  pthread_mutex_lock(&list_update_mtx);

  NVM_DEBUG("Deleting %s", filename);

  list_node *file_node = node_look_up(filename, FileEntry);

  if (file_node) {
    list_node *prev = file_node->GetPrev();
    list_node *next = file_node->GetNext();

    nvm_entry *entry = (nvm_entry *)file_node->GetData();
    nvm_file *file = (nvm_file *)entry->GetData();

    bool ret;

    if (last_slash > 0) {
      ret = file->Delete(filename + last_slash + 1, nvm_api);
    } else {
      ret = file->Delete(filename, nvm_api);
    }

    if (ret) {
      NVM_DEBUG("NO MORE LINKS.. removing from list");

      //we have no more link files
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
        head = head->GetNext();
      }

      if (next == nullptr && prev == nullptr) {
        NVM_DEBUG("Head becomes null");
        head = nullptr;
      }

      delete entry;
      delete file_node;
    }
  }

  pthread_mutex_unlock(&list_update_mtx);
  return 0;
}

nvm_directory *nvm_directory::OpenParentDirectory(const char *filename)
{
  int i = 0;
  int last_slash = 0;

  while (filename[i] != '\0') {
    if (filename[i] == '/') {
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

  nvm_directory *ret = OpenDirectory(_name);

  delete _name;

  return ret;
}

nvm_directory *nvm_directory::OpenDirectory(const char *_name)
{
  return directory_look_up(_name);
}

void nvm_directory::Delete(nvm *_nvm_api)
{
  NVM_DEBUG("delete %s", name);

  //delete all files in the directory
  list_node *temp = head;

  while (temp != nullptr) {
    nvm_entry *entry = (nvm_entry *)temp->GetData();

    switch (entry->GetType()) {
      case DirectoryEntry: {
        nvm_directory *dir = (nvm_directory *)entry->GetData();
        dir->Delete(_nvm_api);
        break;
      }
      case FileEntry: {
        nvm_file *fd = (nvm_file *)entry->GetData();
        fd->DeleteAllLinks(_nvm_api);
        break;
      }
      default:
        NVM_FATAL("unknown entry type");
        break;
    }

    temp = temp->GetNext();
  }
}

void nvm_directory::GetChildren(std::vector<std::string>* result)
{
  list_node *temp;

  pthread_mutex_lock(&list_update_mtx);
  temp = head;
  pthread_mutex_unlock(&list_update_mtx);

  while (temp) {
    nvm_entry *entry = (nvm_entry *)temp->GetData();

    switch (entry->GetType()) {
      case DirectoryEntry: {
        nvm_directory *dir = (nvm_directory *)entry->GetData();
        dir->EnumerateNames(result);
        break;
      }
      case FileEntry: {
        nvm_file *fd = (nvm_file *)entry->GetData();
        fd->EnumerateNames(result);
        break;
      }
      default:
        NVM_FATAL("unknown nvm entry");
        break;
    }

    temp = temp->GetNext();
  }
}

int nvm_directory::GetChildren(const char *_name,
                                             std::vector<std::string>* result)
{
  nvm_directory *dir = directory_look_up(_name);

  if (dir == nullptr) {
    return -1;
  }

  dir->GetChildren(result);
  return 0;
}

int nvm_directory::DeleteDirectory(const char *_name)
{
  pthread_mutex_lock(&list_update_mtx);

  list_node *dir_node = node_look_up(_name, DirectoryEntry);

  if (dir_node == nullptr) {
    pthread_mutex_unlock(&list_update_mtx);
    return 0;
  }

  list_node *prev = dir_node->GetPrev();
  list_node *next = dir_node->GetNext();

  if (prev) {
    prev->SetNext(next);
  }

  if (next) {
    next->SetPrev(prev);
  }

  if (prev == nullptr && next != nullptr) {
    head = head->GetNext();
  }

  if (next == nullptr && prev == nullptr) {
    head = nullptr;
  }

  pthread_mutex_unlock(&list_update_mtx);

  NVM_DEBUG("deleting %s at %p", _name, dir_node);

  nvm_entry *entry = (nvm_entry *)dir_node->GetData();
  nvm_directory *directory = (nvm_directory *)entry->GetData();

  directory->Delete(nvm_api);

  delete entry;
  delete dir_node;

  return 0;
}

} // namespace rocksdb

#endif
