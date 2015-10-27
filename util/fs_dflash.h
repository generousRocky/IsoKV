#ifndef _FS_DFLASH_H_
#define _FS_DFLASH_H_

namespace rocksdb {

class dflash_file;
class dflash_dir;

typedef enum {
  FileEntry,
  DirectoryEntry
} dflash_entry_type;

class dflash_entry {
 private:
  dflash_entry_type type_;
  void* data_;
 public:
  dflash_entry(const dflash_entry_type type, void* data);
  ~dflash_entry();
  void* GetData();
  dflash_entry_type GetType();
};

class dflash_file {
 private:
  list_node* names;
  FileType type_;

  unsigned long size_;
  int fd_;

  // Private metadata
  DFlashPrivateMetadata* metadata_handle_;

  // time_t last_modified; //TODO: Check
  // pthread_mutex_t meta_mtx; //TODO Check
  // pthread_mutex_t file_lock; //TODO: Check

  // bool opened_for_write; //TODO: Check

  dflash_dir* parent;
  DFlashWritableFile *seq_writable_file;

 public:
  dflash_file(const char* _name, const int fd, dflash_dir* _parent);
  ~dflash_file();

  // Used by DFlashDirectory
  bool CanOpen(const char* mode);
  bool HasName(const char* name, const int n);
  void ChangeName(const char* crt_name, const char* new_name);
  void EnumerateNames(std::vector<std::string>* result);
  dflash_dir* GetParent(); // TODO: Check
  void SetParent(dflash_dir* _parent); //TODO: Check
  // void SetSeqWritableFile(DFlashWritableFile *_writable_file);

  void SetType(FileType type) { type_ = type; }
  FileType GetType() { return type_; }

  // TODO: Check this
  FilePrivateMetadata* GetMetadataHandle() {
    return metadata_handle_;
  }
  void* GetMetadata() {
    return metadata_handle_->GetMetadata(this);
  }

  unsigned long GetSize();
  DFlashWritableFile* GetWritePointer() {
    return seq_writable_file;
  }

  // time_t GetLastModified(); //TODO: Check
  void UpdateFileModificationTime();
  void Close(const char *mode);

  int GetFD();

  bool LoadPrivateMetadata(std::string fname, void* metadata);
  bool LoadSpecialMetadata(std::string fname);
  void RecoverAndLoadMetadata(struct nvm* nvm);

  // liglightnvm
  // append
  // read (sequential)
  // read (random)

  // TODO: Check both
  // bool Delete(const char* filename, struct nvm* nvm_api);
  // void DeleteAllLinks(struct nvm* _nvm_api);

  // TODO: Check
  int LockFile();
  void UnlockFile();

  // TODO: Check
  void AddName(const char* name);

  // FTL operations - TODO: Check
  Status Save(const int fd);
  Status Load(const int fd);

  // TODO: Check
  // Save metadata that is not present in the MANIFEST (e.g., the last log)
  void SaveSpecialMetadata(std::string fname);
};

class dflash_dir {
 private:
  char *name;

  //list of nvm_entries (dflash_file or dflash_dir)
  list_node *head;
  nvm *nvm_api;

  pthread_mutex_t list_update_mtx;
  pthread_mutexattr_t list_update_mtx_attr;

  void *create_node(const char *name, const dflash_entry_type type);
  dflash_dir *parent;

 public:
  dflash_dir(const char *_name, const int n, nvm *_nvm_api, dflash_dir *_parent);
  ~dflash_dir();

  char *GetName();
  bool HasName(const char *_name, const int n);
  void EnumerateNames(std::vector<std::string>* result);
  void Delete(nvm *_nvm_api);

  list_node *node_look_up(list_node *prev, const char *look_up_name);
  list_node *node_look_up(const char *look_up_name, const dflash_entry_type type);
  dflash_dir *directory_look_up(const char *directory_name);
  dflash_file *file_look_up(const char *filename);

  dflash_file *open_file_if_exists(const char *filename);
  dflash_dir *OpenParentDirectory(const char *filename);
  dflash_dir *OpenDirectory(const char *name);
  dflash_dir *GetParent();
  dflash_file *nvm_fopen(const char *filename, const char *mode);
  dflash_file *create_file(const char *filename);

  nvm *GetDFlashApi();

  int CreateDirectory(const char *name);
  int DeleteDirectory(const char *name);
  int GetFileSize(const char *filename, unsigned long *size);
  int GetFileModificationTime(const char *filename, time_t *mtime);
  int LinkFile(const char *src, const char *target);
  int RenameFile(const char *crt_filename, const char *new_filename);
  int RenameDirectory(const char *crt_filename, const char *new_filename);
  int DeleteFile(const char *filename);
  int GetChildren(const char *name, std::vector<std::string>* result);

  void GetChildren(std::vector<std::string>* result);

  bool FileExists(const char *name);

  void Remove(dflash_file *fd);
  void Add(dflash_file *fd);
  void Remove(dflash_dir *fd);
  void Add(dflash_dir *fd);
  void ChangeName(const char *_name, const int n);

  void nvm_fclose(dflash_file *file, const char *mode);

  Status Save(const int fd);
  Status Load(const int fd);
};

} //rocksdb namespace
#endif //_FS_DFLASH_H_
