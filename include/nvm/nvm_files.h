#ifndef _NVM_FILES_
#define _NVM_FILES_

#include "db/filename.h"
#include <errno.h>

namespace rocksdb {

#include <vector>

class NVMWritableFile;

class NVMPrivateMetadata: public FilePrivateMetadata {
 private:
  nvm_file *file_;
 protected:
  static const uint32_t separator_ = 42;
  friend class Env;
 public:
  NVMPrivateMetadata(nvm_file *file);
  virtual ~NVMPrivateMetadata();

  void* GetMetadata() override;
  void UpdateMetadataHandle(nvm_file *file);

  static void* GetMetadata(nvm_file* file);
};

class nvm_file {
  private:
    list_node *names;

    pthread_mutexattr_t page_update_mtx_attr;
    pthread_mutex_t page_update_mtx;

    //TODO: look with new vector
    unsigned long size_;
    unsigned long meta_size_;
    int fd_;

    // Private metadata
    NVMPrivateMetadata* metadata_handle_;

    // TODO: current_vblock_ is used to write, logic should move to WritableFile
    struct vblock *current_vblock_;
    std::vector<struct vblock *>vblocks_;       //Vector of virtual flash blocks
    std::vector<struct nvm_page *> pages;

    pthread_mutex_t write_lock;

#ifdef NVM_ALLOCATE_BLOCKS
    std::vector<struct nvm_page *> block_pages;
#endif

    time_t last_modified;

    pthread_mutex_t meta_mtx;
    pthread_mutex_t file_lock;

    bool opened_for_write;

    nvm_directory *parent;
    NVMWritableFile *seq_writable_file;

    bool ClaimNewPage(nvm *nvm_api, const unsigned long lun_id,
                    const unsigned long block_id, const unsigned long page_id);

  protected:
    friend class NVMPrivateMetadata;
    friend class NVMEnv;
    friend class Env;

  public:
    nvm_file(const char *_name, const int fd, nvm_directory *_parent);
    ~nvm_file();

    bool HasBlock();

    bool CanOpen(const char *mode);
    bool ClaimNewPage(nvm *nvm_api);
    bool ClearLastPage(nvm *nvm_api);
    bool HasName(const char *name, const int n);
    void ChangeName(const char *crt_name, const char *new_name);
    void EnumerateNames(std::vector<std::string>* result);
    void SetSeqWritableFile(NVMWritableFile *_writable_file);
    void UpdateCurrentBlock() {
      current_vblock_ = vblocks_.back();
    }
    FilePrivateMetadata* GetMetadataHandle() {
      return metadata_handle_;
    }
    void* GetMetadata() {
      return metadata_handle_->GetMetadata(this);
    }

    unsigned long GetSize();
    unsigned long GetPersistentSize();
    NVMWritableFile* GetWritePointer() {
      return seq_writable_file;
    }

    time_t GetLastModified();
    void UpdateFileModificationTime();
    void Close(const char *mode);

    int GetFD();

    size_t GetNextPos() { return vblocks_.size() + 1; }
    void GetBlock(struct nvm *nvm, unsigned int vlun_id);
    void ReplaceBlock(struct nvm *nvm, unsigned int vlun_id,
                                                      unsigned int block_idx);
    void PutBlock(struct nvm *nvm, struct vblock *vblock);
    void PutAllBlocks(struct nvm *nvm);
    size_t FlushBlock(struct nvm *nvm, char *data, size_t ppa_offset,
                                  const size_t data_len, bool page_aligned);
    size_t Read(struct nvm *nvm, size_t read_pointer, char *data,
                                                              size_t data_len);

    size_t ReadBlock(struct nvm *nvm, unsigned int block_offset,
                                   size_t ppa_offset, unsigned int page_offset,
                                                  char *data, size_t data_len);
    struct nvm_page *GetNVMPage(const unsigned long idx);
    struct nvm_page *GetLastPage(unsigned long *page_idx);
    bool SetPage(const unsigned long page_idx, nvm_page *page);

    struct nvm_page *RequestPage(nvm *nvm_api);
    struct nvm_page *RequestPage(nvm *nvm_api, const unsigned long lun_id,
                    const unsigned long block_id, const unsigned long page_id);
    void ReclaimPage(nvm *nvm_api, struct nvm_page *pg);

    nvm_directory *GetParent();

    void SetParent(nvm_directory *_parent);

    size_t nvm_fread(void *data, const unsigned long offset, const size_t len);

    bool Delete(const char *filename, struct nvm *nvm_api);
    void DeleteAllLinks(struct nvm *_nvm_api);

    int LockFile();
    void UnlockFile();

    void AddName(const char *name);

    Status Save(const int fd);
    Status Load(const int fd);
};

class NVMFileLock : public FileLock {
  public:
    nvm_file *fd_;
    std::string filename;
    nvm_directory *root_dir_;

    bool Unlock();
};

class NVMSequentialFile: public SequentialFile {
  private:
    std::string filename_;

    union {
      nvm_file *fd_;
      int posix_fd_;
    };

    nvm_directory *dir_;
    size_t read_pointer_;

    //TODO: Implement this. Only cache the page that is left half read. We need
    //to keep track of it when skiping too.
    char *current_page_;                //Cached current page;

    unsigned long channel;
    unsigned long page_pointer;

    unsigned long crt_page_idx;
    struct nvm_page *crt_page;

  public:
    NVMSequentialFile(const std::string& fname, nvm_file *f,
                                                          nvm_directory *_dir);
    virtual ~NVMSequentialFile();

    virtual Status Read(size_t n, Slice* result, char* scratch) override;
    virtual Status Skip(uint64_t n) override;
    virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class NVMRandomAccessFile: public RandomAccessFile {
  private:
    std::string filename_;

    union {
      nvm_file *fd_;
      int posix_fd_;
    };

    nvm_directory *dir_;
    unsigned long channel;

    struct nvm_page *SeekPage(const unsigned long offset,
                  unsigned long *page_pointer, unsigned long *page_idx) const;

  public:
    NVMRandomAccessFile(const std::string& fname, nvm_file *f,
                                                          nvm_directory *_dir);
    virtual ~NVMRandomAccessFile();

    virtual Status Read(uint64_t offset, size_t n, Slice* result,
                                                char* scratch) const override;

#ifdef OS_LINUX
    virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif

    virtual void Hint(AccessPattern pattern) override;
    virtual Status InvalidateCache(size_t offset, size_t length) override;
};

// Use nvm write to write data to a file.
class NVMWritableFile : public WritableFile {
  private:
    const std::string filename_;

    union {
      nvm_file *fd_;
      int posix_fd_;
    };

    // Only used by posix for CURRENT, LOG, LOCK, and IDENTITY
    uint64_t filesize_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
    bool fallocate_with_keep_size_;
#endif

    // From here all variables are used exclusively by the DFlash backend
    nvm_directory *dir_;

    size_t cursize_;            // Current buf_ length. It follows mem_
    size_t curflush_;           // Bytes in buf_ that have already been flushed
    size_t buf_limit_;          // Limit of the allocated memory region
    char *buf_;                 // Buffer to cache writes
    char *mem_;                 // Points to the place to append data in memory.
                                // It defines the part of the buffer containing
                                // valid data.
    char *flush_;               // Points to place in buf_ until which data has
                                // been flushed to the media

    bool l0_table;              // Signals if this file is being used to store a
                                // level 0 sstable. This is used by the Flush
                                // method to determine if the size of the file
                                // can exceed the size of a block or not.

    // write
    struct vblock_partial_meta write_pointer_;

    unsigned long channel;

    //JAVIER: This will go
    unsigned long last_page_idx;
    struct nvm_page *last_page;

    bool closed_;

    size_t CalculatePpaOffset(size_t curflush);
    bool Flush(const bool closing);
    bool GetNewBlock();
    bool UpdateLastPage();

  public:
    NVMWritableFile(const std::string& fname, nvm_file *fd, nvm_directory *dir);
    ~NVMWritableFile();

    virtual Status Append(const Slice& data) override;
    virtual Status Close() override;

    virtual Status Flush() override;
    virtual Status Sync() override;
    virtual Status Fsync() override;

    virtual uint64_t GetFileSize() override;

    virtual Status InvalidateCache(size_t offset, size_t length) override;
    virtual FilePrivateMetadata* GetMetadataHandle() override;

#ifdef ROCKSDB_FALLOCATE_PRESENT
    virtual Status Allocate(off_t offset, off_t len) override;
    virtual Status RangeSync(off_t offset, off_t nbytes) override;
    virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

class NVMRandomRWFile : public RandomRWFile {
  private:
    const std::string filename_;

    union {
      nvm_file *fd_;
      int posix_fd_;
    };

    nvm_directory *dir_;
    unsigned long channel;

    // TODO: Buffer PAGE_SIZE
    // size_t cursize_;            // Current buf_ length
    // size_t buf_limit_;          // Limit of the allocated memory region
    // char *buf_;                 // Buffer to cache writes
    // char *dst_;                 // Where to write next in buf_

    struct nvm_page *SeekPage(const unsigned long offset,
                  unsigned long *page_pointer, unsigned long *page_idx) const;

  public:
    NVMRandomRWFile(const std::string& fname, nvm_file *_fd,
                                                          nvm_directory *_dir);
    ~NVMRandomRWFile();

    virtual Status Write(uint64_t offset, const Slice& data) override;
    virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;
    virtual Status Close() override;

    virtual Status Sync() override;
    virtual Status Fsync() override;

#ifdef ROCKSDB_FALLOCATE_PRESENT
    virtual Status Allocate(off_t offset, off_t len) override;
#endif
};

// Posix classes. Copied from env_posix
// This is a momentary solution until we decouple posix file implementations
// from the posix environment
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  Status UnmapCurrentRegion();
  Status MapNewRegion();
  Status Msync();

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                                                    const EnvOptions& options);
  ~PosixMmapFile();

  virtual Status Append(const Slice& data) override;
  virtual Status Close() override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual uint64_t GetFileSize() override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(off_t offset, off_t len) override;
#endif
};

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;
  int fd_;
  bool use_os_buffer_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f,
                                                     const EnvOptions& options);
  ~PosixSequentialFile();

  virtual Status Read(size_t n, Slice* result, char* scratch) override;
  virtual Status Skip(uint64_t n) override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class PosixWritableFile : public WritableFile {
 private:
  const std::string filename_;
  int fd_;
  uint64_t filesize_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool fallocate_with_keep_size_;
#endif

 public:
  PosixWritableFile(const std::string& fname, int fd,
                                                    const EnvOptions& options);
  ~PosixWritableFile();

  virtual Status Append(const Slice& data) override;
  virtual Status Close() override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Fsync() override;
  virtual uint64_t GetFileSize() override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(off_t offset, off_t len) override;
  virtual Status RangeSync(off_t offset, off_t nbytes) override;
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

class PosixRandomRWFile : public RandomRWFile {
 private:
  const std::string filename_;
  int fd_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool fallocate_with_keep_size_;
#endif

 public:
  PosixRandomRWFile(const std::string& fname, int fd, const EnvOptions& options);
  ~PosixRandomRWFile();

  virtual Status Write(uint64_t offset, const Slice& data) override;
  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;
  virtual Status Close() override;

  virtual Status Sync() override;
  virtual Status Fsync() override;

#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual Status Allocate(off_t offset, off_t len) override;
#endif
};

class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;
  bool use_os_buffer_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd,
                                                  const EnvOptions& options);
  ~PosixRandomAccessFile();

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                                                char* scratch) const override;

#ifdef OS_LINUX
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif

  virtual void Hint(AccessPattern pattern) override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class PosixMmapReadableFile: public RandomAccessFile {
 private:
  int fd_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const int fd, const std::string& fname,
                        void* base, size_t length,
                        const EnvOptions& options);
  ~PosixMmapReadableFile();

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                                                char* scratch) const override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class PosixDirectory : public Directory {
 public:
  explicit PosixDirectory(int fd) : fd_(fd) {}
  ~PosixDirectory() {
    close(fd_);
  }

  virtual Status Fsync() override {
    if (fsync(fd_) == -1) {
      return Status::IOError("directory", strerror(errno));
    }
    return Status::OK();
  }

 private:
  int fd_;
};

class PosixFileLock : public FileLock {
 public:
  int fd_;
  std::string filename;

  bool Unlock();
};

} //rocksdb namespace

#endif // _NVM_FILES_
