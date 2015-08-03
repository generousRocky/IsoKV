#ifndef _NVM_FILES_
#define _NVM_FILES_

#include "db/filename.h"

namespace rocksdb {

#include <vector>

class NVMWritableFile;

class nvm_file {
  private:
    list_node *names;

    pthread_mutexattr_t page_update_mtx_attr;
    pthread_mutex_t page_update_mtx;

    //TODO: look with new vector
    unsigned long size_;
    unsigned long meta_size_;
    int fd_;
    FileType type_;

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

    friend class NVMPrivateMetadata;
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

    unsigned long GetSize();

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

    void SetType(std::string fname) {
      uint64_t num;
      size_t dir_pos;
      std::string filename;

      // Asume dbname/file as in filename filename.cc
      dir_pos = fname.find_first_of("/") + 1; // Account for "/"
      filename.append(fname.c_str() + dir_pos);

      if (!ParseFileName(filename, &num, &type_)) {
        NVM_ERROR("Cannot parse file %s\n", fname.c_str());
      }
    }

    Status Save(const int fd);
    Status Load(const int fd);
};

class NVMFileLock : public FileLock {
  public:
    nvm_file *fd_;
    std::string filename;
};

class NVMPrivateMetadata: public FilePrivateMetadata {
 private:
  nvm_file *file_;
  uint32_t separator_ = 0;
 public:
  NVMPrivateMetadata(nvm_file *file);
  virtual ~NVMPrivateMetadata();
  void* GetMetadata() override;
  void UpdateMetadataHandle(nvm_file *file);
};

class NVMSequentialFile: public SequentialFile {
  private:
    std::string filename_;

    nvm_file *fd_;
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

    unsigned long channel;

    nvm_file *fd_;
    nvm_directory *dir_;

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

    nvm_file *fd_;
    nvm_directory *dir_;

    size_t cursize_;            // Current buf_ length. It follows mem_
    size_t curflush_;           // Byte in buf_ that have already been flushed
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

    // Private metadata
    NVMPrivateMetadata* metadata_handle;


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

    unsigned long channel;

    nvm_file *fd_;
    nvm_directory *dir_;

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

} //rocksdb namespace

#endif // _NVM_FILES_
