#ifndef _NVM_FILES_
#define _NVM_FILES_

namespace rocksdb {

#include <vector>

class NVMWritableFile;

class nvm_file {
  private:
    list_node *names;

    pthread_mutexattr_t page_update_mtx_attr;
    pthread_mutex_t page_update_mtx;

    unsigned long size_;
    int fd_;

    // TODO: Should it be a list of blocks?
    struct vblock *vblock_;       // Virtual flash block
    std::vector<struct nvm_page *> pages;

    pthread_mutex_t write_lock;
    sector_t write_ppa_;

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

    void GetBlock(struct nvm *nvm, unsigned int vlun_id);
    void PutBlock(struct nvm *nvm);
    void FreeBlock();
    size_t WriteBlock(struct nvm *nvm, void *data, const size_t data_len);
    size_t Read(struct nvm *nvm, size_t bppa, char *data, size_t data_len);

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
};

class NVMSequentialFile: public SequentialFile {
  private:
    std::string filename_;

    nvm_file *fd_;
    nvm_directory *dir_;

    unsigned int ppa_offset_;           //ppa offset from bppa;
    unsigned int page_offset_;          //byte offset in ppa;

    //TODO: Implement this. Only cache the page that is left half read. We need
    //to keep track of it when skiping too.
    char *current_page_;                //Cached current page;

    unsigned long file_pointer;
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

    size_t cursize_;            // Current buf_ length
    size_t buf_limit_;          // Limit of the allocated memory region
    char *buf_;                 // Buffer to cache writes
    char *dst_;                 // Where to write next in buf_

    uint64_t bytes_per_sync_;

    unsigned long channel;

    //JAVIER: This will go
    unsigned long last_page_idx;
    struct nvm_page *last_page;

    bool closed_;

    bool Flush(const bool closing);
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
    nvm_directory *dir;

    nvm *nvm_api;

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
