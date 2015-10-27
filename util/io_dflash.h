#ifndef _IO_DFlash_
#define _IO_DFlash_

#include <errno.h>
#include <thread>
#include <vector>
#include <unistd.h>
#include "rocksdb/env.h"
#include "db/filename.h"
#include "dflash/fs_dflash.h"

namespace rocksdb {

class DFlashPrivateMetadata: public FilePrivateMetadata {
 private:
  void* priv_meta_;
  dflash_file *file_; //TODO: Check
  static const uint32_t separator_ = 42;
 public:
  DFlashPrivateMetadata(dflash_file *file);
  virtual ~DFlashPrivateMetadata();

  void EncodePrivateMetadata(std::string* dst) override;
  void DecodePrivateMetadata(Slice* encoded_meta) override;
  void FreePrivateMetadata() override;

  void* GetPrivateMetadata() { return priv_meta_; }
  // TODO: These two might go
  void* GetMetadata() override;
  static void* GetMetadata(dflash_file* file);
};

class DFlashFileLock : public FileLock {
  public:
    dflash_file* fd_;
    std::string filename;
    dflash_dir* root_dir_;

    bool Unlock();
};

/*
 * TODO: All
 */
class DFlashSequentialFile: public SequentialFile {
  private:
    const std::string filename_;
    dflash_dir* dir_;

    union {
      dflash_file* fd_;
      int posix_fd_;
    };

    size_t read_pointer_;
  public:
    DFlashSequentialFile(const std::string& fname, dflash_file* f,
                      dflash_dir* _dir);
    virtual ~DFlashSequentialFile();
    virtual Status Read(size_t n, Slice* result, char* scratch) override;
    virtual Status Skip(uint64_t n) override;
    virtual Status InvalidateCache(size_t offset, size_t length) override;
};

/*
 * TODO: All
 */
class DFlashRandomAccessFile: public RandomAccessFile {
  private:
    const std::string filename_;
    dflash_dir* dir_;

    union {
      dflash_file* fd_;
      int posix_fd_;
    };

  public:
    DFlashRandomAccessFile(const std::string& fname, dflash_file* f,
                                                          dflash_dir* _dir);
    virtual ~DFlashRandomAccessFile();
    virtual Status Read(uint64_t offset, size_t n, Slice* result,
                                                char* scratch) const override;
#ifdef OS_LINUX
    virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
    virtual void Hint(AccessPattern pattern) override;
    virtual Status InvalidateCache(size_t offset, size_t length) override;
};

/*
 * TODO: All
 */
class DFlashWritableFile : public WritableFile {
  private:
    const std::string filename_;
    dflash_dir *dir_;

    union {
      dflash_file *fd_;
      int posix_fd_;
    };

    // Only used by posix for CURRENT, LOG, LOCK, and IDENTITY
    uint64_t filesize_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
    bool fallocate_with_keep_size_; //TODO: Check
#endif

    size_t cursize_;            // Current buf_ length. It follows mem_
    size_t curflush_;           // Bytes in buf_ that have already been flushed
    size_t buf_limit_;          // Limit of the allocated memory region
    char *buf_;                 // Buffer to cache writes
    char *mem_;                 // Points to the place to append data in memory.
                                // It defines the part of the buffer containing
                                // valid data.
    char *flush_;               // Points to place in buf_ until which data has
                                // been flushed to the media

    bool Flush(const bool closing);
  public:
    DFlashWritableFile(const std::string& fname, dflash_file *fd,
                       dflash_dir *dir);
    ~DFlashWritableFile();

    virtual Status Truncate(uint64_t size) override { return Status::OK(); }
    virtual bool IsSyncThreadSafe() const override { return false; } //TODO: Check
    virtual bool UseDirectIO() const override { return false; } //TODO: Check
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

// Implementation of PosixFileLock that can unlock itself.
class PosixFileLock : public FileLock {
 public:
  int fd_;
  std::string filename;

  bool Unlock();
};

class DFlashDirectory : public Directory {
  private:
    dflash_dir* fd_;

  public:
    explicit DFlashDirectory(dflash_dir *fd);
    ~DFlashDirectory();

    virtual Status Fsync() override;
};

} //rocksdb namespace

#endif
