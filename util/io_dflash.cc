#ifdef ROCKSDB_PLATFORM_DFLASH

#include <cstring>
#include <thread>
#include "malloc.h"
#include "util/coding.h"
#include "nvm/nvm.h"
#include "posix/lib_posix.h"

// Get nano time includes
#if defined(OS_LINUX) || defined(OS_FREEBSD)
#elif defined(__MACH__)
#include <mach/clock.h>
#include <mach/mach.h>
#else
#include <chrono>
#endif

#if !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif
#if !defined(XFS_SUPER_MAGIC)
#define XFS_SUPER_MAGIC 0x58465342
#endif
#if !defined(EXT4_SUPER_MAGIC)
#define EXT4_SUPER_MAGIC 0xEF53
#endif

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined CYGWIN)
#define POSIX_FADV_NORMAL 0 /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1 /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3 /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4 /* [MC1] dont need these pages */
#endif

namespace rocksdb {

#if defined(OS_LINUX)
// TODO: DFlash implementation
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

/*
 * FilePrivateMetadata implementation
 */
DFlashPrivateMetadata::DFlashPrivateMetadata(nvm_file *file) {
  file_ = file;
  priv_meta_ = nullptr;
}

DFlashPrivateMetadata::~DFlashPrivateMetadata() {}

//TODO: Can we maintain a friend reference to DFlashWritableFile to simplify this?
// void DFlashPrivateMetadata::UpdateMetadataHandle(nvm_file *file) {
  // file_ = file;
// }

void* DFlashPrivateMetadata::GetMetadata(nvm_file *file) {
  std::deque<struct vblock *>::iterator it;
  std::string metadata;

  PutVarint32(&metadata, file->vblocks_.size());
  for (it = file->vblocks_.begin(); it != file->vblocks_.end(); it++) {
    DFLASH_DEBUG("METADATA: Writing(%lu):\nsep:%d,id:%lu\noid:%lu\nnppas:%lu\nbitmap:%lu\nbppa:%llu\nvlunid:%d\nflags:%d\n",
      file->vblocks_.size(), separator_, (*it)->id, (*it)->owner_id, (*it)->nppas, (*it)->ppa_bitmap, (*it)->bppa,
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
  // At this point metadata has not been persisted yet, but it is given
  // FileMetaData; in normal operation metadata will be persisted. In case of
  // of RocksDB crushing before this happens, we can reconstruct this metadata
  // from individual blocks in a recover phase.
  file->blocks_meta_persisted_ = file->vblocks_.size();
  memcpy(vblock_meta->encoded_vblocks, metadata.c_str(), metadata_size);
  return (void*)vblock_meta;
}

// For now store the whole vblock as metadata. When we can retrieve a vblock
// from its ID from the BM we can reduces the amount of metadata stored in
// MANIFEST
void* DFlashPrivateMetadata::GetMetadata() {
  return GetMetadata(file_);
}

void DFlashPrivateMetadata::EncodePrivateMetadata(std::string* dst) {
  Env::EncodePrivateMetadata(dst, GetMetadata(file_));
}

void DFlashPrivateMetadata::DecodePrivateMetadata(Slice* encoded_meta) {
  priv_meta_ = Env::DecodePrivateMetadata(encoded_meta);
}

void DFlashPrivateMetadata::FreePrivateMetadata() {
  if (priv_meta_ != nullptr) {
    Env::FreePrivateMetadata(priv_meta_);
    priv_meta_ = nullptr;
  }
}

/*
 * SequentialFile implementation
 */
DFlashSequentialFile::DFlashSequentialFile(const std::string& fname, nvm_file *fd,
                                                          nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;

  if (!fd_->HasBlock()) {
    DFLASH_ERROR("No block associated with file descriptor for file %s\n", fname.c_str());
  }

  page_cache_.vblock_offset = 0;
  page_cache_.bppa_cached = 0;
  page_cache_.bytes_cached = 0;
  page_cache_.cache = nullptr;

  read_pointer_ = 0;
}

DFlashSequentialFile::~DFlashSequentialFile() {
  // fd_->CleanPageCache(&page_cache_);
  dir_->nvm_fclose(fd_, "r");
}

//TODO: Cache last read page to avoid small reads submitting extra IOs. We
//should only cache until file size
Status DFlashSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  struct nvm *nvm = dir_->GetDFlashApi();

  if (read_pointer_ + n > fd_->GetSize()) {
    n = fd_->GetSize() - read_pointer_;
  }

  if (n <= 0) {
    *result = Slice(scratch, 0);
    return Status::OK();
  }

  // DFLASH_DEBUG("READING FROM SEQ FILE: %s, offset: %lu, n:%lu\n", filename_.c_str(),
                                                              // read_pointer_, n);
  // DFlash_file knows how data is spread across vblocks. Thus we let it recover
  // data from the page cache or update the pointer to the most recent read
  // pages.
  if (fd_->Read(nvm, read_pointer_, scratch, n, &page_cache_, READ_SEQ) != n) {
    return Status::IOError("Unable to read\n");
  }

  read_pointer_ += n;
  *result = Slice(scratch, n);
  return Status::OK();
}

Status DFlashSequentialFile::Skip(uint64_t n) {
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

Status DFlashSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

/*
 * RandomAccessFile implementation
 */
DFlashRandomAccessFile::DFlashRandomAccessFile(const std::string& fname, nvm_file *f,
                                      nvm_directory *dir) : filename_(fname) {
  fd_ = f;
  dir_ = dir;

  page_cache_.vblock_offset = 0;
  page_cache_.bppa_cached = 0;
  page_cache_.bytes_cached = 0;
  page_cache_.cache = nullptr;
}

DFlashRandomAccessFile::~DFlashRandomAccessFile() {
  // fd_->CleanPageCache(&page_cache_);
  dir_->nvm_fclose(fd_, "r");
}

Status DFlashRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
                                                        char* scratch) const {
  if (offset + n > fd_->GetSize()) {
    DFLASH_DEBUG("offset is out of bounds. Filename: %s, offset: %lu, n: %lu, filesize: %lu\n",
                                filename_.c_str(), offset, n, fd_->GetSize());
    // Read all that has been written to either the buffer or the dflash backend
    n = fd_->GetSize() - offset;
  }

  struct nvm *nvm = dir_->GetDFlashApi();

  // DFLASH_DEBUG("READING FROM RA FILE: %s, offset: %lu, n:%lu\n", filename_.c_str(), offset, n);
  if (fd_->Read(nvm, offset, scratch, n, &page_cache_, READ_RAND) != n) {
    return Status::IOError("Unable to read\n");
  }

  *result = Slice(scratch, n);
  return Status::OK();
}

#ifdef OS_LINUX
size_t DFlashRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void DFlashRandomAccessFile::Hint(AccessPattern pattern) {
}

Status DFlashRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

/*
 * WritableFile implementation
 */
DFlashWritableFile::DFlashWritableFile(const std::string& fname, nvm_file *fd,
                                                          nvm_directory *dir) :
  filename_(fname) {
  fd_ = fd;
  dir_ = dir;

  struct nvm *nvm = dir_->GetDFlashApi();
  //TODO: Use the vlun type when this is available
  unsigned int vlun_type = 0;
  //Get block from block manager
  fd_->GetBlock(nvm, vlun_type);

  size_t real_buf_limit = nvm->GetNPagesBlock(vlun_type) * PAGE_SIZE;

  // Account for the metadata to be stored at the end of the file
  buf_limit_ = real_buf_limit - sizeof(struct vblock_close_meta);
  buf_ = (char*)memalign(PAGE_SIZE, real_buf_limit);
  if (!buf_) {
    DFLASH_FATAL("Could not allocate aligned memory\n");
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

DFlashWritableFile::~DFlashWritableFile() {
  if(fd_) {
    fd_->SetSeqWritableFile(nullptr);

    // TODO: This is for first prototype. This should be moved to SaveFTL, which
    // should be called if a crush is detected. Even though we have a way to
    // recover all metadata from individual blocks in case of a total crash (e.g.,
    // power failure), this mechanism (which is cheaper) should be resistant to
    // RocksDB internal crushes.
    // TODO: Do not save special metadata for manifest
    if ((fd_->GetNPersistentMetaBlocks() == 0) &&
                                            (fd_->GetType() != kMetaDatabase)) {
      DFLASH_DEBUG("Saving special metadata for file: %s\n", filename_.c_str());
      fd_->SaveSpecialMetadata(filename_);
    }
  }

  DFlashWritableFile::Close();
}

void DFlashWritableFile::FileDeletedEvent() {
  fd_ = nullptr;
}

size_t DFlashWritableFile::CalculatePpaOffset(size_t curflush) {
  // For now we assume that all blocks have the same size. When this assumption
  // no longer holds, we would need to iterate vblocks_ in nvm_file, or hold a
  // ppa pointer for each WritableFile.
  // We always flush one page at the time
  size_t disaligned_data = curflush_ % PAGE_SIZE;
  size_t aligned_data = curflush / PAGE_SIZE;
  uint8_t x = (disaligned_data == 0) ? 0 : 1;
  return (aligned_data + x);
}

// We try to flush at a page granurality. We need to see how this affects
// writes.
bool DFlashWritableFile::Flush(const bool force_flush) {
  struct nvm *nvm = dir_->GetDFlashApi();
  size_t flush_len = cursize_ - curflush_;
  size_t ppa_flush_offset = CalculatePpaOffset(curflush_);
  struct vblock_close_meta vblock_meta;
  bool page_aligned = false;

  if(fd_ == nullptr) {
    DFLASH_DEBUG("FILE WAS DELETED. DON'T FLUSH")
    return true;
  }

  if (!force_flush && flush_len < PAGE_SIZE) {
    return true;
  }

  if (flush_len == 0) {
    return true;
  }

  assert (curflush_ + flush_len <= buf_limit_);

  if (force_flush) {
    DFLASH_DEBUG("FORCED FLUSH IN FILE %s. this: %p ref: %p\n",
          filename_.c_str(), this, fd_);
    // Append vblock medatada when closing a block.
    // TODO: If a file is forced to be flushed and not all metadata has been
    // stored on the MANIFEST, by not saving closing metadata, we do cannot
    // reconstruct the DFLash file when reading; this will trigger an error.
    if (curflush_ + flush_len == buf_limit_) {
      DFLASH_DEBUG("Next blockid: %lu\n", fd_->GetNextBlockID());
      unsigned int meta_size = sizeof(vblock_meta);
      vblock_meta.written_bytes = buf_limit_;
      vblock_meta.ppa_bitmap = 0x0; //Use real bad page information
      vblock_meta.next_vblock_id = fd_->GetNextBlockID();
      vblock_meta.flags = VBLOCK_CLOSED;
      memcpy(mem_, &vblock_meta, meta_size);
      flush_len += meta_size;
    } else {
      //TODO: Pass on to upper layers to append metadata to RocksDB WAL
      // TODO: Save this partial write structure to the metadata file
      write_pointer_.ppa_offset = ppa_flush_offset + flush_len / PAGE_SIZE;
      write_pointer_.page_offset = flush_len % PAGE_SIZE;
    }
    page_aligned = (flush_len % PAGE_SIZE == 0) ? true : false;
  } else {
    size_t disaligned_data = flush_len % PAGE_SIZE;
    flush_len -= disaligned_data;
    page_aligned = true;
  }

  size_t written_bytes = fd_->FlushBlock(nvm, flush_, ppa_flush_offset,
                                                        flush_len, page_aligned);
  if (written_bytes < flush_len) {
    DFLASH_DEBUG("unable to write data");
    return false;
  }

  curflush_ += written_bytes;
  flush_ += written_bytes;

  return true;
}

// At this moment we buffer a whole block before syncing in normal operation.
// TODO: We need to differentiate between the log and the memtable to use
// different buffer sizes.
Status DFlashWritableFile::Append(const Slice& data) {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  if(fd_ == nullptr) {
    return Status::IOError("file has been deleted");
  }

  const char* src = data.data();
  size_t left = data.size();
  size_t offset = 0;

  // If the size of the appended data does not fit in one flash block, fill out
  // this block, get a new block and continue writing
  if (cursize_ + left > buf_limit_) {
    PreallocateNewBlock();
    size_t fits_in_buf = (buf_limit_ - cursize_);
    memcpy(mem_, src, fits_in_buf);
    mem_ += fits_in_buf;
    cursize_ += fits_in_buf;
    if (Flush(true) == false) {
      return Status::IOError("out of ssd space");
    }
    UseNewBlock();
    offset = fits_in_buf;
  }
  memcpy(mem_, src + offset, left - offset);
  mem_ += left - offset;
  cursize_ += left - offset;

  return Status::OK();
}

Status DFlashWritableFile::Close() {
  if (closed_ || fd_ == nullptr) {
    if (buf_) {
      free(buf_);
    }
    buf_ = nullptr;
    return Status::OK();
  }

  closed_ = true;

  if (Flush(true) == false) {
    return Status::IOError("out of ssd space");
  }

  DFLASH_DEBUG("File %s - size: %lu - writablesize: %lu\n", filename_.c_str(),
                                        fd_->GetSize(), GetFileSize());

  dir_->nvm_fclose(fd_, "a");

  if (buf_) {
    free(buf_);
  }
  buf_ = nullptr;
  return Status::OK();
}

// We do the caching the backend and sync using direct I/O. Thus, we do not need
// to flush cached data to OS cache.
Status DFlashWritableFile::Flush() {
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

Status DFlashWritableFile::Sync() {
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

Status DFlashWritableFile::Fsync() {
  if (closed_) {
    return Status::IOError("file has been closed");
  }

  if (Flush(false) == false) {
    return Status::IOError("out of ssd space");
  }
  return Status::OK();
}

uint64_t DFlashWritableFile::GetFileSize() {
  if(fd_ == nullptr) {
    return 0;
  }
  DFLASH_DEBUG("FILESIZE: %lu, %lu, %lu\n", fd_->GetPersistentSize(), cursize_, curflush_);
  return fd_->GetPersistentSize() + cursize_ - curflush_;
}

Status DFlashWritableFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
Status DFlashWritableFile::Allocate(off_t offset, off_t len) {
  return Status::OK();
}

Status DFlashWritableFile::RangeSync(off_t offset, off_t nbytes) {
  return Status::OK();
}

size_t DFlashWritableFile::GetUniqueId(char* id, size_t max_size) const {
  if(fd_ == nullptr) {
    return -1;
  }
    
  return GetUniqueIdFromFile(fd_, id, max_size);
}

#endif

FilePrivateMetadata* DFlashWritableFile::GetMetadataHandle() {
  if(fd_ == nullptr) {
    return nullptr;
  }
  return fd_->GetMetadataHandle();
}

} // namespace rocksdb
#endif
