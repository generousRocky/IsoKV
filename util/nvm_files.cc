#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

#if defined(OS_LINUX)

static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size)
{
    if (max_size < kMaxVarint64Length*3)
    {
	return 0;
    }

    struct stat buf;

    int result = fstat(fd, &buf);
    if (result == -1)
    {
	return 0;
    }

    long version = 0;

    result = ioctl(fd, FS_IOC_GETVERSION, &version);
    if (result == -1)
    {
	return 0;
    }

    uint64_t uversion = (uint64_t)version;

    char* rid = id;

    rid = EncodeVarint64(rid, buf.st_dev);
    rid = EncodeVarint64(rid, buf.st_ino);
    rid = EncodeVarint64(rid, uversion);
    assert(rid >= id);

    return static_cast<size_t>(rid-id);
}

#endif

nvm_file::nvm_file(const char *_name)
{
    SAFE_ALLOC(name, char[strlen(_name) + 1]);
    strcpy(name, _name);

    size = 0;

    first_page = nullptr;
}

nvm_file::~nvm_file()
{
    delete[] name;
}

char *nvm_file::GetName()
{
    return name;
}

unsigned long nvm_file::GetSize()
{
    return size;
}

struct list_node *nvm_file::GetNVMPagesList()
{
    return first_page;
}

NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *f, const EnvOptions& options, NVMFileManager *file_manager) :
		filename_(fname),
		use_os_buffer_(options.use_os_buffer)
{
    file_ = f;

    file_manager_ = file_manager;

    file_pointer = 0;
}

NVMSequentialFile::~NVMSequentialFile()
{
    file_manager_->nvm_fclose(file_);
}

Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch)
{
    size_t r = 0;

    if(file_pointer < file_->GetSize())
    {
	r = file_manager_->nvm_fread(scratch, file_pointer, n, file_);
	file_pointer += r;

	if(r == 0)
	{
	    return IOError(filename_, errno);
	}
    }

    IOSTATS_ADD(bytes_read, r);
    *result = Slice(scratch, r);

    return Status::OK();
}

Status NVMSequentialFile::Skip(uint64_t n)
{
    if(file_pointer + n >= file_->GetSize())
    {
	file_pointer = file_->GetSize();
    }
    else
    {
	file_pointer += n;
    }

    return Status::OK();
}

Status NVMSequentialFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

// pread() based random-access

NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, int fd, const EnvOptions& options) :
		    filename_(fname),
		    fd_(fd),
		    use_os_buffer_(options.use_os_buffer)
{
    assert(!options.use_mmap_reads || sizeof(void*) < 8);
}

NVMRandomAccessFile::~NVMRandomAccessFile()
{
    close(fd_);
}

Status NVMRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    Status s;

    ssize_t r = -1;

    size_t left = n;

    char* ptr = scratch;

    while (left > 0)
    {
	r = pread(fd_, ptr, left, static_cast<off_t>(offset));
	if (r <= 0)
	{
	    if (errno == EINTR)
	    {
		continue;
	    }
	    break;
	}
	ptr += r;
	offset += r;
	left -= r;
    }

    IOSTATS_ADD_IF_POSITIVE(bytes_read, n - left);

    *result = Slice(scratch, (r < 0) ? 0 : n - left);
    if (r < 0)
    {
	// An error: return a non-ok status
	s = IOError(filename_, errno);
    }
    return s;
}

#ifdef OS_LINUX
size_t NVMRandomAccessFile::GetUniqueId(char* id, size_t max_size) const
{
    return GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void NVMRandomAccessFile::Hint(AccessPattern pattern)
{

}

Status NVMRandomAccessFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}


	    // base[0,length-1] contains the mmapped contents of the file.
NVMMmapReadableFile::NVMMmapReadableFile(const int fd, const std::string& fname, void* base, size_t length, const EnvOptions& options) :
		fd_(fd),
		filename_(fname),
		mmapped_region_(base),
		length_(length)
{
    fd_ = fd_ + 0;  // suppress the warning for used variables

    assert(options.use_mmap_reads);
    assert(options.use_os_buffer);
}

NVMMmapReadableFile::~NVMMmapReadableFile()
{
    int ret = munmap(mmapped_region_, length_);
    if (ret != 0)
    {
	fprintf(stdout, "failed to munmap %p length %zu \n", mmapped_region_, length_);
    }
}

Status NVMMmapReadableFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    Status s;

    if (offset + n > length_)
    {
	*result = Slice();
	s = IOError(filename_, EINVAL);
    }
    else
    {
	*result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
}

Status NVMMmapReadableFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

// Roundup x to a multiple of y
size_t NVMMmapFile::Roundup(size_t x, size_t y)
{
    return ((x + y - 1) / y) * y;
}

size_t NVMMmapFile::TruncateToPageBoundary(size_t s)
{
    s -= (s & (page_size_ - 1));

    assert((s % page_size_) == 0);

    return s;
}

Status NVMMmapFile::UnmapCurrentRegion()
{
    if (base_ != nullptr)
    {
	if (last_sync_ < limit_)
	{
	    // Defer syncing this data until next Sync() call, if any
	    pending_sync_ = true;
	}

	int munmap_status = munmap(base_, limit_ - base_);
	if (munmap_status != 0)
	{
	    return IOError(filename_, munmap_status);
	}
	file_offset_ += limit_ - base_;

	base_ = nullptr;
	limit_ = nullptr;
	last_sync_ = nullptr;
	dst_ = nullptr;

	// Increase the amount we map the next time, but capped at 1MB
	if (map_size_ < (1<<20))
	{
	    map_size_ *= 2;
	}
    }
    return Status::OK();
}

Status NVMMmapFile::MapNewRegion()
{
#ifdef ROCKSDB_FALLOCATE_PRESENT
    assert(base_ == nullptr);

    // we can't fallocate with FALLOC_FL_KEEP_SIZE here
    int alloc_status = fallocate(fd_, 0, file_offset_, map_size_);
    if (alloc_status != 0)
    {
	// fallback to posix_fallocate
	alloc_status = posix_fallocate(fd_, file_offset_, map_size_);
    }

    if (alloc_status != 0)
    {
	return Status::IOError("Error allocating space to file : " + filename_ + "Error : " + strerror(alloc_status));
    }

    void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, file_offset_);
    if (ptr == MAP_FAILED)
    {
	return Status::IOError("MMap failed on " + filename_);
    }

    base_ = reinterpret_cast<char*>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_;
    last_sync_ = base_;
    return Status::OK();

#else

    return Status::NotSupported("This platform doesn't support fallocate()");

#endif
}

NVMMmapFile::NVMMmapFile(const std::string& fname, int fd, size_t page_size, const EnvOptions& options) :
	    filename_(fname),
	    fd_(fd),
	    page_size_(page_size),
	    map_size_(Roundup(65536, page_size)),
	    base_(nullptr),
	    limit_(nullptr),
	    dst_(nullptr),
	    last_sync_(nullptr),
	    file_offset_(0),
	    pending_sync_(false)
{
#ifdef ROCKSDB_FALLOCATE_PRESENT

    fallocate_with_keep_size_ = options.fallocate_with_keep_size;

#endif

    assert((page_size & (page_size - 1)) == 0);
    assert(options.use_mmap_writes);
}


NVMMmapFile::~NVMMmapFile()
{
    if (fd_ >= 0)
    {
	NVMMmapFile::Close();
    }
}

Status NVMMmapFile::Append(const Slice& data)
{
    const char* src = data.data();

    size_t left = data.size();

    PrepareWrite(static_cast<size_t>(GetFileSize()), left);

    while (left > 0)
    {
	assert(base_ <= dst_);
	assert(dst_ <= limit_);

	size_t avail = limit_ - dst_;

	if (avail == 0)
	{
	    Status s = UnmapCurrentRegion();
	    if (!s.ok())
	    {
		return s;
	    }

	    s = MapNewRegion();
	    if (!s.ok())
	    {
		return s;
	    }
	}

	size_t n = (left <= avail) ? left : avail;

	memcpy(dst_, src, n);
	IOSTATS_ADD(bytes_written, n);

	dst_ += n;
	src += n;
	left -= n;
    }

    return Status::OK();
}

Status NVMMmapFile::Close()
{
    Status s;

    size_t unused = limit_ - dst_;

    s = UnmapCurrentRegion();
    if (!s.ok())
    {
	s = IOError(filename_, errno);
    }
    else if (unused > 0)
    {
	// Trim the extra space at the end of the file
	if (ftruncate(fd_, file_offset_ - unused) < 0)
	{
	    s = IOError(filename_, errno);
	}
    }

    if (close(fd_) < 0)
    {
	if (s.ok())
	{
	    s = IOError(filename_, errno);
	}
    }

    fd_ = -1;

    base_ = nullptr;
    limit_ = nullptr;

    return s;
}

Status NVMMmapFile::Flush()
{
    return Status::OK();
}

Status NVMMmapFile::Sync()
{
    Status s;

    if (pending_sync_)
    {
	// Some unmapped data was not synced
	pending_sync_ = false;

	if (fdatasync(fd_) < 0)
	{
	    s = IOError(filename_, errno);
	}
    }

    if (dst_ > last_sync_)
    {
	// Find the beginnings of the pages that contain the first and last
	// bytes to be synced.

	size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
	size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);

	last_sync_ = dst_;
	if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0)
	{
	    s = IOError(filename_, errno);
	}
    }

    return s;
}

/**
 * Flush data as well as metadata to stable storage.
 */
Status NVMMmapFile::Fsync()
{
    if (pending_sync_)
    {
	// Some unmapped data was not synced
	pending_sync_ = false;

	if (fsync(fd_) < 0)
	{
	    return IOError(filename_, errno);
	}
    }
    // This invocation to Sync will not issue the call to
    // fdatasync because pending_sync_ has already been cleared.
    return Sync();
}

/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t NVMMmapFile::GetFileSize()
{
    size_t used = dst_ - base_;
    return file_offset_ + used;
}

Status NVMMmapFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMMmapFile::Allocate(off_t offset, off_t len)
{
    int alloc_status = fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
    if (alloc_status == 0)
    {
	return Status::OK();
    }
    else
    {
	return IOError(filename_, errno);
    }
}
#endif

NVMWritableFile::NVMWritableFile(const std::string& fname, int fd, size_t capacity, const EnvOptions& options) :
	    filename_(fname),
	    fd_(fd),
	    cursize_(0),
	    capacity_(capacity),
	    buf_(new char[capacity]),
	    filesize_(0),
	    pending_sync_(false),
	    pending_fsync_(false),
	    last_sync_size_(0),
	    bytes_per_sync_(options.bytes_per_sync),
	    rate_limiter_(options.rate_limiter)
{
#ifdef ROCKSDB_FALLOCATE_PRESENT

    fallocate_with_keep_size_ = options.fallocate_with_keep_size;

#endif

    assert(!options.use_mmap_writes);
}

NVMWritableFile::~NVMWritableFile()
{
    if (fd_ >= 0)
    {
	NVMWritableFile::Close();
    }
}

inline size_t NVMWritableFile::RequestToken(size_t bytes)
{
    if (rate_limiter_ && io_priority_ < Env::IO_TOTAL)
    {
	bytes = std::min(bytes, static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));
	rate_limiter_->Request(bytes, io_priority_);
    }
    return bytes;
}

Status NVMWritableFile::Append(const Slice& data)
{
    const char* src = data.data();

    size_t left = data.size();

    Status s;

    pending_sync_ = true;
    pending_fsync_ = true;

    PrepareWrite(static_cast<size_t>(GetFileSize()), left);

    // if there is no space in the cache, then flush
    if (cursize_ + left > capacity_)
    {
	s = Flush();
	if (!s.ok())
	{
	    return s;
	}

	// Increase the buffer size, but capped at 1MB
	if (capacity_ < (1 << 20))
	{
	    capacity_ *= 2;
	    buf_.reset(new char[capacity_]);
	}
	assert(cursize_ == 0);
    }

    // if the write fits into the cache, then write to cache
    // otherwise do a write() syscall to write to OS buffers.
    if (cursize_ + left <= capacity_)
    {
	memcpy(buf_.get() + cursize_, src, left);
	cursize_ += left;
    }
    else
    {
	while (left != 0)
	{
	    ssize_t done = write(fd_, src, RequestToken(left));
	    if (done < 0)
	    {
		if (errno == EINTR)
		{
		    continue;
		}
		return IOError(filename_, errno);
	    }
	    IOSTATS_ADD(bytes_written, done);

	    left -= done;
	    src += done;
	}
    }

    filesize_ += data.size();
    return Status::OK();
}

Status NVMWritableFile::Close()
{
    Status s;

    s = Flush(); // flush cache to OS
    if (!s.ok())
    {
	return s;
    }

    size_t block_size;
    size_t last_allocated_block;

    GetPreallocationStatus(&block_size, &last_allocated_block);
    if (last_allocated_block > 0)
    {
	// trim the extra space preallocated at the end of the file
	// NOTE(ljin): we probably don't want to surface failure as an IOError,
	// but it will be nice to log these errors.
	int dummy __attribute__((unused));
	dummy = ftruncate(fd_, filesize_);

#ifdef ROCKSDB_FALLOCATE_PRESENT

	// in some file systems, ftruncate only trims trailing space if the
	// new file size is smaller than the current size. Calling fallocate
	// with FALLOC_FL_PUNCH_HOLE flag to explicitly release these unused
	// blocks. FALLOC_FL_PUNCH_HOLE is supported on at least the following
	// filesystems:
	//   XFS (since Linux 2.6.38)
	//   ext4 (since Linux 3.0)
	//   Btrfs (since Linux 3.7)
	//   tmpfs (since Linux 3.5)
	// We ignore error since failure of this operation does not affect
	// correctness.
	fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_, block_size * last_allocated_block - filesize_);

#endif
    }

    if (close(fd_) < 0)
    {
	s = IOError(filename_, errno);
    }

    fd_ = -1;
    return s;
}

// write out the cached data to the OS cache
Status NVMWritableFile::Flush()
{
    size_t left = cursize_;

    char* src = buf_.get();

    while (left != 0)
    {
	ssize_t done = write(fd_, src, RequestToken(left));
	if (done < 0)
	{
	    if (errno == EINTR)
	    {
		continue;
	    }
	    return IOError(filename_, errno);
	}
	IOSTATS_ADD(bytes_written, done);

	left -= done;
	src += done;
    }
    cursize_ = 0;

    // sync OS cache to disk for every bytes_per_sync_
    // TODO: give log file and sst file different options (log
    // files could be potentially cached in OS for their whole
    // life time, thus we might not want to flush at all).
    if (bytes_per_sync_ && filesize_ - last_sync_size_ >= bytes_per_sync_)
    {
	RangeSync(last_sync_size_, filesize_ - last_sync_size_);
	last_sync_size_ = filesize_;
    }

    return Status::OK();
}

Status NVMWritableFile::Sync()
{
    Status s = Flush();
    if (!s.ok())
    {
	return s;
    }

    if (pending_sync_ && fdatasync(fd_) < 0)
    {
	return IOError(filename_, errno);
    }

    pending_sync_ = false;
    return Status::OK();
}

Status NVMWritableFile::Fsync()
{
    Status s = Flush();
    if (!s.ok())
    {
	return s;
    }

    if (pending_fsync_ && fsync(fd_) < 0)
    {
	return IOError(filename_, errno);
    }

    pending_fsync_ = false;
    pending_sync_ = false;
    return Status::OK();
}

uint64_t NVMWritableFile::GetFileSize()
{
    return filesize_;
}

Status NVMWritableFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMWritableFile::Allocate(off_t offset, off_t len)
{
    int alloc_status = fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
    if (alloc_status == 0)
    {
	return Status::OK();
    }
    else
    {
	return IOError(filename_, errno);
    }
}

Status NVMWritableFile::RangeSync(off_t offset, off_t nbytes)
{
    if (sync_file_range(fd_, offset, nbytes, SYNC_FILE_RANGE_WRITE) == 0)
    {
	return Status::OK();
    }
    else
    {
	return IOError(filename_, errno);
    }
}

size_t NVMWritableFile::GetUniqueId(char* id, size_t max_size) const
{
    return GetUniqueIdFromFile(fd_, id, max_size);
}

#endif


NVMRandomRWFile::NVMRandomRWFile(const std::string& fname, int fd, const EnvOptions& options) :
	    filename_(fname),
	    fd_(fd),
	    pending_sync_(false),
	    pending_fsync_(false)
{
#ifdef ROCKSDB_FALLOCATE_PRESENT

    fallocate_with_keep_size_ = options.fallocate_with_keep_size;

#endif
    assert(!options.use_mmap_writes && !options.use_mmap_reads);
}

NVMRandomRWFile::~NVMRandomRWFile()
{
    if (fd_ >= 0)
    {
	Close();
    }
}

Status NVMRandomRWFile::Write(uint64_t offset, const Slice& data)
{
    const char* src = data.data();

    size_t left = data.size();

    Status s;

    pending_sync_ = true;
    pending_fsync_ = true;

    while (left != 0)
    {
	ssize_t done = pwrite(fd_, src, left, offset);
	if (done < 0)
	{
	    if (errno == EINTR)
	    {
		continue;
	    }
	    return IOError(filename_, errno);
	}
	IOSTATS_ADD(bytes_written, done);

	left -= done;
	src += done;
	offset += done;
    }

    return Status::OK();
}

Status NVMRandomRWFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    Status s;

    ssize_t r = -1;

    size_t left = n;

    char* ptr = scratch;

    while (left > 0)
    {
	r = pread(fd_, ptr, left, static_cast<off_t>(offset));
	if (r <= 0)
	{
	    if (errno == EINTR)
	    {
		continue;
	    }
	    break;
	}

	ptr += r;
	offset += r;
	left -= r;
    }

    IOSTATS_ADD_IF_POSITIVE(bytes_read, n - left);
    *result = Slice(scratch, (r < 0) ? 0 : n - left);
    if (r < 0)
    {
	s = IOError(filename_, errno);
    }
    return s;
}

Status NVMRandomRWFile::Close()
{
    Status s = Status::OK();

    if (fd_ >= 0 && close(fd_) < 0)
    {
	s = IOError(filename_, errno);
    }

    fd_ = -1;
    return s;
}

Status NVMRandomRWFile::Sync()
{
    if (pending_sync_ && fdatasync(fd_) < 0)
    {
	return IOError(filename_, errno);
    }

    pending_sync_ = false;
    return Status::OK();
}

Status NVMRandomRWFile::Fsync()
{
    if (pending_fsync_ && fsync(fd_) < 0)
    {
	return IOError(filename_, errno);
    }

    pending_fsync_ = false;
    pending_sync_ = false;
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMRandomRWFile::Allocate(off_t offset, off_t len)
{
    int alloc_status = fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0, offset, len);
    if (alloc_status == 0)
    {
	return Status::OK();
    }
    else
    {
	return IOError(filename_, errno);
    }
}
#endif

NVMDirectory::NVMDirectory(int fd) :
		    fd_(fd)
{
}

NVMDirectory::~NVMDirectory()
{
    close(fd_);
}

Status NVMDirectory::Fsync()
{
    if (fsync(fd_) == -1)
    {
	return IOError("directory", errno);
    }
    return Status::OK();
}

}

#endif
