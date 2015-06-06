#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

extern pthread_mutex_t rw_mtx;

#if defined(OS_LINUX)

static size_t GetUniqueIdFromFile(nvm_file *fd, char* id, size_t max_size)
{
    if (max_size < kMaxVarint64Length*3)
    {
	return 0;
    }

    char* rid = id;

    rid = EncodeVarint64(rid, (uint64_t)fd);

    if(rid >= id)
    {
	return static_cast<size_t>(rid - id);
    }
    else
    {
	return static_cast<size_t>(id - rid);
    }
}

#endif

nvm_file::nvm_file(const char *_name, const int fd)
{
    SAFE_ALLOC(name, char[strlen(_name) + 1]);
    strcpy(name, _name);

    size = 0;

    fd_ = fd;

    last_modified = 0;

    first_page = nullptr;

    pthread_mutex_init(&page_update_mtx, nullptr);
}

nvm_file::~nvm_file()
{
    delete[] name;

    //delete all nvm pages in the list
    list_node *temp = first_page;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	delete temp;

	temp = temp1;
    }
}

time_t nvm_file::GetLastModified()
{
    return last_modified;
}

char *nvm_file::GetName()
{
    return name;
}

int nvm_file::GetFD()
{
    return fd_;
}

unsigned long nvm_file::GetSize()
{
    unsigned long ret;

    pthread_mutex_lock(&page_update_mtx);

    ret = size;

    pthread_mutex_unlock(&page_update_mtx);

    return ret;
}

void nvm_file::UpdateFileModificationTime()
{
    last_modified = time(nullptr);
}

void nvm_file::make_dummy(struct nvm *nvm_api)
{
    for(int i = 0; i < 5; ++i)
    {
	for(int j = 0; j < 128; ++j)
	{
	    list_node *temp;
	    ALLOC_CLASS(temp, list_node(&nvm_api->luns[0].blocks[i].pages[j]));

	    size += nvm_api->luns[0].blocks[i].pages[j].sizes[0];

	    if(first_page == nullptr)
	    {
		first_page = temp;
		continue;
	    }

	    first_page->SetPrev(temp);
	    temp->SetNext(first_page);

	    first_page = temp;
	}
    }

    last_modified = time(nullptr);

    list_node *enumerator = first_page;

    while(enumerator != nullptr)
    {
	struct nvm_page *pg = (struct nvm_page *)enumerator->GetData();

	NVM_DEBUG("dummy file has page %lu %lu %lu", pg->lun_id, pg->block_id, pg->id);

	enumerator = enumerator->GetNext();
    }
}

void nvm_file::Delete(struct nvm *nvm_api)
{
    list_node *temp = first_page;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	struct nvm_page *to_reclaim = (struct nvm_page *)temp->GetData();

	nvm_api->ReclaimPage(to_reclaim);

	delete temp;

	temp = temp1;
    }

    first_page = nullptr;
}

struct list_node *nvm_file::GetNVMPagesList()
{
    struct list_node *ret;

    pthread_mutex_lock(&page_update_mtx);

    ret = first_page;

    pthread_mutex_unlock(&page_update_mtx);

    return ret;
}

size_t nvm_file::ReadPage(const nvm_page *page, const unsigned long channel, struct nvm *nvm_api, void *data)
{
    unsigned long offset;

    unsigned long page_size = page->sizes[channel];
    unsigned long block_size = nvm_api->luns[page->lun_id].nr_pages_per_blk * page_size;
    unsigned long lun_size = nvm_api->luns[page->lun_id].nr_blocks * block_size;

    offset = page->lun_id * lun_size + page->block_id * block_size + page->id * page_size;

    pthread_mutex_lock(&rw_mtx);

    if(lseek(fd_, offset, SEEK_SET) < 0)
    {
	pthread_mutex_unlock(&rw_mtx);

	return -1;
    }

    if((unsigned)read(fd_, data, page_size) != page_size)
    {
	pthread_mutex_unlock(&rw_mtx);

	return -1;
    }

    pthread_mutex_unlock(&rw_mtx);

    return page_size;
}

NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *f, NVMFileManager *file_manager, struct nvm *_nvm_api) :
		filename_(fname)
{
    file_ = f;

    file_manager_ = file_manager;

    file_pointer = 0;

    crt_page = file_->GetNVMPagesList();
    page_pointer = 0;

    channel = 0;

    nvm_api = _nvm_api;
}

NVMSequentialFile::~NVMSequentialFile()
{
    file_manager_->nvm_fclose(file_);
}

void NVMSequentialFile::SeekPage(const unsigned long offset)
{
    NVM_ASSERT(crt_page != nullptr, "crt_page is null");

    page_pointer += offset;

    nvm_page *pg = (nvm_page *)crt_page->GetData();

    while(page_pointer >= pg->sizes[channel])
    {
	page_pointer -= pg->sizes[channel];

	crt_page = crt_page->GetNext();

	if(crt_page == nullptr)
	{
	    //we have reached end of file
	    return;
	}

	pg = (nvm_page *)crt_page->GetData();
    }
}

Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch)
{
    if(file_pointer + n > file_->GetSize())
    {
	n = file_->GetSize() - file_pointer;
    }

    if(n <= 0)
    {
	*result = Slice(scratch, 0);
	return Status::OK();
    }

    if(crt_page == nullptr)
    {
	*result = Slice(scratch, 0);
	return Status::OK();
    }

    size_t len = n;
    size_t l;
    size_t scratch_offset = 0;
    size_t size_to_copy;

    char *data;

    while(len > 0)
    {
	nvm_page *pg = (nvm_page *)crt_page->GetData();

	SAFE_ALLOC(data, char[pg->sizes[channel]]);

	l = file_->ReadPage(pg, channel, nvm_api, data);

	if(len > l - page_pointer)
	{
	    size_to_copy = l - page_pointer;
	}
	else
	{
	    size_to_copy = len;
	}

	memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

	len -= size_to_copy;
	scratch_offset += size_to_copy;

	SeekPage(size_to_copy);

	delete[] data;
    }

    file_pointer += n;

    *result = Slice(scratch, n);

    return Status::OK();
}

//n is unsigned -> skip is only allowed forward
Status NVMSequentialFile::Skip(uint64_t n)
{
    if(n == 0)
    {
	return Status::OK();
    }

    if(crt_page == nullptr)
    {
	return Status::IOError(filename_, "EINVAL crt page is null");
    }

    if(file_pointer + n > file_->GetSize())
    {
	return Status::IOError(filename_, "EINVAL file pointer goes out of bounds");
    }

    SeekPage(n - file_pointer);

    file_pointer += n;

    return Status::OK();
}

Status NVMSequentialFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, nvm_file *f, NVMFileManager *file_manager, struct nvm *_nvm_api) :
		    filename_(fname)
{
    file_ = f;

    file_manager_ = file_manager;

    first_page = file_->GetNVMPagesList();

    channel = 0;

    nvm_api = _nvm_api;
}

NVMRandomAccessFile::~NVMRandomAccessFile()
{
    file_manager_->nvm_fclose(file_);
}

struct list_node *NVMRandomAccessFile::SeekPage(const unsigned long offset, unsigned long *page_pointer) const
{
    NVM_ASSERT(first_page != nullptr, "first_page is null");

    *page_pointer = offset;

    struct list_node *crt_page = first_page;

    nvm_page *pg = (nvm_page *)crt_page->GetData();

    while(*page_pointer >= pg->sizes[channel])
    {
	*page_pointer -= pg->sizes[channel];

	crt_page = crt_page->GetNext();

	if(crt_page == nullptr)
	{
	    //we have reached end of file
	    return nullptr;
	}

	pg = (nvm_page *)crt_page->GetData();
    }

    return crt_page;
}

Status NVMRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    unsigned long page_pointer;

    if(offset + n > file_->GetSize())
    {
	n = file_->GetSize() - offset;
    }

    if(n <= 0)
    {
	NVM_DEBUG("n is <= 0");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

    if(first_page == nullptr)
    {
	NVM_DEBUG("first page is null");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

    size_t len = n;
    size_t l;
    size_t scratch_offset = 0;
    size_t size_to_copy;

    char *data;

    struct list_node *crt_page = SeekPage(offset, &page_pointer);

    while(len > 0)
    {
	if(crt_page == nullptr)
	{
	    n -= len;
	    break;
	}

	nvm_page *pg = (nvm_page *)crt_page->GetData();

	SAFE_ALLOC(data, char[pg->sizes[channel]]);

	l = file_->ReadPage(pg, channel, nvm_api, data);

	if(len > l - page_pointer)
	{
	    size_to_copy = l - page_pointer;
	}
	else
	{
	    size_to_copy = len;
	}

	memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

	len -= size_to_copy;
	scratch_offset += size_to_copy;

	crt_page = crt_page->GetNext();
	page_pointer = 0;

	delete[] data;
    }

    NVM_DEBUG("read %lu bytes", n);

    *result = Slice(scratch, n);

    return Status::OK();
}

#ifdef OS_LINUX
size_t NVMRandomAccessFile::GetUniqueId(char* id, size_t max_size) const
{
    return GetUniqueIdFromFile(file_, id, max_size);
}
#endif

void NVMRandomAccessFile::Hint(AccessPattern pattern)
{

}

Status NVMRandomAccessFile::InvalidateCache(size_t offset, size_t length)
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
    //return GetUniqueIdFromFile(fd_, id, max_size);
    return 0;
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
