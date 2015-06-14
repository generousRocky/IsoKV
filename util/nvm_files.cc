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
    char *name;

    SAFE_ALLOC(name, char[strlen(_name) + 1]);
    strcpy(name, _name);

    ALLOC_CLASS(names, list_node(name))

    size = 0;

    fd_ = fd;

    last_modified = 0;

    first_page = nullptr;
    last_page = nullptr;

    opened_for_write = false;

    pthread_mutexattr_init(&page_update_mtx_attr);
    pthread_mutexattr_settype(&page_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);

    pthread_mutex_init(&page_update_mtx, &page_update_mtx_attr);
    pthread_mutex_init(&meta_mtx, &page_update_mtx_attr);
    pthread_mutex_init(&file_lock, &page_update_mtx_attr);
}

nvm_file::~nvm_file()
{
    //delete all nvm pages in the list
    list_node *temp = first_page;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	delete temp;

	temp = temp1;
    }

    temp = names;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	delete[] (char *)temp->GetData();
	delete temp;

	temp = temp1;
    }

    pthread_mutexattr_destroy(&page_update_mtx_attr);
    pthread_mutex_destroy(&page_update_mtx);
    pthread_mutex_destroy(&meta_mtx);
    pthread_mutex_destroy(&file_lock);
}

int nvm_file::LockFile()
{
    return pthread_mutex_lock(&file_lock);
}

void nvm_file::UnlockFile()
{
    pthread_mutex_unlock(&file_lock);
}

bool nvm_file::ClearLastPage(nvm *nvm_api)
{
    pthread_mutex_lock(&page_update_mtx);

    if(last_page == nullptr)
    {
	pthread_mutex_unlock(&page_update_mtx);

	return true;
    }

    struct nvm_page *pg = (struct nvm_page *)last_page->GetData();

    nvm_api->ReclaimPage(pg);

    pg = nvm_api->RequestPage();

    if(pg == nullptr)
    {
	pthread_mutex_unlock(&page_update_mtx);

	return false;
    }

    last_page->SetData(pg);

    pthread_mutex_unlock(&page_update_mtx);

    return true;
}

bool nvm_file::ClaimNewPage(nvm *nvm_api)
{
    struct nvm_page *new_page = nvm_api->RequestPage();

    if(new_page == nullptr)
    {
	return false;
    }

    struct list_node *to_add;

    ALLOC_CLASS(to_add, list_node(new_page));

    to_add->SetPrev(last_page);

    pthread_mutex_lock(&page_update_mtx);

    if(last_page)
    {
	last_page->SetNext(to_add);
    }

    last_page = to_add;

    if(first_page == nullptr)
    {
	first_page = to_add;
    }

    pthread_mutex_unlock(&page_update_mtx);

    return true;
}

time_t nvm_file::GetLastModified()
{
    time_t ret;

    pthread_mutex_lock(&meta_mtx);

    ret = last_modified;

    pthread_mutex_unlock(&meta_mtx);

    return ret;
}

void nvm_file::Close(const char *mode)
{
    if(mode[0] == 'r' || mode[0] == 'l')
    {
	return;
    }

    opened_for_write = false;
}

bool nvm_file::CanOpen(const char *mode)
{
    bool ret = true;

    //file can always be opened for read or lock
    if(mode[0] == 'r' || mode[0] == 'l')
    {
	return ret;
    }

    pthread_mutex_lock(&meta_mtx);

    if(opened_for_write)
    {
	ret = false;
    }
    else
    {
	opened_for_write = true;
    }

    pthread_mutex_unlock(&meta_mtx);

    return ret;
}

void nvm_file::EnumerateNames(std::vector<std::string>* result)
{
    pthread_mutex_lock(&meta_mtx);

    list_node *name_node = names;

    while(name_node)
    {
	char *_name = (char *)name_node->GetData();

	result->push_back(_name);

	name_node = name_node->GetNext();
    }

    pthread_mutex_unlock(&meta_mtx);
}

bool nvm_file::HasName(const char *name, const int n)
{
    int i;

    pthread_mutex_lock(&meta_mtx);

    list_node *name_node = names;

    while(name_node)
    {
	char *_name = (char *)name_node->GetData();

	for(i = 0; i < n; ++i)
	{
	    if(name[i] != _name[i])
	    {
		goto next;
	    }
	}
	if(_name[i] == '\0')
	{
	    pthread_mutex_unlock(&meta_mtx);

	    return true;
	}

next:

	name_node = name_node->GetNext();
    }

    pthread_mutex_unlock(&meta_mtx);

    return false;
}

void nvm_file::AddName(const char *name)
{
    list_node *name_node;

    char *_name;

    SAFE_ALLOC(_name, char[strlen(name) + 1]);
    strcpy(_name, name);

    ALLOC_CLASS(name_node, list_node(_name));

    pthread_mutex_lock(&meta_mtx);

    name_node->SetNext(names);

    if(names)
    {
	names->SetPrev(name_node);
    }

    names = name_node;

    pthread_mutex_unlock(&meta_mtx);
}

void nvm_file::ChangeName(const char *crt_name, const char *new_name)
{
    pthread_mutex_lock(&meta_mtx);

    list_node *name_node = names;

    while(name_node)
    {
	char *crt_name_node = (char *)name_node->GetData();

	if(strcmp(crt_name_node, crt_name) == 0)
	{
	    delete[] crt_name_node;

	    SAFE_ALLOC(crt_name_node, char[strlen(new_name) + 1]);
	    strcpy(crt_name_node, new_name);

	    name_node->SetData(crt_name_node);

	    pthread_mutex_unlock(&meta_mtx);

	    return;
	}

	name_node = name_node->GetNext();
    }

    pthread_mutex_unlock(&meta_mtx);
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
    pthread_mutex_lock(&meta_mtx);

    last_modified = time(nullptr);

    pthread_mutex_unlock(&meta_mtx);
}

void nvm_file::DeleteAllLinks(struct nvm *_nvm_api)
{
    NVM_DEBUG("removing all links in %p", this);

    list_node *temp;

    temp = first_page;

    while(temp != nullptr)
    {
	struct nvm_page *to_reclaim = (struct nvm_page *)temp->GetData();

	_nvm_api->ReclaimPage(to_reclaim);

	temp = temp->GetNext();
    }
}

bool nvm_file::Delete(const char * filename, struct nvm *nvm_api)
{
    bool link_files_left = true;

    list_node *temp;
    list_node *temp1;
    list_node *next;
    list_node *prev;

    pthread_mutex_lock(&meta_mtx);

    temp = names;

    while(temp)
    {
	if(strcmp(filename, (char *)temp->GetData()) == 0)
	{
	    temp1 = temp;

	    prev = temp->GetPrev();
	    next = temp->GetNext();

	    if(prev)
	    {
		prev->SetNext(next);
	    }

	    if(next)
	    {
		next->SetPrev(prev);
	    }

	    if(prev == nullptr && next != nullptr)
	    {
		names = names->GetNext();
	    }

	    if(next == nullptr && prev == nullptr)
	    {
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

    if(link_files_left)
    {
	//we have link file pointing here.. don't delete

	return false;
    }

    pthread_mutex_lock(&page_update_mtx);

    temp = first_page;

    while(temp != nullptr)
    {
	temp1 = temp->GetNext();

	struct nvm_page *to_reclaim = (struct nvm_page *)temp->GetData();

	nvm_api->ReclaimPage(to_reclaim);

	delete temp;

	temp = temp1;
    }

    first_page = nullptr;
    last_page = nullptr;

    size = 0;

    pthread_mutex_unlock(&page_update_mtx);

    return true;
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

size_t nvm_file::WritePage(const nvm_page *page, const unsigned long channel, struct nvm *nvm_api, void *data, const unsigned long data_len)
{
    unsigned long offset;

    unsigned long page_size = page->sizes[channel];

    if(data_len > page_size)
    {
	NVM_FATAL("out of page bounds");
    }

    unsigned long block_size = nvm_api->luns[page->lun_id].nr_pages_per_blk * page_size;
    unsigned long lun_size = nvm_api->luns[page->lun_id].nr_blocks * block_size;

    offset = page->lun_id * lun_size + page->block_id * block_size + page->id * page_size;

    pthread_mutex_lock(&rw_mtx);

    if(lseek(fd_, offset, SEEK_SET) < 0)
    {
	pthread_mutex_unlock(&rw_mtx);

	return -1;
    }

    if((unsigned)write(fd_, data, data_len) != data_len)
    {
	pthread_mutex_unlock(&rw_mtx);

	return -1;
    }

    pthread_mutex_unlock(&rw_mtx);

    pthread_mutex_lock(&page_update_mtx);

    size += data_len;

    pthread_mutex_unlock(&page_update_mtx);

    UpdateFileModificationTime();

    return data_len;
}

NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *f, nvm_directory *_dir, struct nvm *_nvm_api) :
		filename_(fname)
{
    file_ = f;

    dir = _dir;

    file_pointer = 0;

    crt_page = file_->GetNVMPagesList();
    page_pointer = 0;

    channel = 0;

    nvm_api = _nvm_api;
}

NVMSequentialFile::~NVMSequentialFile()
{
    dir->nvm_fclose(file_, "r");
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

NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, nvm_file *f, nvm_directory *_dir, struct nvm *_nvm_api) :
		    filename_(fname)
{
    file_ = f;

    dir = _dir;

    first_page = file_->GetNVMPagesList();

    channel = 0;

    nvm_api = _nvm_api;
}

NVMRandomAccessFile::~NVMRandomAccessFile()
{
    dir->nvm_fclose(file_, "r");
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

NVMWritableFile::NVMWritableFile(const std::string& fname, nvm_file *fd, nvm_directory *dir) :
										filename_(fname)
{
    fd_ = fd;
    dir_ = dir;

    channel = 0;

    buf_ = nullptr;
    last_page = nullptr;

    UpdateLastPage();
}

NVMWritableFile::~NVMWritableFile()
{
    NVMWritableFile::Close();

    if(buf_)
    {
	delete[] buf_;
    }
}

void NVMWritableFile::UpdateLastPage()
{
    nvm_page *pg;

    if(last_page)
    {
	while(last_page->GetNext())
	{
	    last_page = last_page->GetNext();
	}

	pg = (nvm_page *)last_page->GetData();

	bytes_per_sync_ = pg->sizes[channel];
    }
    else
    {
	last_page = fd_->GetNVMPagesList();

	if(last_page == nullptr)
	{
	    goto end;
	}

	bytes_per_sync_ = fd_->GetSize();

	while(last_page->GetNext())
	{
	    pg = (nvm_page *)last_page->GetData();

	    bytes_per_sync_ -= pg->sizes[channel];

	    last_page = last_page->GetNext();
	}

	pg = (nvm_page *)last_page->GetData();

	bytes_per_sync_ = pg->sizes[channel] - bytes_per_sync_;
    }

end:

    cursize_ = 0;

    if(buf_)
    {
	delete[] buf_;
    }

    if(bytes_per_sync_ > 0)
    {
	SAFE_ALLOC(buf_, char[bytes_per_sync_]);
    }
    else
    {
	buf_ = nullptr;
    }
}

bool NVMWritableFile::Flush(const bool forced)
{
    struct nvm_page *pg;

    if(bytes_per_sync_ == 0)
    {
	if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	{
	    return false;
	}

	UpdateLastPage();
    }

    if(cursize_ == bytes_per_sync_ || forced)
    {
	pg = (struct nvm_page *)last_page->GetData();

	if(pg->sizes[channel] == bytes_per_sync_)
	{
	    if(fd_->WritePage(pg, channel, dir_->GetNVMApi(), buf_, cursize_) != cursize_)
	    {
		NVM_DEBUG("unable to write data");
		return false;
	    }

	    if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	    {
		return false;
	    }

	    UpdateLastPage();

	    return true;
	}

	char *crt_data;
	SAFE_ALLOC(crt_data, char[pg->sizes[channel]]);

	fd_->ReadPage(pg, channel, dir_->GetNVMApi(), crt_data);

	unsigned long crt_data_len = pg->sizes[channel] - bytes_per_sync_;

	memcpy(crt_data + crt_data_len, buf_, bytes_per_sync_);

	crt_data_len = pg->sizes[channel];

	if(fd_->ClearLastPage(dir_->GetNVMApi()) == false)
	{
	    return false;
	}

	pg = (struct nvm_page *)last_page->GetData();

	if(fd_->WritePage(pg, channel, dir_->GetNVMApi(), crt_data, crt_data_len) != crt_data_len)
	{
	    NVM_DEBUG("unable to write data");
	    return false;
	}

	if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	{
	    return false;
	}

	UpdateLastPage();

	return true;
    }

    return true;
}

Status NVMWritableFile::Append(const Slice& data)
{
    const char* src = data.data();

    size_t left = data.size();
    size_t offset = 0;

    while(left > 0)
    {
	if(cursize_ + left < bytes_per_sync_)
	{
	    memcpy(buf_, src + offset, left);

	    cursize_ += left;

	    left = 0;
	}
	else
	{
	    memcpy(buf_, src + offset, bytes_per_sync_ - cursize_);

	    cursize_ = bytes_per_sync_;

	    if(Flush(false) == false)
	    {
		return Status::IOError("out of ssd space");
	    }

	    left -= bytes_per_sync_ - cursize_;
	    offset += bytes_per_sync_ - cursize_;
	}
    }

    if(Flush(false) == false)
    {
	return Status::IOError("out of ssd space");
    }

    return Status::OK();
}

Status NVMWritableFile::Close()
{
    if(Flush(true) == false)
    {
	return Status::IOError("out of ssd space");
    }

    dir_->nvm_fclose(fd_, "w");

    return Status::OK();
}

Status NVMWritableFile::Flush()
{
    if(Flush(false) == false)
    {
	return Status::IOError("out of ssd space");
    }

    return Status::OK();
}

Status NVMWritableFile::Sync()
{
    if(Flush(false) == false)
    {
	return Status::IOError("out of ssd space");
    }

    return Status::OK();
}

Status NVMWritableFile::Fsync()
{
    if(Flush(false) == false)
    {
	return Status::IOError("out of ssd space");
    }

    return Status::OK();
}

uint64_t NVMWritableFile::GetFileSize()
{
    return fd_->GetSize() + cursize_;
}

Status NVMWritableFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMWritableFile::Allocate(off_t offset, off_t len)
{
    return Status::OK();
}

Status NVMWritableFile::RangeSync(off_t offset, off_t nbytes)
{
    return Status::OK();
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

NVMDirectory::NVMDirectory(nvm_directory *fd)
{
    fd_ = fd;
}

NVMDirectory::~NVMDirectory()
{

}

Status NVMDirectory::Fsync()
{
    return Status::OK();
}

}

#endif
