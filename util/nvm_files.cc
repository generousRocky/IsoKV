#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

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

nvm_file::nvm_file(const char *_name, const int fd, nvm_directory *_parent)
{
    char *name;

    NVM_DEBUG("constructing file %s in %s", _name, _parent == nullptr ? "NULL" : _parent->GetName());

    SAFE_ALLOC(name, char[strlen(_name) + 1]);
    strcpy(name, _name);

    ALLOC_CLASS(names, list_node(name))

    size = 0;

    fd_ = fd;

    last_modified = time(nullptr);

    pages.clear();

    opened_for_write = false;

    parent = _parent;

    pthread_mutexattr_init(&page_update_mtx_attr);
    pthread_mutexattr_settype(&page_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);

    pthread_mutex_init(&page_update_mtx, &page_update_mtx_attr);
    pthread_mutex_init(&meta_mtx, &page_update_mtx_attr);
    pthread_mutex_init(&file_lock, &page_update_mtx_attr);
}

nvm_file::~nvm_file()
{
    //delete all nvm pages in the vector
    pages.clear();

    list_node *temp = names;

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

void nvm_file::SetParent(nvm_directory *_parent)
{
    parent = _parent;
}

nvm_directory *nvm_file::GetParent()
{
    return parent;
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

    if(pages.size() == 0)
    {
	pthread_mutex_unlock(&page_update_mtx);

	return true;
    }

    struct nvm_page *pg = pages[pages.size() - 1];

    nvm_api->ReclaimPage(pg);

    pg = nvm_api->RequestPage();

    if(pg == nullptr)
    {
	pthread_mutex_unlock(&page_update_mtx);

	return false;
    }

    pages[pages.size() - 1] = pg;

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

    pthread_mutex_lock(&page_update_mtx);

    pages.push_back(new_page);

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

	NVM_DEBUG("change name %s vs %s", crt_name_node, crt_name);

	if(strcmp(crt_name_node, crt_name) == 0)
	{
	    NVM_DEBUG("MATCH");

	    delete[] crt_name_node;

	    SAFE_ALLOC(crt_name_node, char[strlen(new_name) + 1]);
	    strcpy(crt_name_node, new_name);

	    name_node->SetData(crt_name_node);

	    NVM_DEBUG("SET DATA %s", crt_name_node);

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

    pthread_mutex_lock(&page_update_mtx);

    for(unsigned long i = 0; i < pages.size(); ++i)
    {
	_nvm_api->ReclaimPage(pages[i]);
    }

    pages.clear();

    size = 0;

    pthread_mutex_unlock(&page_update_mtx);

    UpdateFileModificationTime();
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

    DeleteAllLinks(nvm_api);

    return true;
}

struct nvm_page *nvm_file::GetLastPage(unsigned long *page_idx)
{
    struct nvm_page *ret;

    pthread_mutex_lock(&page_update_mtx);

    if(pages.size() == 0)
    {
	ret = nullptr;
    }
    else
    {
	ret = pages[pages.size() - 1];

	*page_idx = pages.size() - 1;
    }

    pthread_mutex_unlock(&page_update_mtx);

    return ret;
}

bool nvm_file::SetPage(const unsigned long page_idx, nvm_page *page)
{
    bool ret = false;

    pthread_mutex_lock(&page_update_mtx);

    if(pages.size() > page_idx)
    {
	pages[page_idx] = page;

	ret = true;
    }

    pthread_mutex_unlock(&page_update_mtx);

    return ret;
}

struct nvm_page *nvm_file::GetNVMPage(const unsigned long idx)
{
    struct nvm_page *ret;

    pthread_mutex_lock(&page_update_mtx);

    if(idx < pages.size())
    {
	ret = pages[idx];
    }
    else
    {
	ret = nullptr;
    }

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

    NVM_DEBUG("reading %lu bytes at %lu", page_size, offset);

retry:

    if((unsigned)pread(fd_, data, page_size, offset) != page_size)
    {
	if (errno == EINTR)
	{
	    goto retry;
	}
	return -1;
    }

    IOSTATS_ADD(bytes_read, page_size);

    return page_size;
}

size_t nvm_file::WritePage(struct nvm_page *&page, const unsigned long channel, struct nvm *nvm_api, void *data, const unsigned long data_len)
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

retry:

    NVM_DEBUG("writing page %p", page);

    if((unsigned)pwrite(fd_, data, data_len, offset) != data_len)
    {
	if (errno == EINTR)
	{
	    //EINTR may cause a stale page -> replace page

	    nvm_api->ReclaimPage(page);

	    page = nvm_api->RequestPage();

	    if(page == nullptr)
	    {
		return -1;
	    }

	    goto retry;
	}

	NVM_ERROR("unable to write data");
	return -1;
    }

    pthread_mutex_lock(&page_update_mtx);

    size += data_len;

    pthread_mutex_unlock(&page_update_mtx);

    UpdateFileModificationTime();

    IOSTATS_ADD(bytes_written, data_len);

    return data_len;
}

NVMSequentialFile::NVMSequentialFile(const std::string& fname, nvm_file *f, nvm_directory *_dir) :
		filename_(fname)
{
    file_ = f;

    dir = _dir;

    file_pointer = 0;

    crt_page_idx = 0;
    crt_page = file_->GetNVMPage(0);
    page_pointer = 0;

    channel = 0;

    nvm_api = _dir->GetNVMApi();

    NVM_DEBUG("created %s", fname.c_str());
}

NVMSequentialFile::~NVMSequentialFile()
{
    dir->nvm_fclose(file_, "r");
}

void NVMSequentialFile::SeekPage(const unsigned long offset)
{
    if(crt_page == nullptr)
    {
	crt_page = file_->GetNVMPage(crt_page_idx);

	if(crt_page == nullptr)
	{
	    return;
	}
    }

    page_pointer += offset;

    crt_page_idx += (unsigned long)(page_pointer / crt_page->sizes[channel]);

    crt_page = file_->GetNVMPage(crt_page_idx);

    page_pointer %= crt_page->sizes[channel];
}

Status NVMSequentialFile::Read(size_t n, Slice* result, char* scratch)
{
    NVM_DEBUG("File pointer is %lu, n is %lu, file size is %lu", file_pointer, n, file_->GetSize())

    if(file_pointer + n > file_->GetSize())
    {
	n = file_->GetSize() - file_pointer;
    }

    if(n <= 0)
    {
	*result = Slice(scratch, 0);
	return Status::OK();
    }

    SeekPage(0);

    if(crt_page == nullptr)
    {
	NVM_DEBUG("crt_page is null");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

    size_t len = n;
    size_t l;
    size_t scratch_offset = 0;
    size_t size_to_copy;

    char *data;

    SAFE_ALLOC(data, char[crt_page->sizes[channel]]);

    while(len > 0)
    {
	l = file_->ReadPage(crt_page, channel, nvm_api, data);

	if(len > l - page_pointer)
	{
	    size_to_copy = l - page_pointer;
	}
	else
	{
	    size_to_copy = len;
	}

	NVM_DEBUG("copy %lu to scratch from offset %lu", size_to_copy, scratch_offset);

	memcpy(scratch + scratch_offset, data + page_pointer, size_to_copy);

	len -= size_to_copy;
	scratch_offset += size_to_copy;

	SeekPage(size_to_copy);

	NVM_DEBUG("page pointer becomes %lu and page is %p", page_pointer, crt_page);
    }

    delete[] data;

    file_pointer += n;

    NVM_DEBUG("creating slice with %lu bytes", n);

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

    if(file_pointer + n > file_->GetSize())
    {
	return Status::IOError(filename_, "EINVAL file pointer goes out of bounds");
    }

    SeekPage(n);

    file_pointer += n;

    NVM_DEBUG("SEEKED %lu forward; file pointer is %lu, page pointer is %lu", n, file_pointer, page_pointer);

    return Status::OK();
}

Status NVMSequentialFile::InvalidateCache(size_t offset, size_t length)
{
    return Status::OK();
}

NVMRandomAccessFile::NVMRandomAccessFile(const std::string& fname, nvm_file *f, nvm_directory *_dir) :
		    filename_(fname)
{
    file_ = f;

    dir = _dir;

    channel = 0;

    nvm_api = dir->GetNVMApi();

    NVM_DEBUG("created %s", fname.c_str());
}

NVMRandomAccessFile::~NVMRandomAccessFile()
{
    dir->nvm_fclose(file_, "r");
}

struct nvm_page *NVMRandomAccessFile::SeekPage(const unsigned long offset, unsigned long *page_pointer, unsigned long *page_idx) const
{
    struct nvm_page *crt_page = file_->GetNVMPage(0);

    *page_pointer = offset;

    *page_idx = (unsigned long)(*page_pointer / crt_page->sizes[channel]);

    *page_pointer %= crt_page->sizes[channel];

    crt_page = file_->GetNVMPage(*page_idx);

    return crt_page;
}

Status NVMRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    unsigned long page_pointer;

    if(offset >= file_->GetSize())
    {
	NVM_DEBUG("offset is out of bounds");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

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

    size_t len = n;
    size_t l;
    size_t scratch_offset = 0;
    size_t size_to_copy;

    char *data;

    unsigned long page_idx;

    struct nvm_page *crt_page = SeekPage(offset, &page_pointer, &page_idx);

    if(crt_page == nullptr)
    {
	*result = Slice(scratch, 0);
	return Status::OK();
    }

    SAFE_ALLOC(data, char[crt_page->sizes[channel]]);

    while(len > 0)
    {
	if(crt_page == nullptr)
	{
	    n -= len;
	    break;
	}

	l = file_->ReadPage(crt_page, channel, nvm_api, data);

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

	crt_page = file_->GetNVMPage(++page_idx);
	page_pointer = 0;
    }

    delete[] data;

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
    last_page_idx = 0;

    UpdateLastPage();

    NVM_DEBUG("created %s", fname.c_str());
}

NVMWritableFile::~NVMWritableFile()
{
    NVMWritableFile::Close();

    if(buf_)
    {
	delete[] buf_;
    }

    buf_ = nullptr;
}

void NVMWritableFile::UpdateLastPage()
{
    nvm_page *pg;

    if(last_page)
    {
	NVM_DEBUG("last page is not null, iterating");

	pg = fd_->GetLastPage(&last_page_idx);

	NVM_ASSERT(pg != last_page, "last page did not update");

	last_page = pg;

	bytes_per_sync_ = pg->sizes[channel];
    }
    else
    {
	last_page = fd_->GetLastPage(&last_page_idx);

	NVM_DEBUG("last page was null and now is %p", last_page);

	if(last_page == nullptr)
	{
	    bytes_per_sync_ = 0;

	    goto end;
	}

	bytes_per_sync_ = (last_page_idx + 1) * last_page->sizes[channel] - fd_->GetSize();
    }
end:
    NVM_DEBUG("last page is at %p", last_page);

    cursize_ = 0;

    if(buf_)
    {
	delete[] buf_;

	buf_ = nullptr;
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
    if(cursize_ == 0)
    {
	return true;
    }

    if(bytes_per_sync_ == 0)
    {
	if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	{
	    return false;
	}

	UpdateLastPage();
    }

    if(last_page == nullptr)
    {
	NVM_FATAL("last page is null, cursize is %lu, byte_per_sync is %lu", cursize_, bytes_per_sync_);
    }

    if(cursize_ == bytes_per_sync_ || forced)
    {
	if(last_page->sizes[channel] == bytes_per_sync_)
	{
	    struct nvm_page *wrote_pg = last_page;

	    if(fd_->WritePage(wrote_pg, channel, dir_->GetNVMApi(), buf_, cursize_) != cursize_)
	    {
		NVM_DEBUG("unable to write data");
		return false;
	    }

	    if(last_page != wrote_pg)
	    {
		last_page = wrote_pg;

		if(fd_->SetPage(last_page_idx, last_page) == false)
		{
		    NVM_FATAL("unable to update last page after EINTR");
		}
	    }

	    if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	    {
		return false;
	    }

	    UpdateLastPage();

	    return true;
	}

	char *crt_data;
	SAFE_ALLOC(crt_data, char[last_page->sizes[channel]]);

	fd_->ReadPage(last_page, channel, dir_->GetNVMApi(), crt_data);

	unsigned long crt_data_len = last_page->sizes[channel] - bytes_per_sync_;

	memcpy(crt_data + crt_data_len, buf_, bytes_per_sync_);

	crt_data_len = last_page->sizes[channel];

	if(fd_->ClearLastPage(dir_->GetNVMApi()) == false)
	{
	    return false;
	}

	struct nvm_page *wrote_pg = last_page;

	if(fd_->WritePage(wrote_pg, channel, dir_->GetNVMApi(), crt_data, crt_data_len) != crt_data_len)
	{
	    NVM_DEBUG("unable to write data");
	    return false;
	}

	if(last_page != wrote_pg)
	{
	    last_page = wrote_pg;

	    if(fd_->SetPage(last_page_idx, last_page) == false)
	    {
		NVM_FATAL("unable to update last page after EINTR");
	    }
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

    if(bytes_per_sync_ == 0)
    {
	if(fd_->ClaimNewPage(dir_->GetNVMApi()) == false)
	{
	    return Status::IOError("out of ssd space");
	}

	UpdateLastPage();
    }

    while(left > 0)
    {
	NVM_DEBUG("Appending slice %lu bytes", left);

	if(cursize_ + left <= bytes_per_sync_)
	{
	    NVM_DEBUG("All in buffer from %lu to %p", cursize_, buf_);

	    memcpy(buf_ + cursize_, src + offset, left);

	    cursize_ += left;

	    left = 0;	    
	}
	else
	{
	    memcpy(buf_ + cursize_, src + offset, bytes_per_sync_ - cursize_);

	    NVM_DEBUG("Buffer is at %lu out of %lu. Appending %lu", cursize_, bytes_per_sync_, bytes_per_sync_ - cursize_);

	    left -= bytes_per_sync_ - cursize_;
	    offset += bytes_per_sync_ - cursize_;

	    cursize_ = bytes_per_sync_;

	    if(Flush(false) == false)
	    {
		return Status::IOError("out of ssd space");
	    }
	}
    }

    NVM_DEBUG("flushing");

    if(Flush(false) == false)
    {
	return Status::IOError("out of ssd space");
    }

    NVM_DEBUG("Appending done");

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


NVMRandomRWFile::NVMRandomRWFile(const std::string& fname, nvm_file *_fd, nvm_directory *_dir) :
	    filename_(fname)
{
    fd_ = _fd;
    dir = _dir;

    channel = 0;

    nvm_api = dir->GetNVMApi();

    NVM_DEBUG("created %s", fname.c_str());
}

NVMRandomRWFile::~NVMRandomRWFile()
{
    NVMRandomRWFile::Close();
}

struct nvm_page *NVMRandomRWFile::SeekPage(const unsigned long offset, unsigned long *page_pointer, unsigned long *page_idx) const
{
    struct nvm_page *crt_page = fd_->GetNVMPage(0);

    *page_pointer = offset;

    *page_idx = (unsigned long)(*page_pointer / crt_page->sizes[channel]);

    *page_pointer %= crt_page->sizes[channel];

    crt_page = fd_->GetNVMPage(*page_idx);

    if(crt_page == nullptr && offset == fd_->GetSize())
    {
	if(fd_->ClaimNewPage(dir->GetNVMApi()) == false)
	{
	    NVM_FATAL("Out of SSD space");
	}
    }

    return crt_page;
}

Status NVMRandomRWFile::Write(uint64_t offset, const Slice& data)
{
    unsigned long page_pointer;
    unsigned long page_idx;

    size_t i = 0;

    nvm_page *new_pg = nullptr;

    struct nvm_page *crt_page = nullptr;

    char *crt_data = nullptr;

    const char* src = data.data();

    size_t left = data.size();

    if(offset >= fd_->GetSize())
    {
	NVM_DEBUG("offset is out of bounds");

	return Status::IOError("offset is out of bounds");
    }

    while(left > 0)
    {
	if(crt_page == nullptr)
	{
	    crt_page = SeekPage(offset, &page_pointer, &page_idx);

	    SAFE_ALLOC(crt_data, char[crt_page->sizes[channel]]);
	}
	else
	{
	    crt_page = fd_->GetNVMPage(++page_idx);

	    if(crt_page == nullptr)
	    {
		if(fd_->ClaimNewPage(dir->GetNVMApi()) == false)
		{
		    NVM_FATAL("Out of SSD space");
		}

		crt_page = fd_->GetNVMPage(page_idx);

		if(crt_page == nullptr)
		{
		    NVM_FATAL("Out of SSD space");
		}
	    }

	    page_pointer = 0;
	}

	NVM_ASSERT(crt_page != nullptr, "crt page is null");

	NVM_DEBUG("page pointer is %lu, page size is %u", page_pointer, crt_page->sizes[channel]);

	fd_->ReadPage(crt_page, channel, nvm_api, crt_data);

	for(i = 0; i < crt_page->sizes[channel] - page_pointer && i < left; ++i)
	{
	    crt_data[page_pointer + i] = src[i];
	}

	left -= i;

	new_pg = nvm_api->RequestPage();

	if(new_pg == nullptr)
	{
	    delete[] crt_data;

	    return Status::IOError("request new page returned");
	}

	struct nvm_page *wrote_pg = new_pg;

	if(fd_->WritePage(wrote_pg, channel, nvm_api, crt_data, wrote_pg->sizes[channel]) != wrote_pg->sizes[channel])
	{
	    NVM_FATAL("write error");
	}

	if(wrote_pg != new_pg)
	{
	    new_pg = wrote_pg;
	}

	nvm_api->ReclaimPage(crt_page);
	fd_->SetPage(page_idx, new_pg);
    }

    if(crt_data)
    {
	delete[] crt_data;
    }

    return Status::OK();
}

Status NVMRandomRWFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
    unsigned long page_pointer;

    if(offset >= fd_->GetSize())
    {
	NVM_DEBUG("offset is out of bounds");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

    if(offset + n > fd_->GetSize())
    {
	n = fd_->GetSize() - offset;
    }

    if(n <= 0)
    {
	NVM_DEBUG("n is <= 0");

	*result = Slice(scratch, 0);
	return Status::OK();
    }

    size_t len = n;
    size_t l;
    size_t scratch_offset = 0;
    size_t size_to_copy;

    char *data;

    unsigned long page_idx;

    struct nvm_page *crt_page = SeekPage(offset, &page_pointer, &page_idx);

    if(crt_page == nullptr)
    {
	*result = Slice(scratch, 0);
	return Status::OK();
    }

    SAFE_ALLOC(data, char[crt_page->sizes[channel]]);

    while(len > 0)
    {
	if(crt_page == nullptr)
	{
	    n -= len;
	    break;
	}

	l = fd_->ReadPage(crt_page, channel, nvm_api, data);

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

	crt_page = fd_->GetNVMPage(++page_idx);
	page_pointer = 0;
    }

    delete[] data;

    NVM_DEBUG("read %lu bytes", n);

    *result = Slice(scratch, n);

    return Status::OK();
}

Status NVMRandomRWFile::Close()
{
    dir->nvm_fclose(fd_, "w");

    return Status::OK();
}

Status NVMRandomRWFile::Sync()
{
    return Status::OK();
}

Status NVMRandomRWFile::Fsync()
{
    return Status::OK();
}

#ifdef ROCKSDB_FALLOCATE_PRESENT

Status NVMRandomRWFile::Allocate(off_t offset, off_t len)
{
    return Status::OK();
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
