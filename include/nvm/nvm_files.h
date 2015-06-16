#ifndef _NVM_FILES_
#define _NVM_FILES_

namespace rocksdb
{

class nvm_file
{
    private:
	list_node *names;

	pthread_mutexattr_t page_update_mtx_attr;
	pthread_mutex_t page_update_mtx;

	unsigned long size;

	int fd_;

	struct list_node *first_page;
	struct list_node *last_page;

	time_t last_modified;

	pthread_mutex_t meta_mtx;
	pthread_mutex_t file_lock;

	bool opened_for_write;

    public:
	nvm_file(const char *_name, const int fd);
	~nvm_file();

	bool CanOpen(const char *mode);
	bool ClaimNewPage(nvm *nvm_api);
	bool ClearLastPage(nvm *nvm_api);
	bool HasName(const char *name, const int n);
	void ChangeName(const char *crt_name, const char *new_name);
	void EnumerateNames(std::vector<std::string>* result);

	unsigned long GetSize();

	time_t GetLastModified();
	void UpdateFileModificationTime();
	void Close(const char *mode);

	int GetFD();

	size_t ReadPage(const nvm_page *page, const unsigned long channel, struct nvm *nvm_api, void *data);
	size_t WritePage(const nvm_page *page, const unsigned long channel, struct nvm *nvm_api, void *data, const unsigned long data_len);

	struct list_node *GetNVMPagesList();

	size_t nvm_fread(void *data, const unsigned long offset, const size_t len);

	bool Delete(const char *filename, struct nvm *nvm_api);
	void DeleteAllLinks(struct nvm *_nvm_api);

	int LockFile();
	void UnlockFile();

	void AddName(const char *name);
};

class NVMFileLock : public FileLock
{
    public:
	nvm_file *fd_;
	std::string filename;
};

class NVMSequentialFile: public SequentialFile
{
    private:
	std::string filename_;

	nvm_file *file_;

	unsigned long file_pointer;
	unsigned long channel;
	unsigned long page_pointer;

	struct list_node *crt_page;
	struct nvm *nvm_api;

	nvm_directory *dir;

	void SeekPage(const unsigned long offset);

    public:
	NVMSequentialFile(const std::string& fname, nvm_file *f, nvm_directory *_dir);
	virtual ~NVMSequentialFile();

	virtual Status Read(size_t n, Slice* result, char* scratch) override;
	virtual Status Skip(uint64_t n) override;
	virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class NVMRandomAccessFile: public RandomAccessFile
{
    private:
	std::string filename_;

	nvm_file *file_;

	unsigned long channel;

	struct nvm *nvm_api;

	nvm_directory *dir;

	struct list_node *SeekPage(struct list_node *first_page, const unsigned long offset, unsigned long *page_pointer) const;

    public:
	NVMRandomAccessFile(const std::string& fname, nvm_file *f, nvm_directory *_dir);
	virtual ~NVMRandomAccessFile();

	virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;

#ifdef OS_LINUX
	virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif

	virtual void Hint(AccessPattern pattern) override;

	virtual Status InvalidateCache(size_t offset, size_t length) override;
};

// Use nvm write to write data to a file.
class NVMWritableFile : public WritableFile
{
    private:
	const std::string filename_;

	nvm_file *fd_;
	nvm_directory *dir_;

	size_t cursize_;      // current size of cached data in buf_

	char *buf_;           // a buffer to cache writes

	uint64_t bytes_per_sync_;

	unsigned long channel;

	struct list_node *last_page;

	bool Flush(const bool forced);

	void UpdateLastPage();

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

class NVMRandomRWFile : public RandomRWFile
{
    private:
	const std::string filename_;

	unsigned long channel;

	nvm_file *fd_;
	nvm_directory *dir;

	nvm *nvm_api;

	bool Flush(const bool forced);

	struct list_node *SeekPage(struct list_node *first_page, const unsigned long offset, unsigned long *page_pointer) const;

    public:
	NVMRandomRWFile(const std::string& fname, nvm_file *_fd, nvm_directory *_dir);
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

}

#endif
