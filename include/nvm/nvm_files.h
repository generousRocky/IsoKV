#ifndef _NVM_FILES_
#define _NVM_FILES_

namespace rocksdb
{

class NVMFileLock : public FileLock
{
    public:
	int fd_;
	std::string filename;
};

class nvm_file
{
    private:
	char *name;

	pthread_mutex_t page_update_mtx;

	unsigned long size;

	int fd_;

	struct list_node *first_page;

    public:
	nvm_file(const char *_name, const int fd);
	~nvm_file();

	char *GetName();

	unsigned long GetSize();

	int GetFD();

	size_t ReadPage(const nvm_page *page, const unsigned long channel, struct nvm *nvm_api, void *data);

	struct list_node *GetNVMPagesList();

	size_t nvm_fread(void *data, const unsigned long offset, const size_t len);

	void make_dummy(struct nvm *nvm_api);
};

//TODO: improve running time to log n lookup.
//This is fine for a small number of files
class NVMFileManager
{
    private:
	nvm *nvm_api;

	//list of nvm_files
	list_node *head;

	//looks up if the file is in the list
	list_node *look_up(const char *filename);

	pthread_mutex_t list_update_mtx;

	nvm_file *create_file(const char *filename);
	nvm_file *open_file_if_exists(const char *filename);

    public:
	NVMFileManager(nvm *_nvm_api);
	~NVMFileManager();

	nvm_file *nvm_fopen(const char *filename, const char *mode);
	void nvm_fclose(nvm_file *file);
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

	NVMFileManager *file_manager_;

	void SeekPage(const unsigned long offset);

    public:
	NVMSequentialFile(const std::string& fname, nvm_file *f, NVMFileManager *file_manager, struct nvm *_nvm_api);
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

	struct list_node *first_page;
	struct nvm *nvm_api;

	NVMFileManager *file_manager_;

	struct list_node *SeekPage(const unsigned long offset, unsigned long *page_pointer) const;

    public:
	NVMRandomAccessFile(const std::string& fname, nvm_file *f, NVMFileManager *file_manager, struct nvm *_nvm_api);
	virtual ~NVMRandomAccessFile();

	virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;

#ifdef OS_LINUX
	virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif

	virtual void Hint(AccessPattern pattern) override;

	virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class NVMMmapFile : public WritableFile
{
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

	// Have we done an munmap of unsynced data?
	bool pending_sync_;

#ifdef ROCKSDB_FALLOCATE_PRESENT

	bool fallocate_with_keep_size_;

#endif

	// Roundup x to a multiple of y
	static size_t Roundup(size_t x, size_t y);

	size_t TruncateToPageBoundary(size_t s);

	Status UnmapCurrentRegion();
	Status MapNewRegion();

    public:
	NVMMmapFile(const std::string& fname, int fd, size_t page_size, const EnvOptions& options);
	~NVMMmapFile();

	virtual Status Append(const Slice& data) override;
	virtual Status Close() override;
	virtual Status Flush() override;
	virtual Status Sync() override;

	/**
	 * Flush data as well as metadata to stable storage.
	 */
	virtual Status Fsync() override;

	/**
	 * Get the size of valid data in the file. This will not match the
	 * size that is returned from the filesystem because we use mmap
	 * to extend file by map_size every time.
	 */
	virtual uint64_t GetFileSize() override;

	virtual Status InvalidateCache(size_t offset, size_t length) override;

#ifdef ROCKSDB_FALLOCATE_PRESENT

	virtual Status Allocate(off_t offset, off_t len) override;
#endif
};

// Use nvm write to write data to a file.
class NVMWritableFile : public WritableFile
{
    private:
	const std::string filename_;

	int fd_;

	size_t cursize_;      // current size of cached data in buf_
	size_t capacity_;     // max size of buf_

	unique_ptr<char[]> buf_;           // a buffer to cache writes

	uint64_t filesize_;

	bool pending_sync_;
	bool pending_fsync_;

	uint64_t last_sync_size_;
	uint64_t bytes_per_sync_;

#ifdef ROCKSDB_FALLOCATE_PRESENT

	bool fallocate_with_keep_size_;

#endif
	RateLimiter* rate_limiter_;

	inline size_t RequestToken(size_t bytes);

    public:
	NVMWritableFile(const std::string& fname, int fd, size_t capacity, const EnvOptions& options);
	~NVMWritableFile();

	virtual Status Append(const Slice& data) override;
	virtual Status Close() override;

	// write out the cached data to the OS cache
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

	int fd_;

	bool pending_sync_;
	bool pending_fsync_;

#ifdef ROCKSDB_FALLOCATE_PRESENT

	bool fallocate_with_keep_size_;

#endif

    public:
	NVMRandomRWFile(const std::string& fname, int fd, const EnvOptions& options);
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

class NVMDirectory : public Directory
{
    private:
	int fd_;

    public:
	explicit NVMDirectory(int fd);
	~NVMDirectory();

	virtual Status Fsync() override;
};

}

#endif
