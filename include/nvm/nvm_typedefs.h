#ifndef _NVM_TYPEDEFS_H_
#define _NVM_TYPEDEFS_H_

typedef unsigned long long sector_t;

struct nba_channel
{
    unsigned long int lun_idx;
    unsigned short int chnl_idx;

    unsigned int gran_write;
    unsigned int gran_read;
    unsigned int gran_erase;
};

struct nba_block
{
    unsigned long lun;

    sector_t phys_addr;

    unsigned long id;

    void *internals;
};

struct nvm_channel
{
    unsigned int gran_write;
    unsigned int gran_read;
    unsigned int gran_erase;
};

struct nvm_page
{
    unsigned long lun_id;
    unsigned long block_id;
    unsigned long id;

#ifndef NVM_ALLOCATE_BLOCKS

    bool allocated;

#endif

    unsigned int sizes_no;
    unsigned int *sizes;
};

struct nvm_block
{
    struct nba_block *block;
    struct nvm_page *pages;

#ifdef NVM_ALLOCATE_BLOCKS

    bool allocated;

#else

    bool has_stale_pages;
    bool has_pages_allocated;

#endif
};

struct nvm_lun
{
    unsigned long nr_blocks;

    struct nvm_block *blocks;

    unsigned long nr_pages_per_blk;

    unsigned long nchannels;

    struct nvm_channel *channels;
};

class list_node
{
    private:
	void *data;

	list_node *prev;
	list_node *next;

    public:
	list_node(void *_data);
	~list_node();

	list_node *GetNext();
	list_node *GetPrev();

	void *GetData();
	void *SetData(void *_data);
	void *SetNext(list_node *_next);
	void *SetPrev(list_node *_prev);
};
#ifdef NVM_ALLOCATE_BLOCKS

struct next_block_to_allocate
{
    unsigned long lun_id;
    unsigned long block_id;
};

#else

struct next_page_to_allocate
{
    unsigned long lun_id;
    unsigned long block_id;
    unsigned long page_id;
};

#endif

class nvm
{
    public:
	unsigned long nr_luns;
	unsigned long max_alloc_try_count;

	struct nvm_lun *luns;

	int fd;

	std::string location;

	nvm();
	~nvm();

	void GarbageCollection();

#ifdef NVM_ALLOCATE_BLOCKS

	void ReclaimBlock(const unsigned long lun_id, const unsigned long block_id);
	bool RequestBlock(std::vector<struct nvm_page *> *block_pages);
	bool RequestBlock(std::vector<struct nvm_page *> *block_pages, const unsigned long lun_id, const unsigned long block_id);

#else

	void ReclaimPage(struct nvm_page *page);
	struct nvm_page *RequestPage();
	struct nvm_page *RequestPage(const unsigned long lun_id, const unsigned long block_id, const unsigned long page_id);

#endif

	const char *GetLocation();

    private:

#ifdef NVM_ALLOCATE_BLOCKS

	next_block_to_allocate next_block;

#else

	next_page_to_allocate next_page;

	nvm_block *gc_block;

	std::vector<struct nvm_page *> allocated_pages;

#endif

	pthread_mutex_t allocate_page_mtx;
	pthread_mutexattr_t allocate_page_mtx_attr;

	int open_nvm_device(const char *file);
	int ioctl_initialize();

	void SwapBlocksOnNVM(struct nvm_block *src, struct nvm_block *dest);
	void SwapBlocksInMem(struct nvm_block *src, struct nvm_block *dest);
};

#endif
