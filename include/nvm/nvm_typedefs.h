#ifndef _NVM_TYPEDEFS_H_
#define _NVM_TYPEDEFS_H_

typedef unsigned long long sector_t;

struct nba_channel {
  unsigned long int lun_idx;
  unsigned short int chnl_idx;

  unsigned int gran_write;
  unsigned int gran_read;
  unsigned int gran_erase;
};

struct nba_block {
  unsigned long lun;
  sector_t phys_addr;

  unsigned long id;

  void *internals;
};


//TODO: This is the structure I want to use
// struct vblock {
//   size_t id;
//   size_t owner_id;
//   size_t bppa;
//   size_t nppas;
//   size_t ppa_bitmap;
//   void *priv;
//   unsigned int vlun_id;
//   uint8_t flags;
// };
// Florin: size + number of pages maybe?

struct vblock {
  unsigned long vlun_id;
  sector_t bppa;
  unsigned long id;
  void *internals;
};

// Metadata that RocksDB keeps for each virtual block

#define VBLOCK_OPEN         0x00
#define VBLOCK_CLOSED       0x01

// This metadata written in the first sizeof(struct vblock_recov_meta) bytes
// of the block before it is given to upper layers in RocksDB. This metadata is
// used to reconstruct the nvm_file in case of a crash.
// TODO: Find a better approximation for filename size
struct vblock_recov_meta {
  char filename[100];           // Filename to which the vblock belongs to.
                                // Filenames are used by RocksDB as GUIs
  size_t pos;                   // Position of the block in the nvm_file;
};

// This metadata is appended to each vblock when it is properly closed.
struct vblock_close_meta {
  size_t written_bytes;         // Number of valid bytes written in block
  size_t ppa_bitmap;            // Updated bitmap of valid pages;
  uint8_t flags;                // RDB_VBLOCK_* flags
};

// This metadata is used to keep track of where to write in a partially written
// nvm_block. This metadata allows also to keep track of the write pointer when
// a block is forced to flush and a page is partially written.
struct vblock_partial_meta {
  size_t ppa_offset;            // Last ppa containing valid data
  size_t page_offset;           // Page offset inside of ppa_offset
};

struct nvm_channel {
  unsigned int gran_write;
  unsigned int gran_read;
  unsigned int gran_erase;
};

struct nvm_page {
  unsigned long lun_id;
  unsigned long block_id;
  unsigned long id;

#ifndef NVM_ALLOCATE_BLOCKS
  bool allocated;
#endif

  unsigned int sizes_no;
  unsigned int *sizes;
};

struct nvm_block {
  struct nba_block *block;
  struct nvm_page *pages;

#ifdef NVM_ALLOCATE_BLOCKS
  bool allocated;
#else
  bool has_stale_pages;
  bool has_pages_allocated;
#endif
};

struct nvm_lun {
  int id;

 unsigned long nr_blocks;
  struct nvm_block *blocks;
  unsigned long nr_pages_per_blk;

  unsigned long nchannels;
  struct nvm_channel *channels;
};

class list_node {
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
struct next_block_to_allocate {
  unsigned long lun_id;
  unsigned long block_id;
};
#else
struct next_page_to_allocate {
  unsigned long lun_id;
  unsigned long block_id;
  unsigned long page_id;
};
#endif

class nvm {
  public:
    unsigned long nr_luns;
    unsigned long max_alloc_try_count;

    unsigned sector_size;
    unsigned max_pages_in_io;

    struct nvm_lun *luns;

    int fd;

    std::string location;

    nvm();
    ~nvm();

    void GarbageCollection();

    bool GetBlock(unsigned int vlun_id, struct vblock *vblock);
    bool PutBlock(struct vblock *vblock);
    void EraseBlock(struct vblock *vblock);
    size_t GetNPagesBlock(unsigned int vlun_id);

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
    int close_nvm_device(const char *file);
    int ioctl_initialize();
    int nvm_get_features();

    void SwapBlocksOnNVM(struct nvm_block *src, struct nvm_block *dest);
    void SwapBlocksInMem(struct nvm_block *src, struct nvm_block *dest);
};

#endif //_NVM_TYPEDEFS_H_
