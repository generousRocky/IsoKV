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

struct nvm_block
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

struct nvm_lun
{
    unsigned long nr_blocks;

    struct nvm_block *blocks;

    unsigned long nr_pages_per_blk;

    unsigned long nchannels;

    struct nvm_channel *channels;
};

class nvm
{
    public:
	unsigned long nr_luns;

	struct nvm_lun *luns;

	int fd;

	nvm();
	~nvm();

    private:
	int open_nvm_device(const char *file);
	int ioctl_initialize();

};

#endif
