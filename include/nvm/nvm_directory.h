#ifndef _NVM_DIRECTORY_H_
#define _NVM_DIRECTORY_H_

namespace rocksdb
{

class nvm_directory
{
    private:
	char *name;

    public:
	nvm_directory(const char*_name);
	~nvm_directory();

	bool HasName(const char *_name);

	void Delete();
};

class NVMDirectory : public Directory
{
    private:
	nvm_directory *fd_;

    public:
	explicit NVMDirectory(nvm_directory *fd);
	~NVMDirectory();

	virtual Status Fsync() override;
};

}

#endif
