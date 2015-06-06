#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

NVMFileManager::NVMFileManager(nvm *_nvm_api)
{
    nvm_api = _nvm_api;

    head = nullptr;

    pthread_mutexattr_init(&list_update_mtx_attr);
    pthread_mutexattr_settype(&list_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&list_update_mtx, &list_update_mtx_attr);
}

NVMFileManager::~NVMFileManager()
{
    pthread_mutex_destroy(&list_update_mtx);
    pthread_mutexattr_destroy(&list_update_mtx_attr);

    //delete all files in the list
    list_node *temp = head;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	delete (nvm_file *)temp->GetData();
	delete temp;

	temp = temp1;
    }
}

//checks if the file exists
list_node *NVMFileManager::look_up(const char *filename)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp != nullptr)
    {
	nvm_file *process = (nvm_file *)temp->GetData();

	char *name = process->GetName();

	if(strcmp(name, filename) == 0)
	{
	    delete[] name;
	    return temp;
	}

	delete[] name;
	temp = temp->GetNext();
    }

    return nullptr;
}

nvm_file *NVMFileManager::create_file(const char *filename)
{
    nvm_file *fd;
    list_node *file_node;

    pthread_mutex_lock(&list_update_mtx);

    file_node = look_up(filename);

    if(file_node)
    {
	fd = (nvm_file *)file_node->GetData();

	pthread_mutex_unlock(&list_update_mtx);

	NVM_DEBUG("found file %s at %p", filename, fd);

	return fd;
    }

    ALLOC_CLASS(fd, nvm_file(filename, nvm_api->fd));
    ALLOC_CLASS(file_node, list_node(fd));

    file_node->SetNext(head);

    if(head)
    {
	head->SetPrev(file_node);
    }

    head = file_node;

    pthread_mutex_unlock(&list_update_mtx);

    NVM_DEBUG("created file %s at %p", filename, fd);

    return fd;
}

nvm_file *NVMFileManager::open_file_if_exists(const char *filename)
{
    list_node *file_node = look_up(filename);

    if(file_node)
    {
	nvm_file *process = (nvm_file *)file_node->GetData();

	NVM_DEBUG("found file %s at %p", filename, process);

	return process;
    }

    return nullptr;
}

//opens existing file or creates a new one
nvm_file *NVMFileManager::nvm_fopen(const char *filename, const char *mode)
{
    if(mode[0] != 'a' && mode[0] != 'w')
    {
	return open_file_if_exists(filename);
    }

    return create_file(filename);
}

void NVMFileManager::nvm_fclose(nvm_file *file)
{
    NVM_DEBUG("closing file at %p", file);
}

int NVMFileManager::GetFileSize(const char *filename, unsigned long *size)
{
    list_node *file_node = look_up(filename);

    if(file_node)
    {
	nvm_file *process = (nvm_file *)file_node->GetData();

	NVM_DEBUG("found file %s at %p", filename, process);

	*size = process->GetSize();
	return 0;
    }

    *size = 0;
    return 1;
}

int NVMFileManager::GetFileModificationTime(const char *filename, time_t *mtime)
{
    list_node *file_node = look_up(filename);

    if(file_node)
    {
	nvm_file *process = (nvm_file *)file_node->GetData();

	NVM_DEBUG("found file %s at %p", filename, process);

	*mtime = process->GetLastModified();
	return 0;
    }

    *mtime = 0;
    return 1;
}

int NVMFileManager::RenameFile(const char *crt_filename, const char *new_filename)
{
    pthread_mutex_lock(&list_update_mtx);

    list_node *file_node = look_up(new_filename);

    if(file_node)
    {
	NVM_DEBUG("found file %s at %p", new_filename, file_node);

	pthread_mutex_unlock(&list_update_mtx);
	return 1;
    }

    file_node = look_up(crt_filename);

    if(file_node)
    {
	nvm_file *process = (nvm_file *)file_node->GetData();

	NVM_DEBUG("found file %s at %p", crt_filename, process);

	process->SetName(new_filename);

	pthread_mutex_unlock(&list_update_mtx);
	return 0;
    }

    pthread_mutex_unlock(&list_update_mtx);
    return 1;
}

int NVMFileManager::DeleteFile(const char *filename)
{
    pthread_mutex_lock(&list_update_mtx);

    list_node *file_node = look_up(filename);

    if(file_node)
    {
	list_node *prev = file_node->GetPrev();
	list_node *next = file_node->GetNext();

	if(prev)
	{
	    prev->SetNext(next);
	}

	if(next)
	{
	    next->SetPrev(prev);
	}

	if(next == nullptr && prev == nullptr)
	{
	    head = nullptr;
	}

	nvm_file *file = (nvm_file *)file_node->GetData();

	file->Delete(nvm_api);

	delete file;
	delete file_node;
    }

    pthread_mutex_unlock(&list_update_mtx);

    return 0;
}

}

#endif
