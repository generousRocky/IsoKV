#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

NVMFileManager::NVMFileManager(nvm *_nvm_api)
{
    nvm_api = _nvm_api;

    head = NULL;

    if(pthread_mutex_init(&list_update_mtx, nullptr))
    {
	NVM_FATAL("");
    }
}

NVMFileManager::~NVMFileManager()
{
    pthread_mutex_destroy(&list_update_mtx);

    //delete all files in the list
    if(head)
    {
	list_node *temp = head;

	while(temp != NULL)
	{
	    list_node *temp1 = temp->GetNext();

	    delete (nvm_file *)temp->GetData();
	    delete temp;

	    temp = temp1;
	}
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
	if(strcmp(process->GetName(), filename) == 0)
	{
	    return temp;
	}

	temp = temp->GetNext();
    }

    return nullptr;
}

nvm_file *NVMFileManager::create_file(const char *filename)
{
    nvm_file *fd;
    list_node *file_node;

    ALLOC_CLASS(fd, nvm_file(filename, nvm_api->fd));
    ALLOC_CLASS(file_node, list_node(fd));

    pthread_mutex_lock(&list_update_mtx);

    file_node->SetNext(head);
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
    nvm_file *fd = open_file_if_exists(filename);

    if(fd != nullptr)
    {
	return fd;
    }

    if(mode[0] != 'a' && mode[0] != 'w')
    {
	return nullptr;
    }

    NVM_ASSERT(nvm_api != nullptr, "api is null");

    return create_file(filename);
}

void NVMFileManager::nvm_fclose(nvm_file *file)
{
    NVM_DEBUG("closing file %s at %p", file->GetName(), file);
}

}

#endif
