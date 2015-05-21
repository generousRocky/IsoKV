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

    if(pthread_mutex_init(&rw_mtx, nullptr))
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

//opens existing file or creates a new one
nvm_file *NVMFileManager::nvm_fopen(const char *filename, const char *mode)
{
    nvm_file *to_create;

    list_node *file_node = look_up(filename);

    if(file_node)
    {
	nvm_file *process = (nvm_file *)file_node->GetData();

	NVM_DEBUG("found file %s at %p", filename, process);

	return process;
    }

    if(mode[0] != 'a' && mode[0] != 'w')
    {
	return nullptr;
    }

    if(nvm_api == nullptr)
    {
	NVM_FATAL("api is null");
    }

    ALLOC_CLASS(to_create, nvm_file(filename));
    ALLOC_CLASS(file_node, list_node(to_create));

    pthread_mutex_lock(&list_update_mtx);

    file_node->SetNext(head);
    head = file_node;

    pthread_mutex_unlock(&list_update_mtx);

    NVM_DEBUG("created file %s at %p", filename, to_create);

    return to_create;
}

void NVMFileManager::nvm_fclose(nvm_file *file)
{
    NVM_DEBUG("closing file %s at %p", file->GetName(), file);
}

size_t NVMFileManager::nvm_fread(void *data, const unsigned long offset, const size_t len, nvm_file *fd)
{
    pthread_mutex_lock(&rw_mtx);

    if(fd->GetNVMPagesList() == NULL)
    {
	pthread_mutex_unlock(&rw_mtx);
	return 0;
    }

    /*unsigned long offset_ = offset;

    list_node *crt_node = fd->GetNVMPagesList();
    nvm_page *process_page = (nvm_page *)crt_node->GetData();

    while(crt_node != NULL && offset_ >= process_page->size)
    {
	offset_ -= process_page->size;

	crt_node = crt_node->GetNext();

	if(crt_node)
	{
	    process_page = (nvm_page *)crt_node->GetData();
	}
    }

    if(crt_node == NULL)
    {
	return 0;
    }

    unsigned char *page_data;

    //select channel to read from
    struct nvm_channel  *chnl = &nvm_api->luns[process_page->lun_id]->channels[0];

    SAFE_ALLOC(page_data, unsigned char[chnl->grab_read]);

    unsigned int file_offset =

    if(lseek(nvm_api->fd, , ))

    delete[] page_data;*/

    pthread_mutex_unlock(&rw_mtx);
    return 0;
}

}

#endif
