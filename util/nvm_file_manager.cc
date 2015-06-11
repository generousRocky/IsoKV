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

	delete (nvm_entry *)temp->GetData();
	delete temp;

	temp = temp1;
    }
}

//checks if the node exists
list_node *NVMFileManager::node_look_up(const char *filename)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp != nullptr)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	switch(entry->GetType())
	{
	    case FileEntry:
	    {
		nvm_file *process_file = (nvm_file *)entry->GetData();

		if(process_file->HasName(filename))
		{
		    return temp;
		}
	    }
	    break;

	    case DirectoryEntry:
	    {
		nvm_directory *process_directory = (nvm_directory *)entry->GetData();

		if(process_directory->HasName(filename))
		{
		    return temp;
		}
	    }
	    break;

	    default:
	    {
		NVM_FATAL("Unknown entry type!!");
	    }
	    break;
	}

	temp = temp->GetNext();
    }

    return nullptr;
}


//checks if the node with a specific type exists
list_node *NVMFileManager::node_look_up(const char *filename, const nvm_entry_type type)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp != nullptr)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	if(entry->GetType() != type)
	{
	    temp = temp->GetNext();

	    continue;
	}

	switch(type)
	{
	    case FileEntry:
	    {
		nvm_file *process_file = (nvm_file *)entry->GetData();

		if(process_file->HasName(filename))
		{
		    return temp;
		}
	    }
	    break;

	    case DirectoryEntry:
	    {
		nvm_directory *process_directory = (nvm_directory *)entry->GetData();

		if(process_directory->HasName(filename))
		{
		    return temp;
		}
	    }
	    break;

	    default:
	    {
		NVM_FATAL("Unknown entry type!!");
	    }
	    break;
	}

	temp = temp->GetNext();
    }

    return nullptr;
}

//checks if the directory exists
nvm_directory *NVMFileManager::directory_look_up(const char *directory_name)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp != nullptr)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	if(entry->GetType() != DirectoryEntry)
	{
	    temp = temp->GetNext();

	    continue;
	}

	nvm_directory *process_directory = (nvm_directory *)entry->GetData();

	if(process_directory->HasName(directory_name))
	{
	    return process_directory;
	}

	temp = temp->GetNext();
    }

    return nullptr;
}

//checks if the file exists
nvm_file *NVMFileManager::file_look_up(const char *filename)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp != nullptr)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	if(entry->GetType() != FileEntry)
	{
	    temp = temp->GetNext();

	    continue;
	}

	nvm_file *process_file = (nvm_file *)entry->GetData();

	if(process_file->HasName(filename))
	{
	    return process_file;
	}

	temp = temp->GetNext();
    }

    return nullptr;
}

nvm_file *NVMFileManager::create_file(const char *filename)
{
    nvm_file *fd;
    nvm_entry *entry;
    list_node *file_node;

    pthread_mutex_lock(&list_update_mtx);

    fd = file_look_up(filename);

    if(fd)
    {
	pthread_mutex_unlock(&list_update_mtx);

	NVM_DEBUG("found file %s at %p", filename, fd);

	return fd;
    }

    ALLOC_CLASS(fd, nvm_file(filename, nvm_api->fd));
    ALLOC_CLASS(entry, nvm_entry(FileEntry, fd));
    ALLOC_CLASS(file_node, list_node(entry));

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
    nvm_file *fd = file_look_up(filename);

    return fd;
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
    nvm_file *fd = file_look_up(filename);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", filename, fd);

	*size = fd->GetSize();
	return 0;
    }

    *size = 0;
    return 1;
}

int NVMFileManager::GetFileModificationTime(const char *filename, time_t *mtime)
{
    nvm_file *fd = file_look_up(filename);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", filename, fd);

	*mtime = fd->GetLastModified();
	return 0;
    }

    *mtime = 0;
    return 1;
}

int NVMFileManager::LinkFile(const char *src, const char *target)
{
    pthread_mutex_lock(&list_update_mtx);

    nvm_file *fd = file_look_up(target);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", target, fd);

	pthread_mutex_unlock(&list_update_mtx);
	return -1;
    }

    fd = file_look_up(src);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", src, fd);

	fd->AddName(target);

	pthread_mutex_unlock(&list_update_mtx);
	return 0;
    }

    pthread_mutex_unlock(&list_update_mtx);
    return -1;
}

int NVMFileManager::RenameFile(const char *crt_filename, const char *new_filename)
{
    pthread_mutex_lock(&list_update_mtx);

    nvm_file *fd = file_look_up(new_filename);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", new_filename, fd);

	pthread_mutex_unlock(&list_update_mtx);
	return 1;
    }

    fd = file_look_up(crt_filename);

    if(fd)
    {
	NVM_DEBUG("found file %s at %p", crt_filename, fd);

	fd->ChangeName(crt_filename, new_filename);

	pthread_mutex_unlock(&list_update_mtx);
	return 0;
    }

    pthread_mutex_unlock(&list_update_mtx);
    return 1;
}

int NVMFileManager::DeleteFile(const char *filename)
{
    pthread_mutex_lock(&list_update_mtx);

    list_node *file_node = node_look_up(filename, FileEntry);

    if(file_node)
    {
	list_node *prev = file_node->GetPrev();
	list_node *next = file_node->GetNext();

	nvm_entry *entry = (nvm_entry *)file_node->GetData();

	if(entry->GetType() != FileEntry)
	{
	    NVM_FATAL("Attempt to delete folder using file req");
	}

	nvm_file *file = (nvm_file *)entry->GetData();

	if(file->Delete(filename, nvm_api))
	{
	    //we have no more link files
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
		head = head->GetNext();
	    }

	    if(next == nullptr && prev == nullptr)
	    {
		head = nullptr;
	    }

	    delete entry;
	    delete file_node;
	}
    }

    pthread_mutex_unlock(&list_update_mtx);

    return 0;
}

nvm_directory *NVMFileManager::OpenDirectory(const char *name)
{
    nvm_directory *directory = directory_look_up(name);

    return directory;
}

int NVMFileManager::CreateDirectory(const char *name)
{
    nvm_directory *fd;
    nvm_entry *entry;
    list_node *directory_node;

    pthread_mutex_lock(&list_update_mtx);

    fd = directory_look_up(name);

    if(fd)
    {
	pthread_mutex_unlock(&list_update_mtx);

	NVM_DEBUG("found directory %s at %p", name, fd);

	return -1;
    }

    ALLOC_CLASS(fd, nvm_directory(name));
    ALLOC_CLASS(entry, nvm_entry(DirectoryEntry, fd));
    ALLOC_CLASS(directory_node, list_node(entry));

    directory_node->SetNext(head);

    if(head)
    {
	head->SetPrev(directory_node);
    }

    head = directory_node;

    pthread_mutex_unlock(&list_update_mtx);

    NVM_DEBUG("created directory %s at %p", name, fd);

    return 0;
}

bool NVMFileManager::FileExists(const char *name)
{
    list_node *ret = node_look_up(name);

    return (ret != nullptr);
}

}

#endif
