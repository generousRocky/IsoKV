#ifdef ROCKSDB_PLATFORM_NVM

#include "nvm/nvm.h"

namespace rocksdb
{

nvm_directory::nvm_directory(const char *_name, const int n, nvm *_nvm_api)
{
    NVM_DEBUG("constructing node %s", _name);

    SAFE_ALLOC(name, char[n + 1]);
    strncpy(name, _name, n);
    name[n] = '\0';

    nvm_api = _nvm_api;

    head = nullptr;

    pthread_mutexattr_init(&list_update_mtx_attr);
    pthread_mutexattr_settype(&list_update_mtx_attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&list_update_mtx, &list_update_mtx_attr);
}

nvm_directory::~nvm_directory()
{
    NVM_DEBUG("free directory %s", name);

    delete[] name;

    pthread_mutex_destroy(&list_update_mtx);
    pthread_mutexattr_destroy(&list_update_mtx_attr);

    //delete all files in the directory
    list_node *temp = head;

    while(temp != nullptr)
    {
	list_node *temp1 = temp->GetNext();

	delete (nvm_entry *)temp->GetData();
	delete temp;

	temp = temp1;
    }
}

void nvm_directory::EnumerateNames(std::vector<std::string>* result)
{
    result->push_back(name);
}

bool nvm_directory::HasName(const char *_name, const int n)
{
    int i;

    for(i = 0; i < n; ++i)
    {
	if(name[i] != _name[i])
	{
	    return false;
	}
    }
    return (name[i] == '\0');
}

//checks if the node exists
list_node *nvm_directory::node_look_up(list_node *prev, const char *look_up_name)
{
    list_node *temp;

    NVM_DEBUG("looking for %s in %s", look_up_name, name);

    int i = 0;

    while(look_up_name[i] != '/' && look_up_name[i] != '\0')
    {
	++i;
    }

    NVM_DEBUG("i is %d", i);

    if(i == 0)
    {
	NVM_DEBUG("returning prev");

	return prev;
    }

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    NVM_DEBUG("mutex released %p", temp);

    while(temp != nullptr)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	switch(entry->GetType())
	{
	    case FileEntry:
	    {
		nvm_file *process_file = (nvm_file *)entry->GetData();

		if(process_file->HasName(look_up_name, i) == false)
		{
		    break;
		}

		if(look_up_name[i] == '\0')
		{
		    NVM_DEBUG("found file at %p", temp);

		    return temp;
		}
		else
		{
		    return nullptr;
		}
	    }
	    break;

	    case DirectoryEntry:
	    {
		nvm_directory *process_directory = (nvm_directory *)entry->GetData();

		if(process_directory->HasName(look_up_name, i) == false)
		{
		    break;
		}

		if(look_up_name[i] == '\0')
		{
		    NVM_DEBUG("found directory at %p", temp);

		    return temp;
		}
		else
		{
		    return process_directory->node_look_up(temp, look_up_name + i + 1);
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

    NVM_DEBUG("returning null");

    return nullptr;
}

//checks if the node with a specific type exists
list_node *nvm_directory::node_look_up(const char *look_up_name, const nvm_entry_type type)
{
    list_node *temp = node_look_up(nullptr, look_up_name);

    if(temp == nullptr)
    {
	return nullptr;
    }

    nvm_entry *entry = (nvm_entry *)temp->GetData();

    if(entry->GetType() != type)
    {
	return nullptr;
    }

    return temp;
}

//checks if the directory exists
nvm_directory *nvm_directory::directory_look_up(const char *directory_name)
{
    list_node *temp = node_look_up(directory_name, DirectoryEntry);

    if(temp == nullptr)
    {
	return nullptr;
    }

    return (nvm_directory *)(((nvm_entry *)temp->GetData())->GetData());
}

//checks if the file exists
nvm_file *nvm_directory::file_look_up(const char *filename)
{
    list_node *temp = node_look_up(filename, FileEntry);

    if(temp == nullptr)
    {
	return nullptr;
    }

    return (nvm_file *)(((nvm_entry *)temp->GetData())->GetData());
}

void *nvm_directory::create_node(const char *look_up_name, const nvm_entry_type type)
{
    nvm_file *fd;
    nvm_directory *dd;

    nvm_entry *entry;
    list_node *file_node;
    list_node *iterator;

    void *ret;

    int i = 0;

    while(look_up_name[i] != '/' && look_up_name[i] != '\0')
    {
	++i;
    }

    pthread_mutex_lock(&list_update_mtx);

    iterator = head;

    while(iterator)
    {
	entry = (nvm_entry *)iterator->GetData();

	switch(entry->GetType())
	{
	    case FileEntry:
	    {
		fd = (nvm_file *)entry->GetData();

		if(fd->HasName(look_up_name, i) == false)
		{
		    break;
		}

		if(look_up_name[i] != '\0')
		{
		    ret = nullptr;

		    goto out;
		}

		if(type == FileEntry)
		{
		    ret = fd;
		}
		else
		{
		    ret = nullptr;
		}
	    }
	    goto out;

	    case DirectoryEntry:
	    {
		dd = (nvm_directory *)entry->GetData();

		if(dd->HasName(look_up_name, i) == false)
		{
		    break;
		}

		if(look_up_name[i] != '\0')
		{
		    ret = dd->create_node(look_up_name + i + 1, type);

		    goto out;
		}

		if(type == DirectoryEntry)
		{
		    ret = dd;
		}
		else
		{
		    ret = nullptr;
		}
	    }
	    goto out;

	    default:
	    {
		NVM_FATAL("Unknown entry type!!");
	    }
	    break;
	}

	iterator = iterator->GetNext();
    }

    if(look_up_name[i] == '\0')
    {
	switch(type)
	{
	    case FileEntry:
	    {
		ALLOC_CLASS(fd, nvm_file(look_up_name, nvm_api->fd));
		ALLOC_CLASS(entry, nvm_entry(FileEntry, fd));

		ret = fd;
	    }
	    break;

	    case DirectoryEntry:
	    {
		ALLOC_CLASS(dd, nvm_directory(look_up_name, strlen(look_up_name), nvm_api));
		ALLOC_CLASS(entry, nvm_entry(DirectoryEntry, dd));

		ret = dd;
	    }
	    break;

	    default:
	    {
		NVM_FATAL("unknown node type!!");
	    }
	    break;
	}

	ALLOC_CLASS(file_node, list_node(entry));
    }
    else
    {
	ALLOC_CLASS(dd, nvm_directory(look_up_name, i, nvm_api));
	ALLOC_CLASS(entry, nvm_entry(DirectoryEntry, dd));
	ALLOC_CLASS(file_node, list_node(entry));

	ret = dd->create_node(look_up_name + i + 1, type);
    }

    file_node->SetNext(head);

    if(head)
    {
	head->SetPrev(file_node);
    }

    head = file_node;

out:

    pthread_mutex_unlock(&list_update_mtx);

    NVM_DEBUG("created file %s at %p", look_up_name, ret);

    return ret;
}

nvm_file *nvm_directory::create_file(const char *filename)
{
    return (nvm_file *)create_node(filename, FileEntry);
}

int nvm_directory::CreateDirectory(const char *directory_name)
{
    if(create_node(directory_name, DirectoryEntry) != nullptr)
    {
	return 0;
    }

    return -1;
}

nvm_file *nvm_directory::open_file_if_exists(const char *filename)
{
    return file_look_up(filename);
}

//opens existing file or creates a new one
nvm_file *nvm_directory::nvm_fopen(const char *filename, const char *mode)
{
    if(mode[0] != 'a' && mode[0] != 'w')
    {
	return open_file_if_exists(filename);
    }

    return create_file(filename);
}

int nvm_directory::GetFileSize(const char *filename, unsigned long *size)
{
    nvm_file *fd = file_look_up(filename);

    if(fd)
    {
	*size = fd->GetSize();
	return 0;
    }

    *size = 0;
    return 1;
}

int nvm_directory::GetFileModificationTime(const char *filename, time_t *mtime)
{
    nvm_file *fd = file_look_up(filename);

    if(fd)
    {
	*mtime = fd->GetLastModified();
	return 0;
    }

    *mtime = 0;
    return 1;
}

bool nvm_directory::FileExists(const char *_name)
{
    return (node_look_up(nullptr, _name) != nullptr);
}

void nvm_directory::nvm_fclose(nvm_file *file)
{
    NVM_DEBUG("closing file at %p", file);
}

int nvm_directory::LinkFile(const char *src, const char *target)
{
    pthread_mutex_lock(&list_update_mtx);

    nvm_file *fd = file_look_up(target);

    if(fd)
    {
	pthread_mutex_unlock(&list_update_mtx);
	return -1;
    }

    fd = file_look_up(src);

    if(fd)
    {
	fd->AddName(target);

	pthread_mutex_unlock(&list_update_mtx);
	return 0;
    }

    pthread_mutex_unlock(&list_update_mtx);
    return -1;
}

int nvm_directory::RenameFile(const char *crt_filename, const char *new_filename)
{
    pthread_mutex_lock(&list_update_mtx);

    nvm_file *fd = file_look_up(new_filename);

    if(fd)
    {
	pthread_mutex_unlock(&list_update_mtx);
	return 1;
    }

    fd = file_look_up(crt_filename);

    if(fd)
    {
	fd->ChangeName(crt_filename, new_filename);

	pthread_mutex_unlock(&list_update_mtx);
	return 0;
    }

    pthread_mutex_unlock(&list_update_mtx);
    return 1;
}

int nvm_directory::DeleteFile(const char *filename)
{
    pthread_mutex_lock(&list_update_mtx);

    list_node *file_node = node_look_up(filename, FileEntry);

    if(file_node)
    {
	list_node *prev = file_node->GetPrev();
	list_node *next = file_node->GetNext();

	nvm_entry *entry = (nvm_entry *)file_node->GetData();

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

nvm_directory *nvm_directory::OpenDirectory(const char *_name)
{
    return directory_look_up(_name);
}

void nvm_directory::Delete()
{
    pthread_mutex_lock(&list_update_mtx);



    pthread_mutex_unlock(&list_update_mtx);
}

void nvm_directory::GetChildren(std::vector<std::string>* result)
{
    list_node *temp;

    pthread_mutex_lock(&list_update_mtx);

    temp = head;

    pthread_mutex_unlock(&list_update_mtx);

    while(temp)
    {
	nvm_entry *entry = (nvm_entry *)temp->GetData();

	switch(entry->GetType())
	{
	    case DirectoryEntry:
	    {
		nvm_directory *dir = (nvm_directory *)entry->GetData();

		dir->EnumerateNames(result);
	    }
	    break;

	    case FileEntry:
	    {
		nvm_file *fd = (nvm_file *)entry->GetData();

		fd->EnumerateNames(result);
	    }
	    break;

	    default:
	    {
		NVM_FATAL("unknown nvm entry");
	    }
	    break;
	}

	temp = temp->GetNext();
    }
}

int nvm_directory::GetChildren(const char *_name, std::vector<std::string>* result)
{
    nvm_directory *dir = directory_look_up(_name);

    if(dir == nullptr)
    {
	return -1;
    }

    dir->GetChildren(result);

    return 0;
}

int nvm_directory::DeleteDirectory(const char *_name)
{


    return 0;
}

}

#endif
