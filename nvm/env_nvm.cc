#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <linux/limits.h>
#include "rocksdb/utilities/env_registry.h"
#include "env_nvm.h"

namespace rocksdb {

static EnvRegistrar nvm_reg(
  "nvm://",
  [](const std::string& uri, std::unique_ptr<Env>* env_guard) {

    env_guard->reset(new EnvNVM(uri));

    return env_guard->get();
  }
);

EnvNVM::EnvNVM(
  const std::string& uri
) : Env(), posix_(Env::Default()), uri_(uri), fs_() {
  NVM_TRACE(this, "");
}

EnvNVM::~EnvNVM(void) {
  NVM_TRACE(this, "");

  MutexLock lock(&fs_mutex_);

  for (auto dir : fs_) {
    std::cout << dir.first << std::endl;
    for (auto file : dir.second) {
      std::cout << file->GetFname() << "," << file->GetFileSize() << std::endl;
    }
  }
}

Status EnvNVM::NewSequentialFile(
  const std::string& fpath,
  unique_ptr<SequentialFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);
  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->NewSequentialFile(fpath, result, options);
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::NotFound();
  }

  result->reset(new NVMSequentialFile(file, options));

  return Status::OK();
}

Status EnvNVM::NewRandomAccessFile(
  const std::string& fpath,
  unique_ptr<RandomAccessFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);
  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->NewRandomAccessFile(fpath, result, options);
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::NotFound();
  }
  result->reset(new NVMRandomAccessFile(file, options));

  return Status::OK();
}

Status EnvNVM::ReuseWritableFile(
  const std::string& fpath,
  const std::string& fpath_old,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << "), fpath_old(" << fpath_old << ")");

  FPathInfo info(fpath);
  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->ReuseWritableFile(fpath, fpath_old, result, options);
  }

  return Status::IOError("ReuseWritableFile --> Not implemented.");
}

Status EnvNVM::NewWritableFile(
  const std::string& fpath,
  unique_ptr<WritableFile>* result,
  const EnvOptions& options
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);

  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->NewWritableFile(fpath, result, options);
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (file) {
    DeleteFileUnguarded(fpath);
  }

  file = new NVMFile(this, info);
  fs_[info.dpath()].push_back(file);

  result->reset(new NVMWritableFile(file, options));

  return Status::OK();
}

//
// Deletes a file without taking the fs_mutex_
//
Status EnvNVM::DeleteFileUnguarded(
  const std::string& dpath, const std::string& fname
) {
  NVM_TRACE(this, "dpath(" << dpath << "), fname(" << fname << ")");

  auto dit = fs_.find(dpath);
  if (dit == fs_.end()) {
    NVM_TRACE(this, "Dir NOT found");
    return Status::NotFound();
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(fname)) {
      NVM_TRACE(this, "File found -- erasing");

      NVMFile *file = *it;

      dit->second.erase(it);
      file->Unref();

      return Status::OK();
    }
  }

  NVM_TRACE(this, "File NOT found");
  return Status::NotFound();
}

//
// Deletes a file without taking the fs_mutex_
//
Status EnvNVM::DeleteFileUnguarded(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);

  return DeleteFileUnguarded(info.dpath(), info.fname());
}

Status EnvNVM::DeleteFile(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");
  MutexLock lock(&fs_mutex_);

  FPathInfo info(fpath);

  return DeleteFileUnguarded(info.dpath(), info.fname());
}

Status EnvNVM::FileExists(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);

  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->FileExists(fpath);
  }

  MutexLock lock(&fs_mutex_);
  if (FindFileUnguarded(fpath)) {
    return Status::OK();
  }

  return Status::NotFound();
}

Status EnvNVM::GetChildren(
  const std::string& dpath,
  std::vector<std::string>* result
) {
  NVM_TRACE(this, "dpath(" << dpath << ")");

  posix_->GetChildren(dpath, result);   // Merging posix and nvm

  MutexLock lock(&fs_mutex_);
  for (auto file : fs_[dpath]) {
      result->push_back(file->GetFname());
  }

  for (auto fname : *result) {
    NVM_TRACE(this, "fname(" << fname << ")");
  }

  return Status::OK();
}

Status EnvNVM::GetChildrenFileAttributes(
  const std::string& dpath,
  std::vector<FileAttributes>* result
) {
  NVM_TRACE(this, "dpath(" << dpath << ")");

  return Status::IOError("GetChildrenFileAttributes --> Not implemented");
}

NVMFile* EnvNVM::FindFileUnguarded(const std::string& fpath) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);

  auto dit = fs_.find(info.dpath());
  if (dit == fs_.end()) {
    NVM_TRACE(this, "!found");
    return NULL;
  }

  for (auto it = dit->second.begin(); it != dit->second.end(); ++it) {
    if ((*it)->IsNamed(info.fname())) {
      NVM_TRACE(this, "found");
      return *it;
    }
  }

  NVM_TRACE(this, "!found");
  return NULL;
}

Status EnvNVM::GetFileSize(const std::string& fpath, uint64_t* fsize) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);
  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->GetFileSize(fpath, fsize);
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath);
  if (!file) {
    return Status::IOError("File not not found");
  }

  *fsize = file->GetFileSize();

  return Status::OK();
}

Status EnvNVM::GetFileModificationTime(
  const std::string& fpath,
  uint64_t* file_mtime
) {
  NVM_TRACE(this, "fpath(" << fpath << ")");

  FPathInfo info(fpath);
  if (!info.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->GetFileModificationTime(fpath, file_mtime);
  }

  MutexLock lock(&fs_mutex_);

  return Status::IOError("GetFileModificationTime --> Not implemented");
}

Status EnvNVM::RenameFile(
  const std::string& fpath_src,
  const std::string& fpath_tgt
) {
  NVM_TRACE(this, "fpath_src(" << fpath_src << "), fpath_tgt(" << fpath_tgt << ")");

  FPathInfo info_src(fpath_src);
  FPathInfo info_tgt(fpath_tgt);

  if (info_src.nvm_managed() ^ info_src.nvm_managed()) {
    return Status::IOError(
      "Renaming a non-NVM file to a NVM file or the other way around."
    );
  }

  if (!info_src.nvm_managed()) {
    NVM_TRACE(this, "delegating...");
    return posix_->RenameFile(fpath_src, fpath_tgt);
  }

  if (info_src.dpath().compare(info_tgt.dpath())) {
    return Status::IOError("Directory change not supported when renaming");
  }

  MutexLock lock(&fs_mutex_);

  NVMFile *file = FindFileUnguarded(fpath_src);         // Get the source file
  if (!file) {
    return Status::NotFound();
  }

  NVMFile *file_target = FindFileUnguarded(fpath_tgt);  // Delete target
  if (file_target) {
    DeleteFileUnguarded(fpath_tgt);
  }

  file->Rename(info_tgt.fname());                       // The actual renaming

  return Status::OK();
}

}       // namespace rocksdb
