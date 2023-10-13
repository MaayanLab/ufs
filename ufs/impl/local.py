''' A physical storage filesystem
'''
import os
import stat
import shutil
import itertools
import typing as t
from ufs.spec import UFS

class Local(UFS):
  def __init__(self):
    super().__init__()
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}
  def ls(self, path):
    return os.listdir(path.as_path())
  def info(self, path):
    info = os.stat(path.as_path())
    if info.st_mode & stat.S_IFDIR:
      type = 'directory'
    elif info.st_mode & stat.S_IFREG:
      type = 'file'
    else:
      raise NotImplementedError()
    return {
      'type': type,
      'size': info.st_size,
      'atime': info.st_atime,
      'ctime': info.st_ctime,
      'mtime': info.st_mtime,
    }
  def open(self, path, mode, *, size_hint = None):
    fd = next(self._cfd)
    self._fds[fd] = open(path.as_path(), mode)
    return fd
  def seek(self, fd, pos, whence = 0):
    return self._fds[fd].seek(pos, whence)
  def read(self, fd, amnt = -1):
    return self._fds[fd].read(amnt)
  def write(self, fd, data: bytes):
    return self._fds[fd].write(data)
  def truncate(self, fd, length):
    self._fds[fd].truncate(length)
  def close(self, fd):
    return self._fds.pop(fd).close()
  def unlink(self, path):
    os.unlink(path.as_path())

  def mkdir(self, path):
    os.mkdir(path.as_path())
  def rmdir(self, path):
    os.rmdir(path.as_path())
  def flush(self, fd):
    self._fds[fd].flush()
  
  def copy(self, src, dst):
    shutil.copy(src.as_path(), dst.as_path())
  def rename(self, src, dst):
    os.rename(src.as_path(), dst.as_path())
