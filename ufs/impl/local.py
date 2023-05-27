''' A physical storage filesystem
'''
import os
import stat
import shutil
import itertools
from ufs.spec import UFS, FileStat

class Local(UFS):
  def __init__(self) -> None:
    super().__init__()
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}
  def ls(self, path: str):
    return os.listdir(path)
  def info(self, path: str) -> FileStat:
    info = os.stat(path)
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
  def open(self, path: str, mode: str) -> int:
    fd = next(self._cfd)
    self._fds[fd] = open(path, mode)
    return fd
  def seek(self, fd: int, pos: int, how: int):
    return self._fds[fd].seek(pos, how)
  def read(self, fd: int, amnt: int = -1) -> bytes:
    return self._fds[fd].read(amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._fds[fd].write(data)
  def truncate(self, fd: int, length: int):
    self._fds[fd].trunate(length)
  def close(self, fd: int):
    return self._fds.pop(fd).close()
  def unlink(self, path: str):
    os.unlink(path)

  def mkdir(self, path: str):
    os.mkdir(path)
  def rmdir(self, path: str):
    os.rmdir(path)
  def flush(self, fd: int):
    self._fds[fd].flush()
  
  def copy(self, src: str, dst: str):
    shutil.copy(src, dst)
  def rename(self, src: str, dst: str):
    os.rename(src, dst)
