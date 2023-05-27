''' Compatibility with fsspec filesystems

Usage:
from fsspec.implementations.local import LocalFileSystem
ufs = FSSpec(LocalFileSystem())
'''
import time
import typing as t
import itertools
import pathlib
from ufs.spec import UFS, FileStat

class FSSpec(UFS):
  def __init__(self, fs) -> None:
    self._fs = fs
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}
  def ls(self, path: str) -> list[str]:
    return [
      str(pathlib.PurePosixPath(p).relative_to(path))
      for p in self._fs.ls(path, detail=False)
    ]
  def info(self, path: str) -> FileStat:
    info = self._fs.info(path)
    return {
      'type': info['type'],
      'size': info['size'],
      'atime': info.get('atime', time.time()),
      'ctime': info.get('ctime', info.get('created', time.time())),
      'mtime': info.get('mtime', time.time()),
    }
  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    fd = next(self._cfd)
    self._fds[fd] = self._fs.open(path, mode)
    return fd
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._fds[fd].seek(pos, whence)
  def read(self, fd: int, amnt: int) -> bytes:
    return self._fds[fd].read(amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._fds[fd].write(data)
  def truncate(self, fd: int, length: int):
    self._fds[fd].trunate(length)
  def close(self, fd: int):
    return self._fds.pop(fd).close()
  def unlink(self, path: str):
    return self._fs.rm_file(path)

  # optional
  def mkdir(self, path: str):
    return self._fs.mkdir(path)
  def rmdir(self, path: str):
    return self._fs.rmdir(path)
  def flush(self, fd: int):
    self._fds[fd].flush()

  def copy(self, src: str, dst: str):
    self._fs.copy(src, dst)

  def rename(self, src: str, dst: str):
    self._fs.rename(src, dst)

  def __repr__(self) -> str:
    return f"FSSpec({self._fs.__class__.__name__}())"
