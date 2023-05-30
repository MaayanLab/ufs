''' Compatibility with fsspec filesystems

Usage:
from fsspec.implementations.local import LocalFileSystem
ufs = FSSpec(LocalFileSystem())
'''
import time
import typing as t
import itertools
import functools
from ufs.utils.cache import TTLCache
from ufs.pathlib import pathparent, pathname
from datetime import datetime
from ufs.spec import UFS, FileStat


def fsspec_ls(fs, path: str):
  detail = fs.ls(path, detail=True)
  ret = {
    pathname(item['name']): item
    for item in detail
    if pathparent(item['name']) == path
  }
  return ret

class FSSpec(UFS):
  def __init__(self, fs, ttl=60) -> None:
    self._fs = fs
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}
    self._ls_cache = TTLCache(resolve=functools.partial(fsspec_ls, self._fs), ttl=ttl)

  def _path(self, path: str):
    return self._fs.root_marker + path[1:]

  def _ppath(self, path: str):
    return self._path(pathparent(path))

  def ls(self, path: str) -> list[str]:
    return list(self._ls_cache(self._path(path.rstrip('/'))).keys())

  def info(self, path: str) -> FileStat:
    try:
      if pathparent(path) == pathname(path): info = self._fs.info(self._path(path))
      else: info = self._ls_cache(self._ppath(path))[pathname(path)]
    except KeyError:
      raise FileNotFoundError(path)
    atime = info.get('atime', time.time())
    if isinstance(atime, datetime): atime = atime.timestamp()
    ctime = info.get('ctime', info.get('created', info.get('CreationDate', time.time())))
    if isinstance(ctime, datetime): ctime = ctime.timestamp()
    mtime = info.get('mtime', info.get('modified', info.get('LastModified', time.time())))
    if isinstance(mtime, datetime): mtime = mtime.timestamp()
    return {
      'type': info['type'],
      'size': info['size'],
      'atime': atime,
      'ctime': ctime,
      'mtime': mtime,
    }

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    fd = next(self._cfd)
    self._ls_cache.discard(self._ppath(path))
    self._fds[fd] = (
      path,
      self._fs.open(self._path(path), mode),
    )
    return fd
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._fds[fd][1].seek(pos, whence)
  def read(self, fd: int, amnt: int) -> bytes:
    return self._fds[fd][1].read(amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._fds[fd][1].write(data)
  def truncate(self, fd: int, length: int):
    return self._fds[fd][1].trunate(length)
  def close(self, fd: int):
    path, fh = self._fds.pop(fd)
    self._ls_cache.discard(self._ppath(path))
    return fh.close()
  def unlink(self, path: str):
    self._ls_cache.discard(self._ppath(path))
    self._fs.rm_file(self._path(path))

  # optional
  def mkdir(self, path: str):
    self._ls_cache.discard(self._path(path))
    self._ls_cache.discard(self._ppath(path))
    return self._fs.mkdir(self._path(path))
  def rmdir(self, path: str):
    self._ls_cache.discard(self._path(path))
    self._ls_cache.discard(self._ppath(path))
    return self._fs.rmdir(self._path(path))
  def flush(self, fd: int):
    self._fds[fd][1].flush()

  def copy(self, src: str, dst: str):
    self._ls_cache.discard(self._ppath(dst))
    self._fs.copy(self._path(src), self._path(dst))

  def rename(self, src: str, dst: str):
    self._ls_cache.discard(self._ppath(src))
    self._ls_cache.discard(self._ppath(dst))
    self._fs.rename(self._path(src), self._path(dst))

  def __repr__(self) -> str:
    return f"FSSpec({self._fs.__class__.__name__}())"
