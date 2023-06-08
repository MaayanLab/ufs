''' A filesystem wrapper which uses one UFS as a writecache for another UFS.

Usage:
Writecache(SomeUFS(), Memory())

In this case, we will temporarily write to memory until the write is complete then we will flush
to the underlying ufs. This is useful when your UFS doesn't support seeks.
'''

import typing as t
import itertools
from ufs.spec import UFS, FileStat
from ufs.pathlib import pathparent, pathname

class Writecache(UFS):
  def __init__(self, ufs: UFS, cache: UFS):
    super().__init__()
    self._ufs = ufs
    self._cache = cache
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}

  @staticmethod
  def from_dict(*, ufs, cache):
    return Writecache(
      ufs=UFS.from_dict(**ufs),
      cache=UFS.from_dict(**cache),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
      cache=self._cache.to_dict(),
    )

  def ls(self, path: str) -> list[str]:
    return list({
      *self._ufs.ls(path),
      *[
        pathname(p)
        for ufs, _, p in self._fds.values()
        if ufs is self._cache and pathparent(p) == path
      ],
    })

  def info(self, path: str) -> FileStat:
    for fd, (ufs, _, p) in self._fds.items():
      if ufs is self._cache and p == path:
        return self._cache.info(f"/{fd}")
    return self._ufs.info(path)

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    if 'r' in mode and '+' not in mode:
      fr = self._ufs.open(path, mode)
      fd = next(self._cfd)
      self._fds[fd] = (self._ufs, fr, path)
      return fd
    elif 'r' in mode or 'a' in mode:
      fr = self._ufs.open(path, 'rb')
      fd = next(self._cfd)
      fw = self._cache.open(f"/{fd}", mode='wb+')
      while buf := self._ufs.read(fr, 5*1024):
        self._cache.write(fw, buf)
      self._ufs.close(fr)
      if 'r' in mode: self._cache.seek(fw, 0)
      self._fds[fd] = (self._cache, fw, path)
      return fd
    else:
      fd = next(self._cfd)
      fw = self._cache.open(f"/{fd}", mode='wb+')
      self._fds[fd] = (self._cache, fw, path)
      return fd
  
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    ufs, fh, _ = self._fds[fd]
    return ufs.seek(fh, pos, whence)
  def read(self, fd: int, amnt: int) -> bytes:
    ufs, fh, _ = self._fds[fd]
    return ufs.read(fh, amnt)
  def write(self, fd: int, data: bytes) -> int:
    ufs, fh, _ = self._fds[fd]
    return ufs.write(fh, data)
  def truncate(self, fd: int, length: int):
    ufs, fh, _ = self._fds[fd]
    return ufs.truncate(fh, length)
  def close(self, fd: int):
    ufs, fh, path = self._fds.pop(fd)
    if ufs is self._cache:
      fw = self._ufs.open(path, 'wb')
      self._cache.seek(fh, 0)
      while buf := self._cache.read(fh, 5*1024):
        self._ufs.write(fw, buf)
      self._cache.close(fh)
      self._cache.unlink(f"/{fd}")
      return self._ufs.close(fw)
    else:
      return self._ufs.close(fh)

  def unlink(self, path: str):
    self._ufs.unlink(path)

  def mkdir(self, path: str):
    return self._ufs.mkdir(path)
  def rmdir(self, path: str):
    return self._ufs.rmdir(path)
  def flush(self, fd: int):
    ufs, fh, _ = self._fds[fd]
    return ufs.flush(fh)

  def copy(self, src: str, dst: str):
    return self._ufs.copy(src, dst)

  def rename(self, src: str, dst: str):
    return self._ufs.rename(src, dst)

  def start(self):
    self._ufs.start()
    self._cache.start()

  def stop(self):
    self._cache.stop()
    self._ufs.stop()
