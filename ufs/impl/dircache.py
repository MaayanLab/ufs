''' A UFS wrapper which uses TTL caching for ls & info, this is useful when
these operations are expensive such as with remote stores.
'''

import typing as t
from ufs.spec import UFS, FileStat
from ufs.utils.pathlib import pathparent
from ufs.utils.cache import TTLCache

class DirCache(UFS):
  def __init__(self, ufs: UFS, ttl=60):
    super().__init__()
    self._ttl = ttl
    self._ufs = ufs
    self._ls_cache = TTLCache(resolve=self._ufs.ls, ttl=ttl)
    self._info_cache = TTLCache(resolve=self._ufs.info, ttl=ttl)
    self._fds = {}

  @staticmethod
  def from_dict(*, ufs, ttl):
    return DirCache(UFS.from_dict(**ufs), ttl=ttl)

  def to_dict(self):
    return dict(super().to_dict(), ufs=self._ufs.to_dict(), ttl=self._ttl)

  def ls(self, path: str) -> list[str]:
    return self._ls_cache(path)

  def info(self, path: str) -> FileStat:
    return self._info_cache(path)

  def open(self, path: str, mode: t.Literal['rb', 'wb', 'ab', 'rb+', 'ab+']) -> int:
    self._info_cache.discard(path)
    self._ls_cache.discard(pathparent(path))
    fd = self._ufs.open(path, mode)
    self._fds[fd] = path
    return fd
  def seek(self, fd: int, pos: int, whence: t.Literal[0, 1, 2] = 0):
    return self._ufs.seek(fd, pos, whence)
  def read(self, fd: int, amnt: int = -1) -> bytes:
    return self._ufs.read(fd, amnt)
  def write(self, fd: int, data: bytes) -> int:
    return self._ufs.write(fd, data)
  def truncate(self, fd: int, length: int):
    return self._ufs.truncate(fd, length)
  def close(self, fd: int):
    path = self._fds.pop(fd)
    self._info_cache.discard(path)
    self._ls_cache.discard(pathparent(path))
    return self._ufs.close(fd)
  def unlink(self, path: str):
    self._info_cache.discard(path)
    self._ls_cache.discard(pathparent(path))
    return self._ufs.unlink(path)

  # optional
  def mkdir(self, path: str):
    self._info_cache.discard(path)
    self._ls_cache.discard(path)
    self._ls_cache.discard(pathparent(path))
    return self._ufs.mkdir(path)

  def rmdir(self, path: str):
    self._info_cache.discard(path)
    self._ls_cache.discard(path)
    self._ls_cache.discard(pathparent(path))
    return self._ufs.rmdir(path)

  def flush(self, fd: int):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src: str, dst: str):
    self._ufs.copy(src, dst)
    self._info_cache.discard(dst)
    self._ls_cache.discard(pathparent(dst))

  def rename(self, src: str, dst: str):
    self._ufs.rename(src, dst)
    self._info_cache.discard(src)
    self._ls_cache.discard(pathparent(src))
    self._info_cache.discard(dst)
    self._ls_cache.discard(pathparent(dst))

  def start(self):
    self._ufs.start()

  def stop(self):
    self._ufs.stop()
