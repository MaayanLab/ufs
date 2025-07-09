''' A filesystem wrapper which uses one UFS as a writecache for another UFS.

Usage:
Writecache(SomeUFS(), Memory())

In this case, we will temporarily write to memory until the write is complete then we will flush
to the underlying ufs. This is useful when your UFS doesn't support seeks.
'''

import itertools
from ufs.spec import SyncUFS
from ufs.utils.pathlib import SafePurePosixPath

class Writecache(SyncUFS):
  def __init__(self, ufs: SyncUFS, cache: SyncUFS):
    super().__init__()
    self._ufs = ufs
    self._cache = cache
    self._cfd = iter(itertools.count(start=5))
    self._fds = {}
  
  def scope(self):
    return self._ufs.scope()

  @staticmethod
  def from_dict(*, ufs, cache):
    return Writecache(
      ufs=SyncUFS.from_dict(**ufs),
      cache=SyncUFS.from_dict(**cache),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
      cache=self._cache.to_dict(),
    )

  def ls(self, path):
    return list({
      *self._ufs.ls(path),
      *[
        p.name
        for ufs, _, p in self._fds.values()
        if ufs is self._cache and p.parent == path
      ],
    })

  def info(self, path):
    for fd, (ufs, _, p) in self._fds.items():
      if ufs is self._cache and p == path:
        return self._cache.info(SafePurePosixPath(f"/{fd}"))
    return self._ufs.info(path)

  def open(self, path, mode, *, size_hint = None):
    if 'r' in mode and '+' not in mode:
      fr = self._ufs.open(path, mode)
      fd = next(self._cfd)
      self._fds[fd] = (self._ufs, fr, path)
      return fd
    elif 'r' in mode or 'a' in mode:
      fr = self._ufs.open(path, 'rb')
      fd = next(self._cfd)
      fw = self._cache.open(SafePurePosixPath(f"/{fd}"), mode='wb')
      while True:
        buf = self._ufs.read(fr, self.CHUNK_SIZE)
        if not buf: break
        self._cache.write(fw, buf)
      self._ufs.close(fr)
      if 'r' in mode: self._cache.seek(fw, 0)
      self._fds[fd] = (self._cache, fw, path)
      return fd
    else:
      fd = next(self._cfd)
      fw = self._cache.open(SafePurePosixPath(f"/{fd}"), mode='wb')
      self._fds[fd] = (self._cache, fw, path)
      return fd
  
  def seek(self, fd, pos, whence = 0):
    ufs, fh, _ = self._fds[fd]
    return ufs.seek(fh, pos, whence)
  def read(self, fd, amnt):
    ufs, fh, _ = self._fds[fd]
    return ufs.read(fh, amnt)
  def write(self, fd, data):
    ufs, fh, _ = self._fds[fd]
    return ufs.write(fh, data)
  def truncate(self, fd, length):
    ufs, fh, _ = self._fds[fd]
    return ufs.truncate(fh, length)
  def close(self, fd):
    ufs, fh, path = self._fds.pop(fd)
    if ufs is self._cache:
      info = self._cache.info(SafePurePosixPath(f"/{fd}"))
      fw = self._ufs.open(path, 'wb', size_hint=info['size'])
      self._cache.seek(fh, 0)
      while True:
        buf = self._cache.read(fh, self.CHUNK_SIZE)
        if not buf: break
        self._ufs.write(fw, buf)
      self._cache.close(fh)
      self._cache.unlink(SafePurePosixPath(f"/{fd}"))
      return self._ufs.close(fw)
    else:
      return self._ufs.close(fh)

  def unlink(self, path):
    self._ufs.unlink(path)

  def mkdir(self, path):
    return self._ufs.mkdir(path)
  def rmdir(self, path):
    return self._ufs.rmdir(path)
  def flush(self, fd):
    ufs, fh, _ = self._fds[fd]
    return ufs.flush(fh)

  def copy(self, src, dst):
    return self._ufs.copy(src, dst)

  def rename(self, src, dst):
    return self._ufs.rename(src, dst)

  def start(self):
    self._ufs.start()
    self._cache.start()

  def stop(self):
    self._cache.stop()
    self._ufs.stop()
