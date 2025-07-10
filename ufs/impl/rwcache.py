''' A filesystem wrapper which uses one UFS as a rw cache for another UFS.
Usually to supplement a store that only supports atomic operations (i.e. no seeking).

Usage:
RWCache(SomeUFS(), Memory())

In this case, we will temporarily write to memory until the write is complete then we will flush
to the underlying ufs. This is useful when your UFS doesn't support seeks.

For large files you could use TemporaryDirectory()
'''

import itertools
from ufs.spec import SyncUFS
from ufs.utils.pathlib import SafePurePosixPath

class RWCache(SyncUFS):
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
    return RWCache(
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
    for fd, (_, p) in self._fds.items():
      if p == path:
        return self._cache.info(SafePurePosixPath(f"/{fd}"))
    return self._ufs.info(path)

  def open(self, path, mode, *, size_hint = None):
    fd = next(self._cfd)
    if 'w' not in mode:
      self._cache.put(SafePurePosixPath(f"/{fd}"), self._ufs.cat(path), size_hint=self._ufs.info(path)['size'])
    fh = self._cache.open(SafePurePosixPath(f"/{fd}"), mode)
    self._fds[fd] = (fh, path)
    return fd

  def seek(self, fd, pos, whence = 0):
    fh, _ = self._fds[fd]
    return self._cache.seek(fh, pos, whence)
  def read(self, fd, amnt):
    fh, _ = self._fds[fd]
    return self._cache.read(fh, amnt)
  def write(self, fd, data):
    fh, _ = self._fds[fd]
    return self._cache.write(fh, data)
  def truncate(self, fd, length):
    fh, _ = self._fds[fd]
    return self._cache.truncate(fh, length)
  def close(self, fd):
    fh, path = self._fds.pop(fd)
    info = self._cache.info(SafePurePosixPath(f"/{fd}"))
    self._cache.close(fh)
    self._ufs.put(path, self._cache.cat(SafePurePosixPath(f"/{fd}")), size_hint=info['size'])
    self._cache.unlink(SafePurePosixPath(f"/{fd}"))
  def unlink(self, path):
    self._ufs.unlink(path)
  def flush(self, fd):
    fh, _ = self._fds[fd]
    return self._cache.flush(fh)

  def mkdir(self, path):
    return self._ufs.mkdir(path)
  def rmdir(self, path):
    return self._ufs.rmdir(path)

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
