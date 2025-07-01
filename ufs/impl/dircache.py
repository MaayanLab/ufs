''' A UFS wrapper which uses TTL caching for ls & info, this is useful when
these operations are expensive such as with remote stores.
'''

from ufs.spec import SyncUFS
from ufs.utils.cache import TTLCache

class DirCache(SyncUFS):
  def __init__(self, ufs: SyncUFS, ttl=60):
    super().__init__()
    self._ttl = ttl
    self._ufs = ufs
    self._ls_cache = TTLCache(resolve=self._ufs.ls, ttl=ttl)
    self._info_cache = TTLCache(resolve=self._ufs.info, ttl=ttl)
    self._fds = {}

  def scope(self):
    return self._ufs.scope()

  @staticmethod
  def from_dict(*, ufs, ttl):
    return DirCache(SyncUFS.from_dict(**ufs), ttl=ttl)

  def to_dict(self):
    return dict(super().to_dict(), ufs=self._ufs.to_dict(), ttl=self._ttl)

  def ls(self, path):
    return self._ls_cache(path)

  def info(self, path):
    return self._info_cache(path)

  def open(self, path, mode, *, size_hint = None):
    self._info_cache.discard(path)
    self._ls_cache.discard(path.parent)
    fd = self._ufs.open(path, mode, size_hint=size_hint)
    self._fds[fd] = path
    return fd
  def seek(self, fd, pos, whence = 0):
    return self._ufs.seek(fd, pos, whence)
  def read(self, fd, amnt = -1):
    return self._ufs.read(fd, amnt)
  def write(self, fd, data: bytes):
    return self._ufs.write(fd, data)
  def truncate(self, fd, length):
    return self._ufs.truncate(fd, length)
  def close(self, fd):
    path = self._fds.pop(fd)
    self._info_cache.discard(path)
    self._ls_cache.discard(path.parent)
    return self._ufs.close(fd)
  def unlink(self, path):
    self._info_cache.discard(path)
    self._ls_cache.discard(path.parent)
    return self._ufs.unlink(path)

  # optional
  def mkdir(self, path):
    self._info_cache.discard(path)
    self._ls_cache.discard(path)
    self._ls_cache.discard(path.parent)
    return self._ufs.mkdir(path)

  def rmdir(self, path):
    self._info_cache.discard(path)
    self._ls_cache.discard(path)
    self._ls_cache.discard(path.parent)
    return self._ufs.rmdir(path)

  def flush(self, fd):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src, dst):
    self._ufs.copy(src, dst)
    self._info_cache.discard(dst)
    self._ls_cache.discard(dst.parent)

  def rename(self, src, dst):
    self._ufs.rename(src, dst)
    self._info_cache.discard(src)
    self._ls_cache.discard(src.parent)
    self._info_cache.discard(dst)
    self._ls_cache.discard(dst.parent)

  def start(self):
    self._ufs.start()

  def stop(self):
    self._ufs.stop()
