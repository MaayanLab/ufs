from ufs.spec import SyncUFS, AsyncUFS, AccessScope

class SimpleAsync(AsyncUFS):
  ''' Applicable when no blocking is involved
  so there is no need to dispatch to another thread. i.e.
  for Memory, etc..
  '''
  def __init__(self, ufs: SyncUFS):
    super().__init__()
    self._ufs = ufs

  def scope(self) -> AccessScope:
    return self._ufs.scope()

  @staticmethod
  def from_dict(*, ufs):
    return SimpleAsync(
      ufs=SyncUFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )

  async def ls(self, path):
    return self._ufs.ls(path)

  async def info(self, path):
    return self._ufs.info(path)

  async def open(self, path, mode, *, size_hint = None):
    return self._ufs.open(path, mode, size_hint=size_hint)

  async def seek(self, fd, pos, whence = 0):
    return self._ufs.seek(fd, pos, whence)

  async def read(self, fd, amnt = -1):
    return self._ufs.read(fd, amnt)

  async def write(self, fd, data: bytes):
    return self._ufs.write(fd, data)

  async def truncate(self, fd, length):
    return self._ufs.truncate(fd, length)

  async def close(self, fd):
    return self._ufs.close(fd)

  async def unlink(self, path):
    return self._ufs.unlink(path)

  async def mkdir(self, path):
    return self._ufs.mkdir(path)

  async def rmdir(self, path):
    return self._ufs.rmdir(path)

  async def copy(self, src, dst):
    return self._ufs.copy(src, dst)
