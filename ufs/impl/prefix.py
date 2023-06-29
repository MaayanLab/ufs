''' A filesystem wrapper for prefixes in another.
This ensures all operations will be within the path prefix & exceptions masqueraded.

Usage:
ufs = Prefix(UFS(), '/some/subpath/')
'''

import logging
from ufs.spec import UFS
from ufs.utils.pathlib import SafePurePosixPath, PathLike

logger = logging.getLogger(__name__)

class Prefix(UFS):
  def __init__(self, ufs: UFS, prefix: PathLike = '/'):
    super().__init__()
    self._ufs = ufs
    self._prefix = SafePurePosixPath(prefix)

  @staticmethod
  def from_dict(*, ufs, prefix):
    return Prefix(
      ufs=UFS.from_dict(**ufs),
      prefix=prefix,
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
      prefix=str(self._prefix),
    )

  def ls(self, path):
    return self._ufs.ls(self._prefix / path)
  def info(self, path):
    return self._ufs.info(self._prefix / path)
  def open(self, path, mode, *, size_hint = None):
    return self._ufs.open(self._prefix / path, mode, size_hint=size_hint)
  def seek(self, fd, pos, whence = 0):
    return self._ufs.seek(fd, pos, whence)
  def read(self, fd, amnt):
    return self._ufs.read(fd, amnt)
  def write(self, fd, data):
    return self._ufs.write(fd, data)
  def truncate(self, fd, length):
    return self._ufs.truncate(fd, length)
  def close(self, fd):
    return self._ufs.close(fd)
  def unlink(self, path):
    return self._ufs.unlink(self._prefix / path)

  # optional
  def mkdir(self, path):
    return self._ufs.mkdir(self._prefix / path)
  def rmdir(self, path):
    return self._ufs.rmdir(self._prefix / path)
  def flush(self, fd):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src, dst):
    return self._ufs.copy(self._prefix / src, self._prefix / dst)

  def rename(self, src, dst):
    return self._ufs.rename(self._prefix / src, self._prefix / dst)

  def start(self):
    self._ufs.start()

  def stop(self):
    self._ufs.stop()
