''' A filesystem wrapper for prefixes in another.
This ensures all operations will be within the path prefix & exceptions masqueraded.

Usage:
ufs = Prefix(UFS(), '/some/subpath/')
'''

import os
import errno
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
      prefix=self._prefix,
    )

  def ls(self, path):
    try:
      return self._ufs.ls(self._prefix / path)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
  def info(self, path):
    try:
      return self._ufs.info(self._prefix / path)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
  def open(self, path, mode, *, size_hint = None):
    try:
      return self._ufs.open(self._prefix / path, mode, size_hint=size_hint)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
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
    try:
      return self._ufs.unlink(self._prefix / path)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))

  # optional
  def mkdir(self, path):
    try:
      return self._ufs.mkdir(self._prefix / path)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
  def rmdir(self, path):
    try:
      return self._ufs.rmdir(self._prefix / path)
    except FileNotFoundError:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(path))
  def flush(self, fd):
    return self._ufs.flush(fd)

  # fallback
  def copy(self, src, dst):
    try:
      return self._ufs.copy(self._prefix / src, self._prefix / dst)
    except FileNotFoundError as e:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(SafePurePosixPath(e.filename).relative_to(self._prefix)))

  def rename(self, src, dst):
    try:
      return self._ufs.rename(self._prefix / src, self._prefix / dst)
    except FileNotFoundError as e:
      raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(SafePurePosixPath(e.filename).relative_to(self._prefix)))

  def start(self):
    self._ufs.start()

  def stop(self):
    self._ufs.stop()
