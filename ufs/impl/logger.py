''' A filesystem wrapper for logging.
This logs all operations it passes through to the underlying UFS.

Usage:
ufs = Logger(UFS())
'''

import logging
import traceback
from ufs.spec import UFS

logger = logging.getLogger(__name__)

class Logger(UFS):
  def __init__(self, ufs: UFS):
    super().__init__()
    self._ufs = ufs
    self._logger = logger.getChild(repr(self._ufs))
    self._logger.warning(self._ufs)

  @staticmethod
  def from_dict(*, ufs):
    return Logger(
      ufs=UFS.from_dict(**ufs),
    )

  def to_dict(self):
    return dict(super().to_dict(),
      ufs=self._ufs.to_dict(),
    )

  def _call(self, op, *args, **kwargs):
    method = f"{op}({', '.join([*[repr(arg) for arg in args], *[key + '=' + repr(value) for key, value in kwargs.items()]])})"
    try:
      ret = getattr(self._ufs, op)(*args, **kwargs)
      self._logger.warning(f"{method} -> {ret}")
      return ret
    except Exception as e:
      self._logger.error(f"{method} raised {traceback.format_exc()}")
      raise e

  def ls(self, path):
    return self._call('ls', path)

  def info(self, path):
    return self._call('info', path)
  def open(self, path, mode, *, size_hint = None):
    return self._call('open', path, mode, size_hint=size_hint)
  def seek(self, fd, pos, whence = 0):
    return self._call('seek', fd, pos, whence)
  def read(self, fd, amnt):
    return self._call('read', fd, amnt)
  def write(self, fd, data: bytes):
    return self._call('write', fd, data)
  def truncate(self, fd, length):
    return self._call('truncate', fd, length)
  def close(self, fd):
    return self._call('close', fd)
  def unlink(self, path):
    return self._call('unlink', path)

  # optional
  def mkdir(self, path):
    return self._call('mkdir', path)
  def rmdir(self, path):
    return self._call('rmdir', path)
  def flush(self, fd):
    return self._call('flush', fd)

  # fallback
  def copy(self, src, dst):
    return self._call('copy', src, dst)

  def rename(self, src, dst):
    return self._call('rename', src, dst)

  def start(self):
    return self._call('start')

  def stop(self):
    return self._call('stop')
